package com.example.cryptobot.scheduler

import com.example.cryptobot.agent.AgentDecisionValidator
import com.example.cryptobot.agent.AgentTradeDecision
import com.example.cryptobot.agent.OpenAiAgentClient
import com.example.cryptobot.alerts.DiscordAlertClient
import com.example.cryptobot.coinbase.CoinbaseClient
import com.example.cryptobot.config.BotProperties
import com.example.cryptobot.persistence.TradeLedgerClient
import com.example.cryptobot.strategy.MarketSnapshot
import com.example.cryptobot.strategy.SimpleDipBuyStrategy
import com.example.cryptobot.strategy.TradingDecision
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

@Component
class BotRunner(
    private val props: BotProperties,
    private val coinbaseClient: CoinbaseClient,
    private val strategy: SimpleDipBuyStrategy,
    private val alerts: DiscordAlertClient,
    private val ledger: TradeLedgerClient,
    private val agentClient: OpenAiAgentClient,
    private val agentValidator: AgentDecisionValidator,
) : CommandLineRunner {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun run(vararg args: String) {
        if (!props.enabled) {
            log.info("Bot disabled.")
            return
        }

        buildSnapshots()
            .flatMap { snapshots ->
                if (props.agentEnabled) {
                    Flux.fromIterable(snapshots)
                        .flatMap { snapshot ->
                            agentClient.decide(snapshot)
                                .map { agentDecision -> snapshot to agentDecision }
                        }
                        .collectList()
                        .flatMap { rankedAgentResults ->
                            val best = rankedAgentResults
                                .sortedByDescending { (_, decision) -> decision.score }
                                .firstOrNull()

                            if (best == null) {
                                log.info("No agent decisions produced.")
                                Mono.just(Unit)
                            } else {
                                val snapshot = best.first
                                val agentDecision = best.second

                                log.info(
                                    "Top ranked opportunity: product={} score={} action={} confidence={} reason={}",
                                    agentDecision.productId,
                                    agentDecision.score,
                                    agentDecision.action,
                                    agentDecision.confidence,
                                    agentDecision.reason
                                )

                                val validatedDecision = agentValidator.validate(agentDecision)
                                execute(snapshot, validatedDecision)
                            }
                        }
                } else {
                    Flux.fromIterable(snapshots)
                        .flatMap { snapshot ->
                            val decision = strategy.decide(snapshot)
                            execute(snapshot, decision)
                        }
                        .then()
                }
            }
            .retryWhen(
                Retry.backoff(2, Duration.ofSeconds(3))
                    .maxBackoff(Duration.ofSeconds(15))
                    .jitter(0.25)
                    .filter { ex ->
                        ex is java.util.concurrent.TimeoutException ||
                                ex is java.net.ConnectException ||
                                ex is java.nio.channels.ClosedChannelException ||
                                ex is javax.net.ssl.SSLException ||
                                ex is org.springframework.web.reactive.function.client.WebClientRequestException ||
                                ex is IllegalStateException && ex.message?.contains("response body has been released") == true
                    }
                    .doBeforeRetry { signal ->
                        log.warn(
                            "Retrying bot tick. attempt={} error={}",
                            signal.totalRetries() + 1,
                            signal.failure().message
                        )
                    }
            )
            .doOnSubscribe { log.info("Bot tick started") }
            .doOnSuccess { log.info("Bot tick completed") }
            .doOnError { ex ->
                log.error("Bot tick failed", ex)
                alerts.send("🚨 Crypto bot failed: `${ex.message ?: ex::class.simpleName}`").block()
            }
            .block()

        log.info("Bot job finished successfully")
    }

    private fun buildSnapshots(): Mono<List<MarketSnapshot>> {
        log.info("Building market snapshots for {}", props.productIds)

        return coinbaseClient.listAccounts()
            .flatMapMany { accountsResponse ->
                val accounts = accountsResponse.accounts
                val usdAvailable = accounts
                    .filter { it.currency == "USD" }
                    .sumOf { it.availableBalance.decimal() }

                Flux.fromIterable(props.productIds)
                    .flatMap { productId ->
                        coinbaseClient.getProduct(productId)
                            .map { product ->
                                val price = product.price.toBigDecimalOrNull() ?: BigDecimal.ZERO

                                val baseCurrency = productId.substringBefore("-")

                                val cryptoBalance = accounts
                                    .filter { it.currency == baseCurrency }
                                    .sumOf { it.availableBalance.decimal() }

                                MarketSnapshot(
                                    productId = productId,
                                    price = price,
                                    change24hPercent = product.pricePercentageChange24h?.toBigDecimalOrNull() ?: BigDecimal.ZERO,
                                    usdAvailable = usdAvailable,
                                    volume24h = product.volume24h?.toBigDecimalOrNull() ?: BigDecimal.ZERO,
                                    high24h = BigDecimal.ZERO,
                                    low24h = BigDecimal.ZERO,
                                    priceChange24h = BigDecimal.ZERO,
                                    priceTo24hHighPercent = BigDecimal.ZERO,
                                    priceTo24hLowPercent = BigDecimal.ZERO,
                                    cryptoBalance = cryptoBalance,
                                )
                            }
                            .doOnSuccess {
                                log.info(
                                    "Market snapshot built: product={} price={} change24h={} volume24h={} high24h={} low24h={} usdAvailable={}",
                                    it.productId,
                                    it.price,
                                    it.change24hPercent,
                                    it.volume24h,
                                    it.high24h,
                                    it.low24h,
                                    it.usdAvailable
                                )
                            }
                    }
            }
            .collectList()
    }

    private fun execute(snapshot: MarketSnapshot, decision: TradingDecision): Mono<Unit> = when (decision) {
        is TradingDecision.Skip -> {
            log.info("SKIP: {}", decision.reason)

            ledger.record(
                snapshot = snapshot,
                decisionType = "SKIP",
                reason = decision.reason,
                dryRun = props.dryRun,
            ).thenReturn(Unit)
        }

        is TradingDecision.Buy -> {
            when {
                props.dryRun -> {
                    val message = "🧪 DRY RUN: would BUY ${decision.quoteSizeUsd} of ${decision.productId}. Reason: ${decision.reason}"
                    log.warn(message)

                    ledger.record(
                        snapshot = snapshot,
                        decisionType = "BUY",
                        reason = decision.reason,
                        dryRun = true,
                        quoteSizeUsd = decision.quoteSizeUsd,
                    )
                        .then(alerts.send(message))
                        .thenReturn(Unit)
                }

                !props.liveTradingEnabled -> {
                    val message = "🛑 LIVE TRADE BLOCKED: liveTradingEnabled=false. Would have bought ${decision.quoteSizeUsd} of ${decision.productId}"
                    log.warn(message)

                    ledger.record(
                        snapshot = snapshot,
                        decisionType = "BLOCKED_BUY",
                        reason = message,
                        dryRun = false,
                        quoteSizeUsd = decision.quoteSizeUsd,
                    )
                        .then(alerts.send(message))
                        .thenReturn(Unit)
                }

                decision.quoteSizeUsd > props.maxBuyQuoteSizeUsd -> {
                    val message = "🛑 LIVE TRADE BLOCKED: quoteSizeUsd=${decision.quoteSizeUsd} exceeds max=${props.maxBuyQuoteSizeUsd}"
                    log.warn(message)

                    ledger.record(
                        snapshot = snapshot,
                        decisionType = "BLOCKED_BUY",
                        reason = message,
                        dryRun = false,
                        quoteSizeUsd = decision.quoteSizeUsd,
                    )
                        .then(alerts.send(message))
                        .thenReturn(Unit)
                }

                else -> {
                    val recentSince = Instant.now().minus(30, ChronoUnit.MINUTES)
                    val todaySince = Instant.now().truncatedTo(ChronoUnit.DAYS)

                    ledger.hasRecentLiveBuy(decision.productId, recentSince)
                        .flatMap { hasRecentBuy ->
                            if (hasRecentBuy) {
                                val message = "🛑 LIVE TRADE BLOCKED: recent live BUY already exists for ${decision.productId}"
                                log.warn(message)

                                ledger.record(
                                    snapshot = snapshot,
                                    decisionType = "BLOCKED_BUY",
                                    reason = message,
                                    dryRun = false,
                                    quoteSizeUsd = decision.quoteSizeUsd,
                                )
                                    .then(alerts.send(message))
                                    .thenReturn(Unit)
                            } else {
                                ledger.liveBuyTotalSince(todaySince)
                                    .flatMap { spentToday ->
                                        val projectedSpend = spentToday + decision.quoteSizeUsd

                                        if (projectedSpend > props.maxDailyBuyUsd) {
                                            val message =
                                                "🛑 LIVE TRADE BLOCKED: daily buy limit exceeded. spentToday=$spentToday projected=$projectedSpend max=${props.maxDailyBuyUsd}"
                                            log.warn(message)

                                            ledger.record(
                                                snapshot = snapshot,
                                                decisionType = "BLOCKED_BUY",
                                                reason = message,
                                                dryRun = false,
                                                quoteSizeUsd = decision.quoteSizeUsd,
                                            )
                                                .then(alerts.send(message))
                                                .thenReturn(Unit)
                                        } else {
                                            val message =
                                                "🚨 LIVE TRADE: BUY ${decision.quoteSizeUsd} of ${decision.productId}. Reason: ${decision.reason}"
                                            log.warn(message)

                                            alerts.send(message)
                                                .then(
                                                    coinbaseClient.createMarketBuy(
                                                        decision.productId,
                                                        decision.quoteSizeUsd
                                                    )
                                                )
                                                .flatMap { response ->
                                                    ledger.record(
                                                        snapshot = snapshot,
                                                        decisionType = "BUY",
                                                        reason = decision.reason,
                                                        dryRun = false,
                                                        quoteSizeUsd = decision.quoteSizeUsd,
                                                        coinbaseSuccess = response.success,
                                                        errorMessage = response.errorResponse?.toString(),
                                                    )
                                                }
                                                .thenReturn(Unit)
                                        }
                                    }
                            }
                        }
                }
            }
        }
        is TradingDecision.Sell -> {
            TODO()
        }
    }
}