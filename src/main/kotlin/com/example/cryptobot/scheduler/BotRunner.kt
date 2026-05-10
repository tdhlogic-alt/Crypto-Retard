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
                                alerts.send(
                                    """
                                    🤖 Agent Decision
                                    
                                    Product: ${agentDecision.productId}
                                    Action: ${agentDecision.action}
                                    Score: ${agentDecision.score}
                                    Confidence: ${agentDecision.confidence}
                                    
                                    Reason:
                                    ${agentDecision.reason}
                                    """.trimIndent()
                                ).then(Mono.defer {
                                    val validatedDecision = agentValidator.validate(snapshot, agentDecision)
                                    execute(snapshot, validatedDecision)
                                })

                                val validatedDecision = agentValidator.validate(snapshot, agentDecision)
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
            .flatMap { accountsResponse ->
                val accounts = accountsResponse.accounts

                val usdAvailable = accounts
                    .filter { it.currency == "USD" }
                    .sumOf { it.availableBalance.decimal() }

                Flux.fromIterable(props.productIds)
                    .flatMap { productId ->
                        val now = Instant.now()
                        val start = now.minus(7, ChronoUnit.DAYS)

                        coinbaseClient.getProduct(productId)
                            .zipWith(
                                coinbaseClient.getCandles(
                                    productId = productId,
                                    granularity = "ONE_HOUR",
                                    start = start,
                                    end = now,
                                )
                            )
                            .map { tuple ->
                                val product = tuple.t1
                                val candles = tuple.t2.candles
                                    .sortedBy { it.start.toLongOrNull() ?: 0L }

                                val closes = candles.mapNotNull { it.close.toBigDecimalOrNull() }
                                val highs = candles.mapNotNull { it.high.toBigDecimalOrNull() }
                                val lows = candles.mapNotNull { it.low.toBigDecimalOrNull() }

                                val price = product.price.toBigDecimalOrNull() ?: BigDecimal.ZERO
                                val baseCurrency = productId.substringBefore("-")

                                val cryptoBalance = accounts
                                    .filter { it.currency == baseCurrency }
                                    .sumOf { it.availableBalance.decimal() }

                                val cryptoValueUsd = cryptoBalance.multiply(price)

                                val close1hAgo = closes.getOrNull((closes.size - 2).coerceAtLeast(0)) ?: price
                                val close4hAgo = closes.getOrNull((closes.size - 5).coerceAtLeast(0)) ?: price
                                val close24hAgo = closes.getOrNull((closes.size - 25).coerceAtLeast(0)) ?: closes.firstOrNull() ?: price
                                val close7dAgo = closes.firstOrNull() ?: price

                                val candleHigh24h = highs.maxOrNull() ?: BigDecimal.ZERO
                                val candleLow24h = lows.minOrNull() ?: BigDecimal.ZERO

                                val candleHigh7d = highs.maxOrNull() ?: BigDecimal.ZERO
                                val candleLow7d = lows.minOrNull() ?: BigDecimal.ZERO

                                productId to MarketSnapshot(
                                    productId = productId,
                                    price = price,
                                    change24hPercent = product.pricePercentageChange24h?.toBigDecimalOrNull() ?: BigDecimal.ZERO,
                                    usdAvailable = usdAvailable,
                                    volume24h = product.volume24h?.toBigDecimalOrNull() ?: BigDecimal.ZERO,

                                    high24h = candleHigh24h,
                                    low24h = candleLow24h,
                                    priceChange24h = price.subtract(close24hAgo),
                                    priceTo24hHighPercent = if (candleHigh24h > BigDecimal.ZERO) {
                                        price.divide(candleHigh24h, 4, RoundingMode.HALF_UP).multiply(BigDecimal("100"))
                                    } else BigDecimal.ZERO,
                                    priceTo24hLowPercent = if (candleLow24h > BigDecimal.ZERO) {
                                        price.divide(candleLow24h, 4, RoundingMode.HALF_UP).multiply(BigDecimal("100"))
                                    } else BigDecimal.ZERO,

                                    cryptoBalance = cryptoBalance,
                                    cryptoValueUsd = cryptoValueUsd,
                                    portfolioUsdValue = BigDecimal.ZERO,
                                    portfolioAllocationPercent = BigDecimal.ZERO,

                                    trend1hPercent = pctChange(close1hAgo, price),
                                    trend4hPercent = pctChange(close4hAgo, price),
                                    trend24hPercent = pctChange(close24hAgo, price),
                                    rsi14 = calculateRsi(closes),
                                    volatility24hPercent = calculateVolatilityPercent(closes),
                                    candleHigh24h = candleHigh24h,
                                    candleLow24h = candleLow24h,
                                    trend7dPercent = pctChange(close7dAgo, price),
                                    candleHigh7d = candleHigh7d,
                                    candleLow7d = candleLow7d,
                                )
                            }
                    }
                    .collectList()
                    .map { pairs ->
                        val snapshots = pairs.map { it.second }

                        val cryptoPortfolioValue = snapshots
                            .sumOf { it.cryptoValueUsd }

                        val totalPortfolioValue = usdAvailable + cryptoPortfolioValue

                        snapshots.map { snapshot ->
                            val allocationPercent =
                                if (totalPortfolioValue > BigDecimal.ZERO) {
                                    snapshot.cryptoValueUsd
                                        .divide(totalPortfolioValue, 4, RoundingMode.HALF_UP)
                                        .multiply(BigDecimal("100"))
                                } else {
                                    BigDecimal.ZERO
                                }

                            snapshot.copy(
                                portfolioUsdValue = totalPortfolioValue,
                                portfolioAllocationPercent = allocationPercent,
                            )
                        }
                    }
            }
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
            when {
                props.dryRun -> {
                    val message = "🧪 DRY RUN: would SELL ${decision.baseSize} of ${decision.productId}. Reason: ${decision.reason}"
                    log.warn(message)

                    ledger.record(
                        snapshot = snapshot,
                        decisionType = "SELL",
                        reason = decision.reason,
                        dryRun = true,
                        baseSize = decision.baseSize,
                    )
                        .then(alerts.send(message))
                        .thenReturn(Unit)
                }

                !props.liveTradingEnabled -> {
                    val message = "🛑 LIVE SELL BLOCKED: liveTradingEnabled=false. Would have sold ${decision.baseSize} of ${decision.productId}"
                    log.warn(message)

                    ledger.record(
                        snapshot = snapshot,
                        decisionType = "BLOCKED_SELL",
                        reason = message,
                        dryRun = false,
                        baseSize = decision.baseSize,
                    )
                        .then(alerts.send(message))
                        .thenReturn(Unit)
                }

                decision.baseSize > snapshot.cryptoBalance -> {
                    val message = "🛑 LIVE SELL BLOCKED: baseSize=${decision.baseSize} exceeds available balance=${snapshot.cryptoBalance}"
                    log.warn(message)

                    ledger.record(
                        snapshot = snapshot,
                        decisionType = "BLOCKED_SELL",
                        reason = message,
                        dryRun = false,
                        baseSize = decision.baseSize,
                    )
                        .then(alerts.send(message))
                        .thenReturn(Unit)
                }

                else -> {
                    val message = "🚨 LIVE TRADE: SELL ${decision.baseSize} of ${decision.productId}. Reason: ${decision.reason}"
                    log.warn(message)

                    alerts.send(message)
                        .then(coinbaseClient.createMarketSell(decision.productId, decision.baseSize))
                        .flatMap { response ->
                            ledger.record(
                                snapshot = snapshot,
                                decisionType = "SELL",
                                reason = decision.reason,
                                dryRun = false,
                                baseSize = decision.baseSize,
                                coinbaseSuccess = response.success,
                                errorMessage = response.errorResponse?.toString(),
                            )
                        }
                        .thenReturn(Unit)
                }
            }
        }
    }

    private fun pctChange(old: BigDecimal, current: BigDecimal): BigDecimal {
        if (old <= BigDecimal.ZERO) return BigDecimal.ZERO
        return current.subtract(old)
            .divide(old, 6, RoundingMode.HALF_UP)
            .multiply(BigDecimal("100"))
    }

    private fun calculateRsi(closes: List<BigDecimal>, period: Int = 14): BigDecimal {
        if (closes.size <= period) return BigDecimal.ZERO

        val recent = closes.takeLast(period + 1)
        var gains = BigDecimal.ZERO
        var losses = BigDecimal.ZERO

        for (i in 1 until recent.size) {
            val diff = recent[i].subtract(recent[i - 1])
            if (diff >= BigDecimal.ZERO) gains += diff else losses += diff.abs()
        }

        if (losses == BigDecimal.ZERO) return BigDecimal("100")
        val rs = gains.divide(losses, 6, RoundingMode.HALF_UP)

        return BigDecimal("100").subtract(
            BigDecimal("100").divide(BigDecimal.ONE + rs, 2, RoundingMode.HALF_UP)
        )
    }

    private fun calculateVolatilityPercent(closes: List<BigDecimal>): BigDecimal {
        if (closes.size < 2) return BigDecimal.ZERO

        val returns = closes.zipWithNext { a, b ->
            if (a > BigDecimal.ZERO) {
                b.subtract(a).divide(a, 8, RoundingMode.HALF_UP).toDouble()
            } else {
                0.0
            }
        }

        val mean = returns.average()
        val variance = returns.map { (it - mean) * (it - mean) }.average()
        return BigDecimal(Math.sqrt(variance) * 100).setScale(2, RoundingMode.HALF_UP)
    }
}