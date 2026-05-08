package com.example.cryptobot.scheduler

import com.example.cryptobot.coinbase.CoinbaseClient
import com.example.cryptobot.config.BotProperties
import com.example.cryptobot.persistence.TradeDecisionLog
import com.example.cryptobot.persistence.TradeDecisionLogRepository
import java.time.OffsetDateTime
import java.time.ZoneOffset
import com.example.cryptobot.strategy.MarketSnapshot
import com.example.cryptobot.strategy.SimpleDipBuyStrategy
import com.example.cryptobot.strategy.TradingDecision
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.math.BigDecimal
import java.time.Duration

@Component
class BotRunner(
    private val props: BotProperties,
    private val coinbaseClient: CoinbaseClient,
    private val strategy: SimpleDipBuyStrategy,
    private val tradeDecisionLogRepository: TradeDecisionLogRepository,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedRateString = "\${bot.fixed-rate-ms}")
    fun tick() {
        if (!props.enabled) {
            log.debug("Bot disabled.")
            return
        }

        buildSnapshot()
            .flatMap { snapshot ->
                strategy.decide(snapshot).let { decision ->
                    execute(snapshot, decision)
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
            .doOnSubscribe {
                log.info("Bot tick started")
            }
            .doOnSuccess {
                log.info("Bot tick completed")
            }
            .doOnError { ex ->
                log.error("Bot tick failed", ex)
            }
            .subscribe()
    }

    private fun buildSnapshot(): Mono<MarketSnapshot> {
        log.info("Building market snapshot for {}", props.productId)

        return Mono.zip(
            coinbaseClient.getProduct(props.productId),
            coinbaseClient.listAccounts()
        )
            .map { tuple ->
                val product = tuple.t1
                val accounts = tuple.t2.accounts
                val usdAvailable = accounts
                    .filter { it.currency == "USD" }
                    .sumOf { it.availableBalance.decimal() }

                MarketSnapshot(
                    productId = props.productId,
                    price = product.price.toBigDecimalOrNull() ?: BigDecimal.ZERO,
                    change24hPercent = product.pricePercentageChange24h?.toBigDecimalOrNull() ?: BigDecimal.ZERO,
                    usdAvailable = usdAvailable,
                )
            }
            .doOnSuccess {
                log.info(
                    "Market snapshot built: product={} price={} change24h={} usdAvailable={}",
                    it.productId,
                    it.price,
                    it.change24hPercent,
                    it.usdAvailable
                )
            }
    }

    private fun execute(snapshot: MarketSnapshot, decision: TradingDecision): Mono<Unit> = when (decision) {
        is TradingDecision.Skip -> {
            log.info("SKIP: {}", decision.reason)

            tradeDecisionLogRepository.save(
                TradeDecisionLog(
                    productId = snapshot.productId,
                    decisionType = "SKIP",
                    reason = decision.reason,
                    price = snapshot.price,
                    change24hPercent = snapshot.change24hPercent,
                    usdAvailable = snapshot.usdAvailable,
                    quoteSizeUsd = null,
                    dryRun = props.dryRun,
                )
            ).thenReturn(Unit)
        }

        is TradingDecision.Buy -> {
            val since = OffsetDateTime.now(ZoneOffset.UTC).toLocalDate()
                .atStartOfDay()
                .atOffset(ZoneOffset.UTC)

            tradeDecisionLogRepository.liveBuyTotalSince(since)
                .flatMap { spentToday ->
                    val projected = spentToday + decision.quoteSizeUsd

                    if (!props.dryRun && projected > props.maxDailyBuyUsd) {
                        val reason = "Daily live buy limit reached. spentToday=$spentToday projected=$projected max=${props.maxDailyBuyUsd}"
                        log.warn("SKIP: {}", reason)

                        tradeDecisionLogRepository.save(
                            TradeDecisionLog(
                                productId = snapshot.productId,
                                decisionType = "SKIP",
                                reason = reason,
                                price = snapshot.price,
                                change24hPercent = snapshot.change24hPercent,
                                usdAvailable = snapshot.usdAvailable,
                                quoteSizeUsd = decision.quoteSizeUsd,
                                dryRun = props.dryRun,
                            )
                        ).thenReturn(Unit)
                    } else if (props.dryRun) {
                        log.warn(
                            "DRY RUN: would BUY {} of {}. Reason: {}",
                            decision.quoteSizeUsd,
                            decision.productId,
                            decision.reason
                        )

                        tradeDecisionLogRepository.save(
                            TradeDecisionLog(
                                productId = snapshot.productId,
                                decisionType = "BUY",
                                reason = decision.reason,
                                price = snapshot.price,
                                change24hPercent = snapshot.change24hPercent,
                                usdAvailable = snapshot.usdAvailable,
                                quoteSizeUsd = decision.quoteSizeUsd,
                                dryRun = true,
                            )
                        ).thenReturn(Unit)
                    } else {
                        log.warn("LIVE TRADE: BUY {} of {}. Reason: {}", decision.quoteSizeUsd, decision.productId, decision.reason)

                        coinbaseClient.createMarketBuy(decision.productId, decision.quoteSizeUsd)
                            .flatMap { response ->
                                tradeDecisionLogRepository.save(
                                    TradeDecisionLog(
                                        productId = snapshot.productId,
                                        decisionType = "BUY",
                                        reason = decision.reason,
                                        price = snapshot.price,
                                        change24hPercent = snapshot.change24hPercent,
                                        usdAvailable = snapshot.usdAvailable,
                                        quoteSizeUsd = decision.quoteSizeUsd,
                                        dryRun = false,
                                        coinbaseSuccess = response.success,
                                        errorMessage = response.errorResponse?.toString(),
                                    )
                                )
                            }
                            .thenReturn(Unit)
                    }
                }
        }
    }
}
