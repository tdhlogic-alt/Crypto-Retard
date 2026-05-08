package com.example.cryptobot.scheduler

import com.example.cryptobot.coinbase.CoinbaseClient
import com.example.cryptobot.config.BotProperties
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
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedRateString = "\${bot.fixed-rate-ms}")
    fun tick() {
        if (!props.enabled) {
            log.debug("Bot disabled.")
            return
        }

        buildSnapshot()
            .map(strategy::decide)
            .flatMap(::execute)
            .retryWhen(
                Retry.backoff(3, Duration.ofSeconds(2))
                    .filter { it !is IllegalArgumentException }
                    .doBeforeRetry { signal ->
                        log.warn("Retry attempt ${signal.totalRetries() + 1}")
                    }
            )
            .doOnError { log.error("Bot tick failed after retries", it) }
            .onErrorResume { Mono.empty() }
            .subscribe()
    }

    @CircuitBreaker(name = "coinbaseApi", fallbackMethod = "buildSnapshotFallback")
    private fun buildSnapshot(): Mono<MarketSnapshot> {
        return Mono.zip(
            coinbaseClient.getProduct(props.productId)
                .timeout(Duration.ofSeconds(30)),
            coinbaseClient.listAccounts()
                .timeout(Duration.ofSeconds(30))
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
    }

    private fun buildSnapshotFallback(ex: Exception): Mono<MarketSnapshot> {
        log.error("Circuit breaker fallback triggered: ${ex.message}")
        return Mono.error(ex)
    }

    private fun execute(decision: TradingDecision): Mono<Unit> = when (decision) {
        is TradingDecision.Skip -> {
            log.info("SKIP: {}", decision.reason)
            Mono.just(Unit)
        }
        is TradingDecision.Buy -> {
            if (props.dryRun) {
                log.warn("DRY RUN: would BUY {} of {}. Reason: {}", decision.quoteSizeUsd, decision.productId, decision.reason)
                Mono.just(Unit)
            } else {
                log.warn("LIVE TRADE: BUY {} of {}. Reason: {}", decision.quoteSizeUsd, decision.productId, decision.reason)
                coinbaseClient.createMarketBuy(decision.productId, decision.quoteSizeUsd)
                    .timeout(Duration.ofSeconds(30))
                    .doOnNext { log.warn("Coinbase order response success={} error={}", it.success, it.errorResponse) }
                    .thenReturn(Unit)
            }
        }
    }
}
