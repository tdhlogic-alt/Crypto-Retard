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
                Retry.backoff(2, Duration.ofSeconds(3))
                    .maxBackoff(Duration.ofSeconds(15))
                    .jitter(0.25)
                    .filter { ex ->
                        ex is java.util.concurrent.TimeoutException ||
                                ex is java.net.ConnectException ||
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
