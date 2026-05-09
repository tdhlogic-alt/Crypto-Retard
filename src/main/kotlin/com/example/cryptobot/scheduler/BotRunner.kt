package com.example.cryptobot.scheduler

import com.example.cryptobot.alerts.DiscordAlertClient
import com.example.cryptobot.coinbase.CoinbaseClient
import com.example.cryptobot.config.BotProperties
import com.example.cryptobot.strategy.MarketSnapshot
import com.example.cryptobot.strategy.SimpleDipBuyStrategy
import com.example.cryptobot.strategy.TradingDecision
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
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
    private val alerts: DiscordAlertClient,
) : CommandLineRunner {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun run(vararg args: String) {
        if (!props.enabled) {
            log.info("Bot disabled.")
            return
        }

        buildSnapshot()
            .flatMap { snapshot ->
                val decision = strategy.decide(snapshot)
                execute(snapshot, decision)
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
            Mono.just(Unit)
        }

        is TradingDecision.Buy -> {
            if (props.dryRun) {
                val message = "🧪 DRY RUN: would BUY ${decision.quoteSizeUsd} of ${decision.productId}. Reason: ${decision.reason}"
                log.warn(message)
                alerts.send(message)
                    .thenReturn(Unit)
            } else {
                val message = "🚨 LIVE TRADE: BUY ${decision.quoteSizeUsd} of ${decision.productId}. Reason: ${decision.reason}"
                log.warn(message)
                alerts.send(message)
                    .then(coinbaseClient.createMarketBuy(decision.productId, decision.quoteSizeUsd))
                    .thenReturn(Unit)
            }
        }
    }
}