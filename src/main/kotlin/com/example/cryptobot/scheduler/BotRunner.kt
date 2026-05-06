package com.example.cryptobot.scheduler

import com.example.cryptobot.coinbase.CoinbaseClient
import com.example.cryptobot.config.BotProperties
import com.example.cryptobot.strategy.MarketSnapshot
import com.example.cryptobot.strategy.SimpleDipBuyStrategy
import com.example.cryptobot.strategy.TradingDecision
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.math.BigDecimal

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
            log.info("Bot disabled. Set bot.enabled=true to run strategy.")
            return
        }

        buildSnapshot()
            .map(strategy::decide)
            .flatMap(::execute)
            .doOnError { log.error("Bot tick failed", it) }
            .onErrorResume { Mono.empty() }
            .block()
    }

    private fun buildSnapshot(): Mono<MarketSnapshot> {
        return Mono.zip(coinbaseClient.getProduct(props.productId), coinbaseClient.listAccounts())
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
                    .doOnNext { log.warn("Coinbase order response success={} error={}", it.success, it.errorResponse) }
                    .thenReturn(Unit)
            }
        }
    }
}
