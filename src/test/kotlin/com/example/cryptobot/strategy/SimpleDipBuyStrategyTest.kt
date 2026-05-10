package com.example.cryptobot.strategy

import com.example.cryptobot.config.BotProperties
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class SimpleDipBuyStrategyTest {
    @Test
    fun `buys when dip threshold is crossed and cash reserve is safe`() {
        val strategy = SimpleDipBuyStrategy(BotProperties())
        val decision = strategy.decide(
            MarketSnapshot(
                productId = "BTC-USD",
                price = BigDecimal("50000"),
                change24hPercent = BigDecimal("-6.0"),
                usdAvailable = BigDecimal("1000.00"),
                volume24h = BigDecimal("25000000000"),
                high24h = BigDecimal("52000"),
                low24h = BigDecimal("48000"),
                priceChange24h = BigDecimal("-3200"),
                priceTo24hHighPercent = BigDecimal("96.15"),
                priceTo24hLowPercent = BigDecimal("104.16"),
                cryptoBalance = BigDecimal("0.25"),
                cryptoValueUsd = BigDecimal("0.000"),
                portfolioUsdValue = BigDecimal("0.000"),
                portfolioAllocationPercent = BigDecimal("0.000"),
            )
        )
        assertTrue(decision is TradingDecision.Buy)
    }

    @Test
    fun `skips when dip is not large enough`() {
        val strategy = SimpleDipBuyStrategy(BotProperties())
        val decision = strategy.decide(
            MarketSnapshot(
                productId = "BTC-USD",
                price = BigDecimal("50000"),
                change24hPercent = BigDecimal("-2.0"),
                usdAvailable = BigDecimal("1000.00"),
                volume24h = BigDecimal("25000000000"),
                high24h = BigDecimal("52000"),
                low24h = BigDecimal("48000"),
                priceChange24h = BigDecimal("-3200"),
                priceTo24hHighPercent = BigDecimal("96.15"),
                priceTo24hLowPercent = BigDecimal("104.16"),
                cryptoBalance = BigDecimal("0.25"),
                cryptoValueUsd = BigDecimal("0.000"),
                portfolioUsdValue = BigDecimal("0.000"),
                portfolioAllocationPercent = BigDecimal("0.000"),
            )        )
        assertTrue(decision is TradingDecision.Skip)
    }
}
