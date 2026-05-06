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
            MarketSnapshot("BTC-USD", BigDecimal("50000"), BigDecimal("-6.0"), BigDecimal("1000.00"))
        )
        assertTrue(decision is TradingDecision.Buy)
    }

    @Test
    fun `skips when dip is not large enough`() {
        val strategy = SimpleDipBuyStrategy(BotProperties())
        val decision = strategy.decide(
            MarketSnapshot("BTC-USD", BigDecimal("50000"), BigDecimal("-2.0"), BigDecimal("1000.00"))
        )
        assertTrue(decision is TradingDecision.Skip)
    }
}
