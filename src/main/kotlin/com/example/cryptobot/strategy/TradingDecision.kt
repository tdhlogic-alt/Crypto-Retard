package com.example.cryptobot.strategy

import java.math.BigDecimal

data class MarketSnapshot(
    val productId: String,
    val price: BigDecimal,
    val change24hPercent: BigDecimal,
    val usdAvailable: BigDecimal,
)

sealed class TradingDecision {
    data class Buy(val productId: String, val quoteSizeUsd: BigDecimal, val reason: String) : TradingDecision()
    data class Skip(val reason: String) : TradingDecision()
}
