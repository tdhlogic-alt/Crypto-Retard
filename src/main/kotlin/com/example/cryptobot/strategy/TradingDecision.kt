package com.example.cryptobot.strategy

import java.math.BigDecimal

data class MarketSnapshot(
    val productId: String,
    val price: BigDecimal,
    val change24hPercent: BigDecimal,
    val usdAvailable: BigDecimal,
    val volume24h: BigDecimal,
    val high24h: BigDecimal,
    val low24h: BigDecimal,
    val priceChange24h: BigDecimal,
    val priceTo24hHighPercent: BigDecimal,
    val priceTo24hLowPercent: BigDecimal,
    val cryptoBalance: BigDecimal,
    val cryptoValueUsd: BigDecimal,
    val portfolioUsdValue: BigDecimal,
    val portfolioAllocationPercent: BigDecimal,
    val trend1hPercent: BigDecimal = BigDecimal.ZERO,
    val trend4hPercent: BigDecimal = BigDecimal.ZERO,
    val trend24hPercent: BigDecimal = BigDecimal.ZERO,
    val rsi14: BigDecimal = BigDecimal.ZERO,
    val volatility24hPercent: BigDecimal = BigDecimal.ZERO,
    val candleHigh24h: BigDecimal = BigDecimal.ZERO,
    val candleLow24h: BigDecimal = BigDecimal.ZERO,
    val trend7dPercent: BigDecimal = BigDecimal.ZERO,
    val candleHigh7d: BigDecimal = BigDecimal.ZERO,
    val candleLow7d: BigDecimal = BigDecimal.ZERO,
)

sealed interface TradingDecision {

    data class Buy(
        val productId: String,
        val quoteSizeUsd: BigDecimal,
        val reason: String,
    ) : TradingDecision

    data class Sell(
        val productId: String,
        val baseSize: BigDecimal,
        val reason: String,
    ) : TradingDecision

    data class Skip(
        val reason: String,
    ) : TradingDecision
}
