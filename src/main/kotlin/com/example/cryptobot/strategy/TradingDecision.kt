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
    val avgCostBasis: BigDecimal = BigDecimal.ZERO,
    val totalInvested: BigDecimal = BigDecimal.ZERO,
    val realizedPnlUsd: BigDecimal = BigDecimal.ZERO,
    val unrealizedPnlUsd: BigDecimal = BigDecimal.ZERO,
    val unrealizedPnlPercent: BigDecimal = BigDecimal.ZERO,
    val highestPriceSeen: BigDecimal = BigDecimal.ZERO,
    val drawdownFromHighPercent: BigDecimal = BigDecimal.ZERO,
    val buyCount: Long = 0,
    val sellCount: Long = 0,
    val marketRegime: String = "UNKNOWN",
    val reasonCode30dWinRate: BigDecimal = BigDecimal.ZERO,
    val reasonCode30dCount: Long = 0,
    val activeThesis: String = "",
    val activeInvalidationCondition: String = "",
    val activeProfitTargetPercent: BigDecimal = BigDecimal.ZERO,
    val activeStopLossPercent: BigDecimal = BigDecimal.ZERO,
    val activeMaxHoldHours: Long = 0,
)

sealed interface TradingDecision {

    data class Buy(
        val productId: String,
        val quoteSizeUsd: BigDecimal,
        val reason: String,
        val reasonCode: String = "NO_CLEAR_EDGE",
        val thesis: String = "",
        val invalidationCondition: String = "",
        val profitTargetPercent: BigDecimal = BigDecimal.ZERO,
        val stopLossPercent: BigDecimal = BigDecimal.ZERO,
        val maxHoldHours: Long = 0,
    ) : TradingDecision

    data class Sell(
        val productId: String,
        val baseSize: BigDecimal,
        val reason: String,
        val reasonCode: String = "NO_CLEAR_EDGE",
    ) : TradingDecision

    data class Skip(
        val reason: String,
    ) : TradingDecision
}
