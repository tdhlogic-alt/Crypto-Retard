package com.example.cryptobot.agent

import java.math.BigDecimal

data class AgentTradeDecision(
    val action: String = "SKIP",
    val productId: String = "BTC-USD",
    val quoteSizeUsd: BigDecimal = BigDecimal.ZERO,
    val confidence: BigDecimal = BigDecimal.ZERO,
    val reason: String = "",
    val score: BigDecimal = BigDecimal.ZERO,
    val baseSize: BigDecimal = BigDecimal.ZERO,
    val reasonCode: String = "NO_CLEAR_EDGE",
    val thesis: String = "",
    val invalidationCondition: String = "",
    val profitTargetPercent: BigDecimal = BigDecimal.ZERO,
    val stopLossPercent: BigDecimal = BigDecimal.ZERO,
    val maxHoldHours: Long = 0,
)
