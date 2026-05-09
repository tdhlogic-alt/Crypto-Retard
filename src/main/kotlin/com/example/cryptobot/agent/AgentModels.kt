package com.example.cryptobot.agent

import java.math.BigDecimal

data class AgentTradeDecision(
    val action: String = "SKIP",
    val productId: String = "BTC-USD",
    val quoteSizeUsd: BigDecimal = BigDecimal.ZERO,
    val confidence: BigDecimal = BigDecimal.ZERO,
    val reason: String = "",
)