package com.example.cryptobot.persistence

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.OffsetDateTime

@Table("trade_decision_log")
data class TradeDecisionLog(
    @Id
    val id: Long? = null,
    val createdAt: OffsetDateTime? = null,
    val productId: String,
    val decisionType: String,
    val reason: String,
    val price: BigDecimal?,
    val change24hPercent: BigDecimal?,
    val usdAvailable: BigDecimal?,
    val quoteSizeUsd: BigDecimal?,
    val dryRun: Boolean,
    val coinbaseOrderId: String? = null,
    val coinbaseSuccess: Boolean? = null,
    val errorMessage: String? = null,
)