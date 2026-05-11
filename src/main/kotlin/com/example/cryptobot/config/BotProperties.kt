package com.example.cryptobot.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.math.BigDecimal

@ConfigurationProperties(prefix = "bot")
data class BotProperties(
    val enabled: Boolean = false,
    val dryRun: Boolean = true,
    val fixedRateMs: Long = 900_000,
    val productIds: List<String> = listOf("BTC-USD"),
    val buyQuoteSizeUsd: BigDecimal = BigDecimal("25.00"),
    val minUsdCashReserve: BigDecimal = BigDecimal("500.00"),
    val maxSingleBuyUsd: BigDecimal = BigDecimal("50.00"),
    val maxDailyBuyUsd: BigDecimal = BigDecimal("100.00"),
    val dipThresholdPercent: BigDecimal = BigDecimal("5.0"),
    val discordWebhookUrl: String = "",
    val maxBuyQuoteSizeUsd: BigDecimal = BigDecimal("10.00"),
    val liveTradingEnabled: Boolean = false,
    val agentEnabled: Boolean = false,
    val agentMinConfidence: BigDecimal = BigDecimal("0.70"),
    val maxAssetAllocationPercent: BigDecimal = BigDecimal("35.0"),
    val minProfitPercentForAiSell: BigDecimal = BigDecimal("3.0"),
    val maxDrawdownFromHighPercent: BigDecimal = BigDecimal("8.0"),
    val minAgentEdgeScore: BigDecimal = BigDecimal("55"),
    val strongAgentEdgeScore: BigDecimal = BigDecimal("75"),
    val allowAiSellAtLoss: Boolean = false,
    val aiSellLossFloorPercent: BigDecimal = BigDecimal("-6.0"),
    val maxAiReasonLength: Int = 240,
    val level2RotationEnabled: Boolean = false,
    val minRotationEdgeScore: BigDecimal = BigDecimal("80"),
    val minRotationScoreGap: BigDecimal = BigDecimal("15"),
    val maxRotationSellPercent: BigDecimal = BigDecimal("35.0"),
    val minRotationNotionalUsd: BigDecimal = BigDecimal("10.00"),
)

@ConfigurationProperties(prefix = "coinbase")
data class CoinbaseProperties(
    val baseUrl: String = "https://api.coinbase.com",
    val apiKeyName: String = "",
    val privateKeyPem: String = "",
)

@ConfigurationProperties(prefix = "openai")
data class OpenAiProperties(
    val apiKey: String = "",
    val model: String = "gpt-5.4-mini",
)
