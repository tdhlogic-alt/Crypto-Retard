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
    val model: String = "gpt-5.5-mini",
)
