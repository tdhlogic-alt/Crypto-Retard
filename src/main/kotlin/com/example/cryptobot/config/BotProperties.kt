package com.example.cryptobot.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.math.BigDecimal
import java.time.Duration

@ConfigurationProperties(prefix = "bot")
data class BotProperties(
    val enabled: Boolean = false,
    val dryRun: Boolean = true,
    val fixedRateMs: Long = 900_000,
    val productIds: List<String> = listOf("BTC-USD"),
    val buyQuoteSizeUsd: BigDecimal = BigDecimal("25.00"),
    val minUsdCashReserve: BigDecimal = BigDecimal("500.00"),
    val maxSingleBuyUsd: BigDecimal = BigDecimal("50.00"),
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
    val maxActionsPerRun: Int = 3,
    val maxBuysPerRun: Int = 2,
    val maxSellsPerRun: Int = 2,
    val maxRotationsPerRun: Int = 1,
    val maxTotalBuyUsdPerRun: BigDecimal = BigDecimal("80.00"),
    val maxDailyLiveBuysPerProduct: Int = 2,
    val buyCooldownHours: Long = 12,
    val postSellCooldownHours: Long = 12,
    val maxDailyLiveBuys: Int = 10,
    val bearTrendMinBuyScore: BigDecimal = BigDecimal("90"),
    val oversoldBounceMaxRsi: BigDecimal = BigDecimal("35"),
    val oversoldBounceMinRecoveryTrendPercent: BigDecimal = BigDecimal("0.0"),
    val minSellNotionalUsd: BigDecimal = BigDecimal("10.00"),
    val strategyModeEnabled: Boolean = true,
    val strategyAiVetoEnabled: Boolean = true,
    val strategyQuoteSizeUsd: BigDecimal = BigDecimal("15.00"),
    val strategyStopLossPercent: BigDecimal = BigDecimal("4.0"),
    val strategyTakeProfitPercent: BigDecimal = BigDecimal("8.0"),
    val strategyTrailingDrawdownPercent: BigDecimal = BigDecimal("4.0"),
    val strategyMinTrend7dPercent: BigDecimal = BigDecimal("2.0"),
    val strategyMinRecovery4hPercent: BigDecimal = BigDecimal("0.0"),
    val strategyMaxEntry24hPercent: BigDecimal = BigDecimal("4.0"),
    val strategyMaxEntryRsi: BigDecimal = BigDecimal("60"),
    val strategyMaxHoldHours: Long = 168,
    val exitOnCompletion: Boolean = true,
)

@ConfigurationProperties(prefix = "coinbase")
data class CoinbaseProperties(
    val baseUrl: String = "https://api.coinbase.com",
    val apiKeyName: String = "",
    val privateKeyPem: String = "",
    val productCacheTtl: Duration = Duration.ofHours(6),
    val rateLimitForPeriod: Int = 6,
    val rateLimitRefreshPeriod: Duration = Duration.ofSeconds(1),
    val rateLimitTimeout: Duration = Duration.ofSeconds(5),
    val retryMaxAttempts: Long = 3,
    val retryInitialBackoff: Duration = Duration.ofSeconds(2),
    val retryMaxBackoff: Duration = Duration.ofSeconds(20),
    val snapshotConcurrency: Int = 3,
)

@ConfigurationProperties(prefix = "openai")
data class OpenAiProperties(
    val apiKey: String = "",
    val model: String = "gpt-5.4-mini",
)
