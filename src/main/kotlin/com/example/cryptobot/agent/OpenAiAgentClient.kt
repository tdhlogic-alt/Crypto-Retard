package com.example.cryptobot.agent

import com.example.cryptobot.config.BotProperties
import com.example.cryptobot.config.OpenAiProperties
import com.example.cryptobot.strategy.MarketSnapshot
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.core.publisher.Mono
import java.math.BigDecimal

@Component
class OpenAiAgentClient(
    private val openAiProps: OpenAiProperties,
    private val botProps: BotProperties,
    private val objectMapper: ObjectMapper,
) {
    private val webClient = WebClient.builder()
        .baseUrl("https://api.openai.com")
        .build()

    fun decide(snapshot: MarketSnapshot): Mono<AgentTradeDecision> {
        if (openAiProps.apiKey.isBlank()) {
            return Mono.just(
                AgentTradeDecision(
                    action = "SKIP",
                    productId = snapshot.productId,
                    reason = "OpenAI API key not configured",
                )
            )
        }

        val prompt = """
            You are an AI crypto swing trading agent.
            
            Your job is to evaluate whether this asset is an attractive short-term trading opportunity relative to other crypto assets.
            
            Consider:
            - 24h momentum
            - volatility
            - proximity to highs/lows
            - volume
            - dip-buy opportunities
            - trend continuation potential
            - risk/reward
            - 1h, 4h, and 24h trend direction
            - RSI14 overbought/oversold state
            - 24h volatility
            - proximity to 24h candle high/low
            - volume and momentum confirmation
            
            You may recommend BUY, SELL, or SKIP.
            
            You should be willing to recommend BUY or SELL when there is a moderate short-term opportunity, even if the signal is not perfect. Avoid overtrading, but do not require extreme conviction.            
            Assign:
            - confidence (0.0-1.0)
            - score (0-100 relative opportunity score)
            
            Prefer higher scores only for the strongest opportunities.
            
            Market snapshot:
            productId=${snapshot.productId}
            price=${snapshot.price}
            change24hPercent=${snapshot.change24hPercent}
            usdAvailable=${snapshot.usdAvailable}
            cryptoBalance=${snapshot.cryptoBalance}
            cryptoValueUsd=${snapshot.cryptoValueUsd}
            portfolioUsdValue=${snapshot.portfolioUsdValue}
            portfolioAllocationPercent=${snapshot.portfolioAllocationPercent}
            avgCostBasis=${snapshot.avgCostBasis}
            totalInvested=${snapshot.totalInvested}
            realizedPnlUsd=${snapshot.realizedPnlUsd}
            unrealizedPnlUsd=${snapshot.unrealizedPnlUsd}
            unrealizedPnlPercent=${snapshot.unrealizedPnlPercent}
            highestPriceSeen=${snapshot.highestPriceSeen}
            drawdownFromHighPercent=${snapshot.drawdownFromHighPercent}
            buyCount=${snapshot.buyCount}
            sellCount=${snapshot.sellCount}
            trend1hPercent=${snapshot.trend1hPercent}
            trend4hPercent=${snapshot.trend4hPercent}
            trend24hPercent=${snapshot.trend24hPercent}
            rsi14=${snapshot.rsi14}
            volatility24hPercent=${snapshot.volatility24hPercent}
            candleHigh24h=${snapshot.candleHigh24h}
            candleLow24h=${snapshot.candleLow24h}
            trend7dPercent=${snapshot.trend7dPercent}
            candleHigh7d=${snapshot.candleHigh7d}
            candleLow7d=${snapshot.candleLow7d}
            
            Use portfolio and position awareness:
            - Avoid buying more of an asset that already has a large allocation.
            - Avoid repeated averaging down unless the setup is strong and risk/reward improved.
            - Consider BUY more favorably when current price is below avgCostBasis but trend/RSI/volume suggest recovery.
            - Consider SELL only if cryptoBalance > 0.
            - Do not recommend SELL for assets with zero balance.
            - Consider partial SELL when unrealizedPnlPercent is meaningfully positive and momentum is weakening.
            - Consider SELL when drawdownFromHighPercent is large after a profitable move, as a trailing-stop style exit.
            - Avoid SELL solely because price is below avgCostBasis unless the thesis is invalidated or downside momentum is severe.
            - Prefer diversifying across allowed assets when opportunity quality is similar.
            
            Existing configured buy size: ${botProps.buyQuoteSizeUsd}
            Max buy size allowed: ${botProps.maxBuyQuoteSizeUsd}
            
            Prefer BUY over SKIP when momentum or dip conditions appear favorable, but remain risk-aware.
            For SELL, prefer partial exits; baseSize should usually be 25%-50% of cryptoBalance unless risk is extreme.
        """.trimIndent()

        val request = mapOf(
            "model" to openAiProps.model,
            "input" to prompt,
            "text" to mapOf(
                "format" to mapOf(
                    "type" to "json_schema",
                    "name" to "agent_trade_decision",
                    "strict" to true,
                    "schema" to mapOf(
                        "type" to "object",
                        "additionalProperties" to false,
                        "properties" to mapOf(
                            "action" to mapOf("type" to "string", "enum" to listOf("BUY", "SELL", "SKIP")),
                            "productId" to mapOf("type" to "string"),
                            "quoteSizeUsd" to mapOf("type" to "string"),
                            "confidence" to mapOf("type" to "number"),
                            "reason" to mapOf("type" to "string"),
                            "score" to mapOf("type" to "number"),
                            "baseSize" to mapOf("type" to "string"),
                        ),
                        "required" to listOf(
                            "action",
                            "productId",
                            "quoteSizeUsd",
                            "baseSize",
                            "confidence",
                            "reason",
                            "score"
                        )
                    )
                )
            ),
        )

        return webClient.post()
            .uri("/v1/responses")
            .headers { it.setBearerAuth(openAiProps.apiKey) }
            .bodyValue(request)
            .retrieve()
            .onStatus({ it.isError }) { response ->
                response.bodyToMono<String>()
                    .defaultIfEmpty("")
                    .flatMap { body ->
                        Mono.error(RuntimeException("OpenAI failed: status=${response.statusCode()} body=$body"))
                    }
            }
            .bodyToMono(String::class.java)
            .map { body ->
                val root = objectMapper.readTree(body)
                val text = root["output"]?.firstOrNull()
                    ?.get("content")?.firstOrNull()
                    ?.get("text")?.asText()
                    ?: error("No structured output text from OpenAI")

                val json = objectMapper.readTree(text)

                AgentTradeDecision(
                    action = json["action"].asText(),
                    productId = json["productId"].asText(),
                    quoteSizeUsd = json["quoteSizeUsd"].asText().toBigDecimal(),
                    confidence = json["confidence"].decimalValue(),
                    reason = json["reason"].asText(),
                    score = BigDecimal(json["score"].asText()),
                    baseSize = json["baseSize"].asText().toBigDecimal(),
                )
            }
            .onErrorResume { ex ->
                Mono.just(
                    AgentTradeDecision(
                        action = "SKIP",
                        productId = snapshot.productId,
                        confidence = java.math.BigDecimal.ZERO,
                        reason = "OpenAI agent failed: ${ex.message}",
                    )
                )
            }
    }
}