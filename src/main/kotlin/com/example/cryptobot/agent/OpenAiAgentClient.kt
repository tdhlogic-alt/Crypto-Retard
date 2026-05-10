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
            
            You may only recommend BUY or SKIP.
            
            You must remain risk-aware.
            
            Assign:
            - confidence (0.0-1.0)
            - score (0-100 relative opportunity score)
            
            Prefer higher scores only for the strongest opportunities.
            
            Market snapshot:
            productId=${snapshot.productId}
            price=${snapshot.price}
            change24hPercent=${snapshot.change24hPercent}
            usdAvailable=${snapshot.usdAvailable}
            
            Existing configured buy size: ${botProps.buyQuoteSizeUsd}
            
            Prefer BUY over SKIP when momentum or dip conditions appear favorable, but remain risk-aware.
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
                        "required" to listOf("action", "productId", "quoteSizeUsd", "confidence", "reason")
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
                    baseSize = json["baseSize"].decimalValue(),
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