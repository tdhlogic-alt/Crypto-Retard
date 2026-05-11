package com.example.cryptobot.agent

import com.example.cryptobot.config.BotProperties
import com.example.cryptobot.config.OpenAiProperties
import com.example.cryptobot.strategy.MarketSnapshot
import com.fasterxml.jackson.databind.JsonNode
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

    fun decidePortfolio(snapshots: List<MarketSnapshot>): Mono<AgentTradeDecision> {
        val fallbackProduct = snapshots.firstOrNull()?.productId ?: "BTC-USD"
        if (openAiProps.apiKey.isBlank()) {
            return Mono.just(AgentTradeDecision(action = "SKIP", productId = fallbackProduct, reason = "OpenAI API key not configured"))
        }
        if (snapshots.isEmpty()) {
            return Mono.just(AgentTradeDecision(action = "SKIP", productId = fallbackProduct, reason = "No market snapshots available"))
        }

        val portfolioLines = snapshots.joinToString("\n") { s ->
            """
            - ${s.productId}: price=${s.price}, usdAvailable=${s.usdAvailable}, balance=${s.cryptoBalance}, valueUsd=${s.cryptoValueUsd}, allocationPct=${s.portfolioAllocationPercent}, avgCost=${s.avgCostBasis}, unrealizedPnlPct=${s.unrealizedPnlPercent}, unrealizedPnlUsd=${s.unrealizedPnlUsd}, drawdownFromHighPct=${s.drawdownFromHighPercent}, regime=${s.marketRegime}, trend1h=${s.trend1hPercent}, trend4h=${s.trend4hPercent}, trend24h=${s.trend24hPercent}, trend7d=${s.trend7dPercent}, rsi14=${s.rsi14}, volatility24h=${s.volatility24hPercent}, activeThesis=${s.activeThesis.take(120)}, invalidation=${s.activeInvalidationCondition.take(120)}
            """.trimIndent()
        }

        val prompt = """
            You are an AI crypto portfolio manager for a small spot-only Coinbase account.
            Evaluate the entire portfolio in one pass and return exactly one action: BUY, SELL, ROTATE, or SKIP.

            Level 2 ROTATE behavior:
            - Use ROTATE only when a new BUY opportunity is meaningfully stronger than a currently held weak asset.
            - ROTATE means: sell part of fundingProductId first, then buy productId.
            - Do not rotate just to churn; require a clear score gap and a strong edge.
            - fundingProductId must be a held asset with balance > 0 and must be different from productId.
            - fundingBaseSize should normally be 10%-35% of that holding, never the whole position unless risk is extreme.
            - quoteSizeUsd is the intended BUY size after the funding sell. Keep it <= ${botProps.maxBuyQuoteSizeUsd}.
            - Avoid selling a position at a small loss unless thesis is invalidated, downside momentum is severe, or the new opportunity is substantially stronger.

            BUY behavior:
            - Use BUY when available USD can fund the trade without violating cash reserve.
            - Prefer BUY over ROTATE when there is already enough USD.

            SELL behavior:
            - Use SELL for profit protection, trailing stop, stop loss, or thesis invalidation.
            - Prefer partial sells, usually 25%-50% of the held asset.

            Risk controls and configured limits:
            allowedProducts=${botProps.productIds}
            usdCashReserve=${botProps.minUsdCashReserve}
            configuredBuySize=${botProps.buyQuoteSizeUsd}
            maxBuySize=${botProps.maxBuyQuoteSizeUsd}
            maxAssetAllocationPct=${botProps.maxAssetAllocationPercent}
            minRotationEdgeScore=${botProps.minRotationEdgeScore}
            minRotationScoreGap=${botProps.minRotationScoreGap}
            maxRotationSellPct=${botProps.maxRotationSellPercent}
            minRotationNotionalUsd=${botProps.minRotationNotionalUsd}

            Return fields:
            - action: BUY, SELL, ROTATE, or SKIP
            - productId: BUY target for BUY/ROTATE, SELL target for SELL, best watched product for SKIP
            - quoteSizeUsd: BUY size for BUY/ROTATE, otherwise 0
            - baseSize: SELL size for SELL, otherwise 0
            - fundingProductId: asset to sell first for ROTATE, otherwise empty string
            - fundingBaseSize: base units to sell first for ROTATE, otherwise 0
            - fundingReason: concise reason for funding sell, otherwise empty string
            - confidence: 0.0-1.0
            - score: 0-100 opportunity score for the primary action
            - reasonCode: one of OVERSOLD_BOUNCE, BREAKOUT_CONTINUATION, MOMENTUM_REVERSAL, PROFIT_PROTECTION, TRAILING_STOP, STOP_LOSS, THESIS_INVALIDATED, REBALANCE, NO_CLEAR_EDGE
            - thesis/invalidationCondition/profitTargetPercent/stopLossPercent/maxHoldHours: required for BUY/ROTATE target; otherwise empty/0.
            - reason: <= ${botProps.maxAiReasonLength} chars.

            Portfolio snapshots:
            $portfolioLines
        """.trimIndent()

        return callOpenAi(prompt, fallbackProduct)
    }

    fun decide(snapshot: MarketSnapshot): Mono<AgentTradeDecision> {
        if (openAiProps.apiKey.isBlank()) {
            return Mono.just(AgentTradeDecision(action = "SKIP", productId = snapshot.productId, reason = "OpenAI API key not configured"))
        }

        val prompt = """
            You are an AI crypto swing trading agent.
            Evaluate this single asset and recommend BUY, SELL, or SKIP.
            Consider momentum, volatility, RSI14, proximity to highs/lows, risk/reward, current position, P&L, thesis, and allocation.
            Do not recommend SELL for assets with zero balance. Prefer partial exits for SELL.
            Existing configured buy size: ${botProps.buyQuoteSizeUsd}
            Max buy size allowed: ${botProps.maxBuyQuoteSizeUsd}
            Keep reason <= ${botProps.maxAiReasonLength} characters.

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
            unrealizedPnlUsd=${snapshot.unrealizedPnlUsd}
            unrealizedPnlPercent=${snapshot.unrealizedPnlPercent}
            drawdownFromHighPercent=${snapshot.drawdownFromHighPercent}
            marketRegime=${snapshot.marketRegime}
            activeThesis=${snapshot.activeThesis}
            activeInvalidationCondition=${snapshot.activeInvalidationCondition}
            trend1hPercent=${snapshot.trend1hPercent}
            trend4hPercent=${snapshot.trend4hPercent}
            trend24hPercent=${snapshot.trend24hPercent}
            trend7dPercent=${snapshot.trend7dPercent}
            rsi14=${snapshot.rsi14}
            volatility24hPercent=${snapshot.volatility24hPercent}
        """.trimIndent()

        return callOpenAi(prompt, snapshot.productId)
    }

    private fun callOpenAi(prompt: String, fallbackProductId: String): Mono<AgentTradeDecision> {
        val request = mapOf(
            "model" to openAiProps.model,
            "input" to prompt,
            "text" to mapOf(
                "format" to mapOf(
                    "type" to "json_schema",
                    "name" to "agent_trade_decision",
                    "strict" to true,
                    "schema" to decisionSchema()
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
                    .flatMap { body -> Mono.error(RuntimeException("OpenAI failed: status=${response.statusCode()} body=$body")) }
            }
            .bodyToMono(String::class.java)
            .map { body ->
                val root = objectMapper.readTree(body)
                val text = root["output"]?.firstOrNull()
                    ?.get("content")?.firstOrNull()
                    ?.get("text")?.asText()
                    ?: error("No structured output text from OpenAI")
                parseDecision(objectMapper.readTree(text))
            }
            .onErrorResume { ex ->
                Mono.just(AgentTradeDecision(action = "SKIP", productId = fallbackProductId, reason = "OpenAI agent failed: ${ex.message}"))
            }
    }

    private fun parseDecision(json: JsonNode): AgentTradeDecision = AgentTradeDecision(
        action = json["action"].asText(),
        productId = json["productId"].asText(),
        quoteSizeUsd = json["quoteSizeUsd"].asText().toBigDecimal(),
        confidence = json["confidence"].decimalValue(),
        reason = json["reason"].asText(),
        score = BigDecimal(json["score"].asText()),
        baseSize = json["baseSize"].asText().toBigDecimal(),
        reasonCode = json["reasonCode"].asText(),
        thesis = json["thesis"].asText(),
        invalidationCondition = json["invalidationCondition"].asText(),
        profitTargetPercent = json["profitTargetPercent"].asText().toBigDecimal(),
        stopLossPercent = json["stopLossPercent"].asText().toBigDecimal(),
        maxHoldHours = json["maxHoldHours"].asLong(),
        fundingProductId = json["fundingProductId"].asText(),
        fundingBaseSize = json["fundingBaseSize"].asText().toBigDecimal(),
        fundingReason = json["fundingReason"].asText(),
    )

    private fun decisionSchema(): Map<String, Any> = mapOf(
        "type" to "object",
        "additionalProperties" to false,
        "properties" to mapOf(
            "action" to mapOf("type" to "string", "enum" to listOf("BUY", "SELL", "ROTATE", "SKIP")),
            "productId" to mapOf("type" to "string"),
            "quoteSizeUsd" to mapOf("type" to "string"),
            "confidence" to mapOf("type" to "number"),
            "reason" to mapOf("type" to "string"),
            "score" to mapOf("type" to "number"),
            "baseSize" to mapOf("type" to "string"),
            "fundingProductId" to mapOf("type" to "string"),
            "fundingBaseSize" to mapOf("type" to "string"),
            "fundingReason" to mapOf("type" to "string"),
            "reasonCode" to mapOf("type" to "string", "enum" to listOf(
                "OVERSOLD_BOUNCE", "BREAKOUT_CONTINUATION", "MOMENTUM_REVERSAL", "PROFIT_PROTECTION",
                "TRAILING_STOP", "STOP_LOSS", "THESIS_INVALIDATED", "REBALANCE", "NO_CLEAR_EDGE"
            )),
            "thesis" to mapOf("type" to "string"),
            "invalidationCondition" to mapOf("type" to "string"),
            "profitTargetPercent" to mapOf("type" to "string"),
            "stopLossPercent" to mapOf("type" to "string"),
            "maxHoldHours" to mapOf("type" to "integer"),
        ),
        "required" to listOf(
            "action", "productId", "quoteSizeUsd", "baseSize", "fundingProductId", "fundingBaseSize", "fundingReason",
            "confidence", "reason", "score", "reasonCode", "thesis", "invalidationCondition",
            "profitTargetPercent", "stopLossPercent", "maxHoldHours"
        )
    )
}
