package com.example.cryptobot.agent

import com.example.cryptobot.config.BotProperties
import com.example.cryptobot.config.OpenAiProperties
import com.example.cryptobot.strategy.MarketSnapshot
import com.example.cryptobot.strategy.StrategySignal
import com.example.cryptobot.strategy.TradingDecision
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

    fun decidePortfolio(snapshots: List<MarketSnapshot>): Mono<AgentTradeDecision> =
        decidePortfolioPlan(snapshots).map { plan ->
            plan.decisions.firstOrNull() ?: AgentTradeDecision(
                action = "SKIP",
                productId = snapshots.firstOrNull()?.productId ?: "BTC-USD",
                reason = "Agent returned an empty plan",
            )
        }


    fun decideStrategyVetoPlan(
        snapshots: List<MarketSnapshot>,
        proposedSignals: List<StrategySignal>,
    ): Mono<AgentTradePlan> {
        val fallbackProduct = snapshots.firstOrNull()?.productId ?: "BTC-USD"
        if (openAiProps.apiKey.isBlank()) {
            return Mono.just(AgentTradePlan(proposedSignals.map { signal ->
                AgentTradeDecision(action = "SKIP", productId = signal.productId(), reason = "OpenAI API key not configured; vetoing non-hard-exit strategy entry")
            }))
        }
        if (proposedSignals.isEmpty()) {
            return Mono.just(AgentTradePlan(listOf(AgentTradeDecision(action = "SKIP", productId = fallbackProduct, reason = "No strategy signals to review"))))
        }

        val portfolioLines = snapshots.joinToString("\n") { s ->
            "- ${s.productId}: price=${s.price}, balance=${s.cryptoBalance}, valueUsd=${s.cryptoValueUsd}, allocationPct=${s.portfolioAllocationPercent}, pnlPct=${s.unrealizedPnlPercent}, drawdown=${s.drawdownFromHighPercent}, regime=${s.marketRegime}, trend1h=${s.trend1hPercent}, trend4h=${s.trend4hPercent}, trend24h=${s.trend24hPercent}, trend7d=${s.trend7dPercent}, rsi14=${s.rsi14}, volatility24h=${s.volatility24hPercent}"
        }
        val signalLines = proposedSignals.joinToString("\n") { signal ->
            val d = signal.decision
            when (d) {
                is TradingDecision.Buy -> "- PROPOSED BUY ${d.productId}: quoteSizeUsd=${d.quoteSizeUsd}, reasonCode=${d.reasonCode}, thesis=${d.thesis.take(160)}, stopLossPct=${d.stopLossPercent}, profitTargetPct=${d.profitTargetPercent}, rationale=${signal.rationale}"
                is TradingDecision.Sell -> "- PROPOSED SELL ${d.productId}: baseSize=${d.baseSize}, reasonCode=${d.reasonCode}, rationale=${signal.rationale}"
                is TradingDecision.Rotate -> "- PROPOSED ROTATE ${d.sell.productId}->${d.buy.productId}: buyUsd=${d.buy.quoteSizeUsd}, rationale=${signal.rationale}"
                is TradingDecision.Skip -> "- PROPOSED SKIP: ${d.reason}"
            }
        }

        val prompt = """
            You are only a veto layer for a crypto trading bot. You are not the trader.
            The deterministic strategy engine has already generated the only allowed candidate trades below.

            Rules:
            - You may not invent new trades.
            - You may not change productId, action, size, stop loss, take profit, thesis, or max hold.
            - To approve a candidate, repeat the same action and productId. Use confidence >= ${botProps.agentMinConfidence} only when approval is strong.
            - To veto a candidate, return SKIP for that product with a concise reason.
            - Prefer veto when the setup is choppy, overextended, low-liquidity, regime-conflicted, or the risk/reward is unclear.
            - Hard exits are not sent to you; never block deterministic risk exits.

            Proposed deterministic candidates:
            $signalLines

            Current portfolio/market snapshots:
            $portfolioLines
        """.trimIndent()

        return callOpenAiPlan(prompt, fallbackProduct)
    }

    private fun StrategySignal.productId(): String = when (val d = decision) {
        is TradingDecision.Buy -> d.productId
        is TradingDecision.Sell -> d.productId
        is TradingDecision.Rotate -> d.buy.productId
        is TradingDecision.Skip -> "BTC-USD"
    }

    fun decidePortfolioPlan(snapshots: List<MarketSnapshot>): Mono<AgentTradePlan> {
        val fallbackProduct = snapshots.firstOrNull()?.productId ?: "BTC-USD"
        if (openAiProps.apiKey.isBlank()) {
            return Mono.just(AgentTradePlan(listOf(AgentTradeDecision(action = "SKIP", productId = fallbackProduct, reason = "OpenAI API key not configured"))))
        }
        if (snapshots.isEmpty()) {
            return Mono.just(AgentTradePlan(listOf(AgentTradeDecision(action = "SKIP", productId = fallbackProduct, reason = "No market snapshots available"))))
        }

        val portfolioLines = snapshots.joinToString("\n") { s ->
            """
            - ${s.productId}: price=${s.price}, usdAvailable=${s.usdAvailable}, balance=${s.cryptoBalance}, valueUsd=${s.cryptoValueUsd}, allocationPct=${s.portfolioAllocationPercent}, avgCost=${s.avgCostBasis}, unrealizedPnlPct=${s.unrealizedPnlPercent}, unrealizedPnlUsd=${s.unrealizedPnlUsd}, drawdownFromHighPct=${s.drawdownFromHighPercent}, regime=${s.marketRegime}, trend1h=${s.trend1hPercent}, trend4h=${s.trend4hPercent}, trend24h=${s.trend24hPercent}, trend7d=${s.trend7dPercent}, rsi14=${s.rsi14}, volatility24h=${s.volatility24hPercent}, reasonCode30dWinRate=${s.reasonCode30dWinRate}, reasonCode30dCount=${s.reasonCode30dCount}, activeThesis=${s.activeThesis.take(120)}, invalidation=${s.activeInvalidationCondition.take(120)}
            """.trimIndent()
        }

        val prompt = """
            You are an AI crypto portfolio manager for a small spot-only Coinbase account.
            Evaluate the entire portfolio in one pass and return a ranked action plan containing up to ${botProps.maxActionsPerRun} actions.
            Each action may be BUY, SELL, ROTATE, or SKIP. Return SKIP only when there are no executable BUY/SELL/ROTATE actions.

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
            - Use the product-level 30d reason-code stats as a feedback loop. If reasonCode30dCount >= 5 and reasonCode30dWinRate < 45, require score >= ${botProps.strongAgentEdgeScore} and a clearly improving 1h/4h setup before proposing that same style of BUY.
            - If reasonCode30dCount >= 5 and reasonCode30dWinRate >= 60, you may slightly prefer that setup, but only when the current regime/trend/RSI still independently support it.
            - Never chase a broader product universe just because an asset is volatile. More products means more false positives; pick only the top 1-2 risk-adjusted opportunities.

            SELL behavior:
            - Use SELL for profit protection, trailing stop, stop loss, or true thesis invalidation.
            - Do not use THESIS_INVALIDATED as a generic "market looks weak" reason. Use it only when the original buy thesis is specifically broken and there is hard evidence: pnl <= ${botProps.aiSellLossFloorPercent}%, drawdown >= ${botProps.maxDrawdownFromHighPercent}%, CRASH/BEAR_TREND regime, or confidence >= 0.80 with score >= ${botProps.strongAgentEdgeScore}.
            - If the setup is merely softer, choppy, or mildly red, prefer SKIP unless PROFIT_PROTECTION, TRAILING_STOP, or STOP_LOSS clearly applies.
            - Prefer partial sells, usually 25%-50% of the held asset.

            Risk controls and configured limits:
            allowedProducts=${botProps.productIds}
            productUniverseGuidance=This is a broadened watchlist, not a mandate to trade. Treat lower-liquidity/high-beta assets as requiring stronger evidence and cleaner setups than BTC/ETH/SOL.
            usdCashReserve=${botProps.minUsdCashReserve}
            configuredBuySize=${botProps.buyQuoteSizeUsd}
            maxBuySize=${botProps.maxBuyQuoteSizeUsd}
            maxAssetAllocationPct=${botProps.maxAssetAllocationPercent}
            minRotationEdgeScore=${botProps.minRotationEdgeScore}
            minRotationScoreGap=${botProps.minRotationScoreGap}
            maxRotationSellPct=${botProps.maxRotationSellPercent}
            minRotationNotionalUsd=${botProps.minRotationNotionalUsd}

            Capital preservation mode:
            - The account is recovering from live-trading drawdown. Prioritize avoiding additional downside over catching upside.
            - If maxBuysPerRun is 0 or maxTotalBuyUsdPerRun is 0, do not propose BUY or ROTATE actions. Return only meaningful SELL actions or SKIP.
            - Do not propose a BUY in CRASH. Do not propose a BUY in BEAR_TREND unless score >= ${botProps.bearTrendMinBuyScore}.
            - Do not propose OVERSOLD_BOUNCE unless RSI <= ${botProps.oversoldBounceMaxRsi} and both 1h/4h trends are recovering above ${botProps.oversoldBounceMinRecoveryTrendPercent}%.
            - Avoid tiny partial exits. SELL notional should be >= ${botProps.minSellNotionalUsd} unless selling the entire remaining dust position.
            - Prefer SKIP over low-conviction trades. It is acceptable and often correct to return no executable trades.

            Multi-action planning rules:
            - Return only actions worth executing in this scheduled run. Do not fill the plan just because slots exist.
            - When many products look similar, select the best risk-adjusted action and return SKIP for the rest rather than forcing activity.
            - Rank actions from most urgent/highest edge to lowest edge.
            - Never include more than ${botProps.maxBuysPerRun} BUY actions, ${botProps.maxSellsPerRun} SELL actions, or ${botProps.maxRotationsPerRun} ROTATE actions.
            - Do not include two actions that buy the same product in the same run.
            - Do not include two actions that sell the same funding/held product in the same run unless risk is extreme.
            - Treat total new BUY spending as capped by maxTotalBuyUsdPerRun=${botProps.maxTotalBuyUsdPerRun} and available USD after reserve.
            - Prefer urgent risk-reduction SELLs before BUYs; prefer BUY over ROTATE when cash is already available.

            Each decision object fields:
            - action: BUY, SELL, ROTATE, or SKIP
            - productId: BUY target for BUY/ROTATE, SELL target for SELL, best watched product for SKIP
            - quoteSizeUsd: BUY size for BUY/ROTATE, otherwise 0
            - baseSize: SELL size for SELL, otherwise 0
            - fundingProductId: asset to sell first for ROTATE, otherwise empty string
            - fundingBaseSize: base units to sell first for ROTATE, otherwise 0
            - fundingReason: concise reason for funding sell, otherwise empty string
            - confidence: 0.0-1.0
            - score: 0-100 opportunity score for the primary action
            - reasonCode: one of TREND_PULLBACK, OVERSOLD_BOUNCE, BREAKOUT_CONTINUATION, MOMENTUM_ACCELERATION, PROFIT_PROTECTION, TRAILING_STOP, STOP_LOSS, THESIS_INVALIDATED, REBALANCE, RELATIVE_STRENGTH_ROTATION_BUY, RELATIVE_STRENGTH_ROTATION_SELL, NO_CLEAR_EDGE
            - thesis/invalidationCondition/profitTargetPercent/stopLossPercent/maxHoldHours: required for BUY/ROTATE target; otherwise empty/0.
            - reason: <= ${botProps.maxAiReasonLength} chars.

            Portfolio snapshots:
            $portfolioLines
        """.trimIndent()

        return callOpenAiPlan(prompt, fallbackProduct)
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
            Performance feedback rules:
            - reasonCode30dWinRate and reasonCode30dCount summarize recent realized performance for this asset's prior reason-code setups.
            - If reasonCode30dCount >= 5 and reasonCode30dWinRate < 45, do not repeat that same setup unless score >= ${botProps.strongAgentEdgeScore} and 1h/4h trends confirm recovery/continuation.
            - If reasonCode30dCount >= 5 and reasonCode30dWinRate >= 60, that is supportive but not sufficient by itself; current market data still has to justify the trade.

            Capital preservation mode rules:
            - If maxBuysPerRun is 0 or maxTotalBuyUsdPerRun is 0, do not recommend BUY. Recommend SELL only for real risk reduction, otherwise SKIP.
            - Never recommend BUY in CRASH. In BEAR_TREND, BUY requires score >= ${botProps.bearTrendMinBuyScore}.
            - OVERSOLD_BOUNCE requires RSI <= ${botProps.oversoldBounceMaxRsi} and both 1h/4h trends recovering above ${botProps.oversoldBounceMinRecoveryTrendPercent}%.
            - Avoid tiny SELLs below minSellNotionalUsd=${botProps.minSellNotionalUsd} unless exiting dust.
            Existing configured buy size: ${botProps.buyQuoteSizeUsd}
            Max buy size allowed: ${botProps.maxBuyQuoteSizeUsd}
            maxBuysPerRun=${botProps.maxBuysPerRun}
            maxTotalBuyUsdPerRun=${botProps.maxTotalBuyUsdPerRun}
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
            reasonCode30dWinRate=${snapshot.reasonCode30dWinRate}
            reasonCode30dCount=${snapshot.reasonCode30dCount}
        """.trimIndent()

        return callOpenAi(prompt, snapshot.productId)
    }

    private fun callOpenAiPlan(prompt: String, fallbackProductId: String): Mono<AgentTradePlan> {
        val request = mapOf(
            "model" to openAiProps.model,
            "input" to prompt,
            "text" to mapOf(
                "format" to mapOf(
                    "type" to "json_schema",
                    "name" to "agent_trade_plan",
                    "strict" to true,
                    "schema" to planSchema()
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
                parsePlan(objectMapper.readTree(text))
            }
            .onErrorResume { ex ->
                Mono.just(AgentTradePlan(listOf(AgentTradeDecision(action = "SKIP", productId = fallbackProductId, reason = "OpenAI agent failed: ${ex.message} due to ${ex.cause} with stacktrace: ${ex.stackTraceToString()}"))))
            }
    }

    private fun callOpenAi(prompt: String, fallbackProductId: String): Mono<AgentTradeDecision> =
        callOpenAiPlan(prompt, fallbackProductId).map { plan ->
            plan.decisions.firstOrNull() ?: AgentTradeDecision(action = "SKIP", productId = fallbackProductId, reason = "Agent returned an empty plan")
        }

    private fun parsePlan(json: JsonNode): AgentTradePlan {
        val decisions = json["decisions"]
            ?.map { parseDecision(it) }
            ?.take(botProps.maxActionsPerRun)
            ?: emptyList()
        return AgentTradePlan(decisions)
    }

    private fun JsonNode.textOrDefault(field: String, default: String): String {
        val value = this[field] ?: return default
        if (value.isNull) return default
        val text = value.asText(default).trim()
        return text.ifBlank { default }
    }

    private fun JsonNode.decimalOrZero(field: String): BigDecimal =
        this[field]?.takeUnless { it.isNull }?.asText()?.trim()?.takeIf { it.isNotBlank() }
            ?.let { runCatching { BigDecimal(it) }.getOrDefault(BigDecimal.ZERO) }
            ?: BigDecimal.ZERO

    private fun JsonNode.longOrZero(field: String): Long =
        this[field]?.takeUnless { it.isNull }?.asText()?.trim()?.toLongOrNull() ?: 0L

    private fun parseDecision(json: JsonNode): AgentTradeDecision {
        val action = json.textOrDefault("action", "SKIP").uppercase()
        val safeAction = if (action in setOf("BUY", "SELL", "ROTATE", "SKIP")) action else "SKIP"

        return AgentTradeDecision(
            action = safeAction,
            productId = json.textOrDefault("productId", "BTC-USD"),
            quoteSizeUsd = json.decimalOrZero("quoteSizeUsd"),
            confidence = json.decimalOrZero("confidence"),
            reason = json.textOrDefault("reason", "No reason provided"),
            score = json.decimalOrZero("score"),
            baseSize = json.decimalOrZero("baseSize"),
            reasonCode = json.textOrDefault("reasonCode", "NO_CLEAR_EDGE"),
            thesis = json.textOrDefault("thesis", ""),
            invalidationCondition = json.textOrDefault("invalidationCondition", ""),
            profitTargetPercent = json.decimalOrZero("profitTargetPercent"),
            stopLossPercent = json.decimalOrZero("stopLossPercent"),
            maxHoldHours = json.longOrZero("maxHoldHours"),
            fundingProductId = json.textOrDefault("fundingProductId", ""),
            fundingBaseSize = json.decimalOrZero("fundingBaseSize"),
            fundingReason = json.textOrDefault("fundingReason", ""),
        )
    }

    private fun planSchema(): Map<String, Any> = mapOf(
        "type" to "object",
        "additionalProperties" to false,
        "properties" to mapOf(
            "decisions" to mapOf(
                "type" to "array",
                "minItems" to 1,
                "maxItems" to botProps.maxActionsPerRun,
                "items" to decisionSchema(),
            )
        ),
        "required" to listOf("decisions"),
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
                "TREND_PULLBACK", "OVERSOLD_BOUNCE", "BREAKOUT_CONTINUATION", "MOMENTUM_ACCELERATION",
                "PROFIT_PROTECTION", "TRAILING_STOP", "STOP_LOSS", "THESIS_INVALIDATED", "REBALANCE",
                "RELATIVE_STRENGTH_ROTATION_BUY", "RELATIVE_STRENGTH_ROTATION_SELL", "NO_CLEAR_EDGE"
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
