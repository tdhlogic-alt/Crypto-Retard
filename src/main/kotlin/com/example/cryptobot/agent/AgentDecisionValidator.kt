package com.example.cryptobot.agent

import com.example.cryptobot.config.BotProperties
import com.example.cryptobot.strategy.MarketSnapshot
import com.example.cryptobot.strategy.TradingDecision
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.RoundingMode

@Component
class AgentDecisionValidator(
    private val props: BotProperties,
) {
    fun validate(snapshot: MarketSnapshot, decision: AgentTradeDecision): TradingDecision =
        validatePortfolio(listOf(snapshot), decision)

    fun validatePortfolio(snapshots: List<MarketSnapshot>, decision: AgentTradeDecision): TradingDecision {
        if (decision.action == "SKIP") {
            return TradingDecision.Skip("Agent skipped: ${decision.reason}")
        }

        if (decision.productId !in props.productIds) {
            return TradingDecision.Skip("Agent product rejected: ${decision.productId}. Allowed=${props.productIds}")
        }

        if (decision.confidence < props.agentMinConfidence) {
            return TradingDecision.Skip("Agent confidence too low: ${decision.confidence}")
        }

        if (decision.score < props.minAgentEdgeScore && decision.action != "SKIP") {
            return TradingDecision.Skip("Agent edge score too low: ${decision.score} < ${props.minAgentEdgeScore}")
        }

        val snapshotByProduct = snapshots.associateBy { it.productId }
        val targetSnapshot = snapshotByProduct[decision.productId]
            ?: return TradingDecision.Skip("Agent product rejected: no snapshot for ${decision.productId}")
        val compactReason = decision.reason.take(props.maxAiReasonLength)

        return when (decision.action) {
            "BUY" -> validateBuy(targetSnapshot, decision, compactReason)
            "SELL" -> validateSell(targetSnapshot, decision, compactReason)
            "ROTATE" -> validateRotate(snapshotByProduct, targetSnapshot, decision, compactReason)
            else -> TradingDecision.Skip("Agent action rejected: ${decision.action}")
        }
    }

    private fun validateBuy(snapshot: MarketSnapshot, decision: AgentTradeDecision, compactReason: String): TradingDecision {
        return when {
            decision.quoteSizeUsd <= BigDecimal.ZERO -> TradingDecision.Skip("Agent buy quote size invalid: ${decision.quoteSizeUsd}")
            decision.quoteSizeUsd > props.maxBuyQuoteSizeUsd -> TradingDecision.Skip("Agent quote size exceeds max: ${decision.quoteSizeUsd} > ${props.maxBuyQuoteSizeUsd}")
            snapshot.usdAvailable - decision.quoteSizeUsd < props.minUsdCashReserve -> TradingDecision.Skip("Agent BUY rejected: USD reserve would fall below minimum")
            snapshot.portfolioAllocationPercent >= props.maxAssetAllocationPercent -> TradingDecision.Skip("Agent BUY rejected: allocation ${snapshot.portfolioAllocationPercent}% already >= max ${props.maxAssetAllocationPercent}%")
            snapshot.marketRegime == "CRASH" -> TradingDecision.Skip("Agent BUY rejected: market regime is CRASH")
            snapshot.marketRegime == "BEAR_TREND" && decision.score < props.bearTrendMinBuyScore -> TradingDecision.Skip("Agent BUY rejected: BEAR_TREND requires score >= ${props.bearTrendMinBuyScore}; score=${decision.score}")
            decision.reasonCode == "OVERSOLD_BOUNCE" && snapshot.rsi14 > props.oversoldBounceMaxRsi -> TradingDecision.Skip("Agent BUY rejected: OVERSOLD_BOUNCE requires RSI <= ${props.oversoldBounceMaxRsi}; rsi=${snapshot.rsi14}")
            decision.reasonCode == "OVERSOLD_BOUNCE" && (snapshot.trend1hPercent <= props.oversoldBounceMinRecoveryTrendPercent || snapshot.trend4hPercent <= props.oversoldBounceMinRecoveryTrendPercent) -> TradingDecision.Skip("Agent BUY rejected: OVERSOLD_BOUNCE requires 1h and 4h recovery trends > ${props.oversoldBounceMinRecoveryTrendPercent}; 1h=${snapshot.trend1hPercent}% 4h=${snapshot.trend4hPercent}%")
            else -> TradingDecision.Buy(
                productId = decision.productId,
                quoteSizeUsd = decision.quoteSizeUsd,
                reason = "Agent BUY code=${decision.reasonCode} confidence=${decision.confidence} score=${decision.score} regime=${snapshot.marketRegime} pnl=${snapshot.unrealizedPnlPercent}% allocation=${snapshot.portfolioAllocationPercent}%: $compactReason",
                reasonCode = decision.reasonCode,
                thesis = decision.thesis,
                invalidationCondition = decision.invalidationCondition,
                profitTargetPercent = decision.profitTargetPercent,
                stopLossPercent = decision.stopLossPercent,
                maxHoldHours = decision.maxHoldHours,
            )
        }
    }

    private fun validateSell(snapshot: MarketSnapshot, decision: AgentTradeDecision, compactReason: String): TradingDecision {
        if (decision.baseSize > snapshot.cryptoBalance) {
            return TradingDecision.Skip("Agent sell size exceeds available balance: ${decision.baseSize} > ${snapshot.cryptoBalance}")
        }
        val sellNotionalUsd = decision.baseSize.multiply(snapshot.price)
        val fullPositionNotionalUsd = snapshot.cryptoBalance.multiply(snapshot.price)

        return when {
            decision.baseSize <= BigDecimal.ZERO -> TradingDecision.Skip("Agent sell size invalid: ${decision.baseSize}")
            snapshot.cryptoBalance <= BigDecimal.ZERO -> TradingDecision.Skip("Agent SELL rejected: no available balance")
            sellNotionalUsd < props.minSellNotionalUsd && fullPositionNotionalUsd >= props.minSellNotionalUsd -> TradingDecision.Skip("Agent SELL rejected: sell notional $sellNotionalUsd is below min ${props.minSellNotionalUsd}")
            !props.allowAiSellAtLoss &&
                    snapshot.unrealizedPnlPercent < BigDecimal.ZERO &&
                    snapshot.unrealizedPnlPercent > props.aiSellLossFloorPercent &&
                    decision.reasonCode != "STOP_LOSS" &&
                    decision.reasonCode != "THESIS_INVALIDATED" -> TradingDecision.Skip(
                "Agent SELL rejected: loss sell not allowed for non-stop/thesis reason. pnl=${snapshot.unrealizedPnlPercent}% reasonCode=${decision.reasonCode}"
            )
            snapshot.unrealizedPnlPercent < props.minProfitPercentForAiSell &&
                    snapshot.drawdownFromHighPercent < props.maxDrawdownFromHighPercent &&
                    decision.reasonCode != "STOP_LOSS" &&
                    decision.reasonCode != "THESIS_INVALIDATED" -> TradingDecision.Skip(
                "Agent SELL rejected: pnl ${snapshot.unrealizedPnlPercent}% below profit threshold ${props.minProfitPercentForAiSell}% and drawdown ${snapshot.drawdownFromHighPercent}% below stop threshold ${props.maxDrawdownFromHighPercent}%"
            )
            else -> TradingDecision.Sell(
                productId = decision.productId,
                baseSize = decision.baseSize,
                reason = "Agent SELL code=${decision.reasonCode} confidence=${decision.confidence} score=${decision.score} regime=${snapshot.marketRegime} pnl=${snapshot.unrealizedPnlPercent}% drawdown=${snapshot.drawdownFromHighPercent}%: $compactReason",
                reasonCode = decision.reasonCode,
            )
        }
    }

    private fun validateRotate(
        snapshotByProduct: Map<String, MarketSnapshot>,
        targetSnapshot: MarketSnapshot,
        decision: AgentTradeDecision,
        compactReason: String,
    ): TradingDecision {
        if (!props.level2RotationEnabled) return TradingDecision.Skip("Agent ROTATE rejected: level2 rotation disabled")
        if (decision.score < props.minRotationEdgeScore) return TradingDecision.Skip("Agent ROTATE rejected: score ${decision.score} < ${props.minRotationEdgeScore}")
        if (decision.reasonCode != "REBALANCE") return TradingDecision.Skip("Agent ROTATE rejected: reasonCode must be REBALANCE")
        if (decision.fundingProductId !in props.productIds) return TradingDecision.Skip("Agent ROTATE rejected: invalid funding product ${decision.fundingProductId}")
        if (decision.fundingProductId == decision.productId) return TradingDecision.Skip("Agent ROTATE rejected: funding and target product are the same")
        if (decision.quoteSizeUsd <= BigDecimal.ZERO || decision.quoteSizeUsd > props.maxBuyQuoteSizeUsd) {
            return TradingDecision.Skip("Agent ROTATE rejected: invalid quoteSizeUsd=${decision.quoteSizeUsd}")
        }
        if (targetSnapshot.portfolioAllocationPercent >= props.maxAssetAllocationPercent) {
            return TradingDecision.Skip("Agent ROTATE rejected: target allocation ${targetSnapshot.portfolioAllocationPercent}% already >= max ${props.maxAssetAllocationPercent}%")
        }

        val fundingSnapshot = snapshotByProduct[decision.fundingProductId]
            ?: return TradingDecision.Skip("Agent ROTATE rejected: no funding snapshot for ${decision.fundingProductId}")
        if (fundingSnapshot.cryptoBalance <= BigDecimal.ZERO) return TradingDecision.Skip("Agent ROTATE rejected: funding asset has no available balance")
        if (decision.fundingBaseSize <= BigDecimal.ZERO) return TradingDecision.Skip("Agent ROTATE rejected: invalid fundingBaseSize=${decision.fundingBaseSize}")
        if (decision.fundingBaseSize > fundingSnapshot.cryptoBalance) {
            return TradingDecision.Skip("Agent ROTATE rejected: fundingBaseSize ${decision.fundingBaseSize} > available ${fundingSnapshot.cryptoBalance}")
        }

        val maxFundingBaseSize = fundingSnapshot.cryptoBalance
            .multiply(props.maxRotationSellPercent)
            .divide(BigDecimal("100"), 12, RoundingMode.HALF_UP)
        if (decision.fundingBaseSize > maxFundingBaseSize) {
            return TradingDecision.Skip("Agent ROTATE rejected: funding sell exceeds ${props.maxRotationSellPercent}% cap")
        }

        val fundingNotionalUsd = decision.fundingBaseSize.multiply(fundingSnapshot.price)
        if (fundingNotionalUsd < props.minRotationNotionalUsd) {
            return TradingDecision.Skip("Agent ROTATE rejected: funding notional $fundingNotionalUsd < min ${props.minRotationNotionalUsd}")
        }
        if (fundingNotionalUsd < decision.quoteSizeUsd.multiply(BigDecimal("0.90"))) {
            return TradingDecision.Skip("Agent ROTATE rejected: funding notional $fundingNotionalUsd is too small for buy ${decision.quoteSizeUsd}")
        }
        if (targetSnapshot.usdAvailable + fundingNotionalUsd - decision.quoteSizeUsd < props.minUsdCashReserve) {
            return TradingDecision.Skip("Agent ROTATE rejected: projected USD reserve would still fall below minimum")
        }

        if (!props.allowAiSellAtLoss &&
            fundingSnapshot.unrealizedPnlPercent < BigDecimal.ZERO &&
            fundingSnapshot.unrealizedPnlPercent > props.aiSellLossFloorPercent
        ) {
            return TradingDecision.Skip("Agent ROTATE rejected: funding sell would realize a small loss without loss sells enabled. pnl=${fundingSnapshot.unrealizedPnlPercent}%")
        }

        val fundingSell = TradingDecision.Sell(
            productId = decision.fundingProductId,
            baseSize = decision.fundingBaseSize,
            reason = "Agent ROTATE funding SELL confidence=${decision.confidence} score=${decision.score} fundingPnl=${fundingSnapshot.unrealizedPnlPercent}% allocation=${fundingSnapshot.portfolioAllocationPercent}%: ${decision.fundingReason.take(props.maxAiReasonLength)}",
            reasonCode = "REBALANCE",
        )
        val targetBuy = TradingDecision.Buy(
            productId = decision.productId,
            quoteSizeUsd = decision.quoteSizeUsd,
            reason = "Agent ROTATE target BUY confidence=${decision.confidence} score=${decision.score} targetRegime=${targetSnapshot.marketRegime} allocation=${targetSnapshot.portfolioAllocationPercent}%: $compactReason",
            reasonCode = "REBALANCE",
            thesis = decision.thesis,
            invalidationCondition = decision.invalidationCondition,
            profitTargetPercent = decision.profitTargetPercent,
            stopLossPercent = decision.stopLossPercent,
            maxHoldHours = decision.maxHoldHours,
        )
        return TradingDecision.Rotate(
            sell = fundingSell,
            buy = targetBuy,
            reason = "Agent ROTATE ${decision.fundingProductId} -> ${decision.productId}: $compactReason",
        )
    }
}
