package com.example.cryptobot.agent

import com.example.cryptobot.config.BotProperties
import com.example.cryptobot.strategy.MarketSnapshot
import com.example.cryptobot.strategy.TradingDecision
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class AgentDecisionValidator(
    private val props: BotProperties,
) {
    fun validate(snapshot: MarketSnapshot, decision: AgentTradeDecision): TradingDecision {
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

        val compactReason = decision.reason.take(props.maxAiReasonLength)

        return when (decision.action) {
            "BUY" -> {
                if (decision.quoteSizeUsd <= BigDecimal.ZERO) {
                    TradingDecision.Skip("Agent buy quote size invalid: ${decision.quoteSizeUsd}")
                } else if (decision.quoteSizeUsd > props.maxBuyQuoteSizeUsd) {
                    TradingDecision.Skip("Agent quote size exceeds max: ${decision.quoteSizeUsd} > ${props.maxBuyQuoteSizeUsd}")
                } else if (snapshot.usdAvailable - decision.quoteSizeUsd < props.minUsdCashReserve) {
                    TradingDecision.Skip("Agent BUY rejected: USD reserve would fall below minimum")
                } else if (snapshot.portfolioAllocationPercent >= props.maxAssetAllocationPercent) {
                    TradingDecision.Skip("Agent BUY rejected: allocation ${snapshot.portfolioAllocationPercent}% already >= max ${props.maxAssetAllocationPercent}%")
                } else {
                    TradingDecision.Buy(
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

            "SELL" -> {
                if (decision.baseSize > snapshot.cryptoBalance) {
                    return TradingDecision.Skip(
                        "Agent sell size exceeds available balance: ${decision.baseSize} > ${snapshot.cryptoBalance}"
                    )
                }
                if (decision.baseSize <= BigDecimal.ZERO) {
                    TradingDecision.Skip("Agent sell size invalid: ${decision.baseSize}")
                } else if (snapshot.cryptoBalance <= BigDecimal.ZERO) {
                    TradingDecision.Skip("Agent SELL rejected: no available balance")
                } else if (
                    !props.allowAiSellAtLoss &&
                    snapshot.unrealizedPnlPercent < BigDecimal.ZERO &&
                    snapshot.unrealizedPnlPercent > props.aiSellLossFloorPercent &&
                    decision.reasonCode != "STOP_LOSS" &&
                    decision.reasonCode != "THESIS_INVALIDATED"
                ) {
                    TradingDecision.Skip(
                        "Agent SELL rejected: loss sell not allowed for non-stop/thesis reason. pnl=${snapshot.unrealizedPnlPercent}% reasonCode=${decision.reasonCode}"
                    )
                } else if (
                    snapshot.unrealizedPnlPercent < props.minProfitPercentForAiSell &&
                    snapshot.drawdownFromHighPercent < props.maxDrawdownFromHighPercent &&
                    decision.reasonCode != "STOP_LOSS" &&
                    decision.reasonCode != "THESIS_INVALIDATED"
                ) {
                    TradingDecision.Skip(
                        "Agent SELL rejected: pnl ${snapshot.unrealizedPnlPercent}% below profit threshold ${props.minProfitPercentForAiSell}% and drawdown ${snapshot.drawdownFromHighPercent}% below stop threshold ${props.maxDrawdownFromHighPercent}%"
                    )
                } else {
                    TradingDecision.Sell(
                        productId = decision.productId,
                        baseSize = decision.baseSize,
                        reason = "Agent SELL code=${decision.reasonCode} confidence=${decision.confidence} score=${decision.score} regime=${snapshot.marketRegime} pnl=${snapshot.unrealizedPnlPercent}% drawdown=${snapshot.drawdownFromHighPercent}%: $compactReason",
                        reasonCode = decision.reasonCode,
                    )
                }
            }

            else -> TradingDecision.Skip("Agent action rejected: ${decision.action}")
        }
    }
}