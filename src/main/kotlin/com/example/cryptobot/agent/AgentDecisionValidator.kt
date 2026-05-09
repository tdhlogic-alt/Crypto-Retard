package com.example.cryptobot.agent

import com.example.cryptobot.config.BotProperties
import com.example.cryptobot.strategy.TradingDecision
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class AgentDecisionValidator(
    private val props: BotProperties,
) {
    fun validate(decision: AgentTradeDecision): TradingDecision {
        if (decision.action == "SKIP") {
            return TradingDecision.Skip("Agent skipped: ${decision.reason}")
        }

        if (decision.action != "BUY") {
            return TradingDecision.Skip("Agent action rejected: ${decision.action}")
        }

        if (decision.productId !in props.productIds) {
            return TradingDecision.Skip(
                "Agent product rejected: ${decision.productId}. Allowed=${props.productIds}"
            )
        }

        if (decision.confidence < props.agentMinConfidence) {
            return TradingDecision.Skip("Agent confidence too low: ${decision.confidence}")
        }

        if (decision.quoteSizeUsd <= BigDecimal.ZERO) {
            return TradingDecision.Skip("Agent quote size invalid: ${decision.quoteSizeUsd}")
        }

        if (decision.quoteSizeUsd > props.maxBuyQuoteSizeUsd) {
            return TradingDecision.Skip(
                "Agent quote size exceeds max: ${decision.quoteSizeUsd} > ${props.maxBuyQuoteSizeUsd}"
            )
        }

        return TradingDecision.Buy(
            productId = decision.productId,
            quoteSizeUsd = decision.quoteSizeUsd,
            reason = "Agent BUY confidence=${decision.confidence}: ${decision.reason}"
        )
    }
}