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

        return when (decision.action) {
            "BUY" -> {
                if (decision.quoteSizeUsd <= BigDecimal.ZERO) {
                    TradingDecision.Skip("Agent buy quote size invalid: ${decision.quoteSizeUsd}")
                } else if (decision.quoteSizeUsd > props.maxBuyQuoteSizeUsd) {
                    TradingDecision.Skip("Agent quote size exceeds max: ${decision.quoteSizeUsd} > ${props.maxBuyQuoteSizeUsd}")
                } else {
                    TradingDecision.Buy(
                        productId = decision.productId,
                        quoteSizeUsd = decision.quoteSizeUsd,
                        reason = "Agent BUY confidence=${decision.confidence}: ${decision.reason}"
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
                } else {
                    TradingDecision.Sell(
                        productId = decision.productId,
                        baseSize = decision.baseSize,
                        reason = "Agent SELL confidence=${decision.confidence}: ${decision.reason}"
                    )
                }
            }

            else -> TradingDecision.Skip("Agent action rejected: ${decision.action}")
        }
    }
}