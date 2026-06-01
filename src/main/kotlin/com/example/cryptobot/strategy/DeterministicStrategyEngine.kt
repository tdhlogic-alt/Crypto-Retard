package com.example.cryptobot.strategy

import com.example.cryptobot.config.BotProperties
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.RoundingMode

@Component
class DeterministicStrategyEngine(
    private val props: BotProperties,
) {
    fun propose(snapshots: List<MarketSnapshot>): List<StrategySignal> {
        val hardExits = snapshots.mapNotNull { hardExitSignal(it) }
        if (hardExits.isNotEmpty()) return hardExits.sortedByDescending { it.priority }.take(props.maxActionsPerRun)

        return snapshots
            .mapNotNull { trendPullbackBuySignal(it) }
            .sortedByDescending { it.priority }
            .take(props.maxActionsPerRun)
    }

    private fun hardExitSignal(snapshot: MarketSnapshot): StrategySignal? {
        if (snapshot.cryptoBalance <= BigDecimal.ZERO || snapshot.price <= BigDecimal.ZERO) return null

        val fullSize = snapshot.cryptoBalance
        val halfSize = snapshot.cryptoBalance.divide(BigDecimal("2"), 12, RoundingMode.HALF_UP)
        val stopLoss = snapshot.activeStopLossPercent.takeIf { it > BigDecimal.ZERO } ?: props.strategyStopLossPercent
        val profitTarget = snapshot.activeProfitTargetPercent.takeIf { it > BigDecimal.ZERO } ?: props.strategyTakeProfitPercent

        return when {
            snapshot.unrealizedPnlPercent <= stopLoss.negate() -> sellSignal(
                snapshot = snapshot,
                baseSize = fullSize,
                reasonCode = "STOP_LOSS",
                priority = 100,
                reason = "Hard stop-loss: pnl=${snapshot.unrealizedPnlPercent}% <= -$stopLoss%",
            )

            snapshot.marketRegime == "CRASH" && snapshot.unrealizedPnlPercent < BigDecimal.ZERO -> sellSignal(
                snapshot = snapshot,
                baseSize = fullSize,
                reasonCode = "STOP_LOSS",
                priority = 98,
                reason = "Hard crash exit: regime=CRASH and position is losing pnl=${snapshot.unrealizedPnlPercent}%",
            )

            snapshot.drawdownFromHighPercent >= props.strategyTrailingDrawdownPercent && snapshot.unrealizedPnlPercent > BigDecimal.ZERO -> sellSignal(
                snapshot = snapshot,
                baseSize = halfSize.maxBase(snapshot.cryptoBalance),
                reasonCode = "TRAILING_STOP",
                priority = 94,
                reason = "Trailing profit protection: drawdownFromHigh=${snapshot.drawdownFromHighPercent}% >= ${props.strategyTrailingDrawdownPercent}%",
            )

            snapshot.unrealizedPnlPercent >= profitTarget -> sellSignal(
                snapshot = snapshot,
                baseSize = halfSize.maxBase(snapshot.cryptoBalance),
                reasonCode = "PROFIT_PROTECTION",
                priority = 90,
                reason = "Take-profit: pnl=${snapshot.unrealizedPnlPercent}% >= target=$profitTarget%",
            )

            else -> null
        }
    }

    private fun sellSignal(
        snapshot: MarketSnapshot,
        baseSize: BigDecimal,
        reasonCode: String,
        priority: Int,
        reason: String,
    ): StrategySignal = StrategySignal(
        decision = TradingDecision.Sell(
            productId = snapshot.productId,
            baseSize = baseSize,
            reason = "Deterministic risk engine: $reason",
            reasonCode = reasonCode,
        ),
        strategyName = "hard-risk-exit",
        priority = priority,
        hardExit = true,
        requiresAiApproval = false,
        rationale = reason,
    )

    private fun trendPullbackBuySignal(snapshot: MarketSnapshot): StrategySignal? {
        if (props.maxBuysPerRun <= 0 || props.maxTotalBuyUsdPerRun <= BigDecimal.ZERO) return null
        if (snapshot.usdAvailable - props.strategyQuoteSizeUsd < props.minUsdCashReserve) return null
        if (snapshot.cryptoValueUsd > BigDecimal.ZERO && snapshot.portfolioAllocationPercent >= props.maxAssetAllocationPercent) return null
        if (snapshot.marketRegime in setOf("CRASH", "BEAR_TREND", "HIGH_VOLATILITY")) return null
        if (snapshot.trend7dPercent < props.strategyMinTrend7dPercent) return null
        if (snapshot.trend24hPercent > props.strategyMaxEntry24hPercent) return null
        if (snapshot.trend4hPercent < props.strategyMinRecovery4hPercent) return null
        if (snapshot.rsi14 <= BigDecimal.ZERO || snapshot.rsi14 > props.strategyMaxEntryRsi) return null

        val reason = "Trend pullback candidate: regime=${snapshot.marketRegime}, 7d=${snapshot.trend7dPercent}%, 24h=${snapshot.trend24hPercent}%, 4h=${snapshot.trend4hPercent}%, rsi=${snapshot.rsi14}"
        return StrategySignal(
            decision = TradingDecision.Buy(
                productId = snapshot.productId,
                quoteSizeUsd = props.strategyQuoteSizeUsd.min(props.maxBuyQuoteSizeUsd).min(props.maxTotalBuyUsdPerRun),
                reason = "Deterministic strategy engine: $reason",
                reasonCode = "TREND_PULLBACK",
                thesis = "Trend is positive while short-term pullback/recovery offers asymmetric entry.",
                invalidationCondition = "Exit if stop-loss is hit, market regime turns CRASH, or recovery trend fails.",
                profitTargetPercent = props.strategyTakeProfitPercent,
                stopLossPercent = props.strategyStopLossPercent,
                maxHoldHours = props.strategyMaxHoldHours,
            ),
            strategyName = "trend-pullback",
            priority = score(snapshot),
            hardExit = false,
            requiresAiApproval = props.strategyAiVetoEnabled,
            rationale = reason,
        )
    }

    private fun score(snapshot: MarketSnapshot): Int {
        var score = 50
        if (snapshot.marketRegime == "BULL_TREND") score += 20
        if (snapshot.marketRegime == "RECOVERY") score += 10
        if (snapshot.trend7dPercent >= BigDecimal("8")) score += 10
        if (snapshot.trend4hPercent >= BigDecimal.ZERO) score += 10
        if (snapshot.rsi14 in BigDecimal("35")..BigDecimal("55")) score += 10
        return score.coerceIn(0, 100)
    }

    private operator fun ClosedRange<BigDecimal>.contains(value: BigDecimal): Boolean =
        value >= start && value <= endInclusive

    private fun BigDecimal.maxBase(available: BigDecimal): BigDecimal =
        if (this <= BigDecimal.ZERO || this > available) available else this
}
