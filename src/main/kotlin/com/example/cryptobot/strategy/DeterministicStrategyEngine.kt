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

        val entries = snapshots
            .flatMap { snapshot ->
                listOfNotNull(
                    trendPullbackBuySignal(snapshot),
                    breakoutContinuationBuySignal(snapshot),
                    momentumAccelerationBuySignal(snapshot),
                    oversoldBounceBuySignal(snapshot),
                )
            }
            .bestSignalPerProduct()
            .sortedByDescending { it.priority }

        val rotations = if (props.level2RotationEnabled) rotationSignals(snapshots, entries) else emptyList()

        return (rotations + entries)
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
        if (!canOpenNewLong(snapshot)) return null
        if (snapshot.marketRegime in setOf("CRASH", "BEAR_TREND", "HIGH_VOLATILITY")) return null
        if (snapshot.trend7dPercent < props.strategyMinTrend7dPercent) return null
        if (snapshot.trend24hPercent > props.strategyMaxEntry24hPercent) return null
        if (snapshot.trend4hPercent < props.strategyMinRecovery4hPercent) return null
        if (snapshot.rsi14 <= BigDecimal.ZERO || snapshot.rsi14 > props.strategyMaxEntryRsi) return null

        val rawScore = score(snapshot, base = 50, reasonCode = "TREND_PULLBACK")
        val reason = "Trend pullback candidate: regime=${snapshot.marketRegime}, 7d=${snapshot.trend7dPercent}%, 24h=${snapshot.trend24hPercent}%, 4h=${snapshot.trend4hPercent}%, rsi=${snapshot.rsi14}"
        return buySignal(
            snapshot = snapshot,
            strategyName = "trend-pullback",
            reasonCode = "TREND_PULLBACK",
            priority = rawScore,
            reason = reason,
            thesis = "Trend is positive while short-term pullback/recovery offers asymmetric entry.",
            invalidation = "Exit if stop-loss is hit, market regime turns CRASH, or recovery trend fails.",
        )
    }

    private fun breakoutContinuationBuySignal(snapshot: MarketSnapshot): StrategySignal? {
        if (!canOpenNewLong(snapshot)) return null
        if (snapshot.marketRegime !in setOf("BULL_TREND", "MOMENTUM", "RECOVERY")) return null
        if (snapshot.trend7dPercent < props.breakoutMinTrend7dPercent) return null
        if (snapshot.trend24hPercent < props.breakoutMinTrend24hPercent) return null
        if (snapshot.priceTo24hHighPercent < props.breakoutMinPriceTo24hHighPercent) return null
        if (snapshot.rsi14 > props.breakoutMaxRsi) return null

        val rawScore = score(snapshot, base = 58, reasonCode = "BREAKOUT_CONTINUATION")
        val reason = "Breakout continuation candidate: regime=${snapshot.marketRegime}, 7d=${snapshot.trend7dPercent}%, 24h=${snapshot.trend24hPercent}%, priceTo24hHigh=${snapshot.priceTo24hHighPercent}%, rsi=${snapshot.rsi14}"
        return buySignal(
            snapshot = snapshot,
            strategyName = "breakout-continuation",
            reasonCode = "BREAKOUT_CONTINUATION",
            priority = rawScore,
            reason = reason,
            thesis = "Price is pressing near the 24h high with supportive multi-day trend, suggesting continuation rather than a random spike.",
            invalidation = "Exit if breakout fails, stop-loss is hit, or market regime deteriorates.",
        )
    }

    private fun momentumAccelerationBuySignal(snapshot: MarketSnapshot): StrategySignal? {
        if (!canOpenNewLong(snapshot)) return null
        if (snapshot.marketRegime in setOf("CRASH", "BEAR_TREND", "HIGH_VOLATILITY")) return null
        if (snapshot.trend1hPercent < props.momentumMinTrend1hPercent) return null
        if (snapshot.trend4hPercent < props.momentumMinTrend4hPercent) return null
        if (snapshot.trend24hPercent < props.momentumMinTrend24hPercent) return null
        if (snapshot.rsi14 > props.momentumMaxRsi) return null

        val rawScore = score(snapshot, base = 55, reasonCode = "MOMENTUM_ACCELERATION")
        val reason = "Momentum acceleration candidate: regime=${snapshot.marketRegime}, 1h=${snapshot.trend1hPercent}%, 4h=${snapshot.trend4hPercent}%, 24h=${snapshot.trend24hPercent}%, rsi=${snapshot.rsi14}"
        return buySignal(
            snapshot = snapshot,
            strategyName = "momentum-acceleration",
            reasonCode = "MOMENTUM_ACCELERATION",
            priority = rawScore,
            reason = reason,
            thesis = "Short and medium-term momentum are aligned, suggesting strengthening demand.",
            invalidation = "Exit if acceleration fades, stop-loss is hit, or market regime deteriorates.",
        )
    }

    private fun oversoldBounceBuySignal(snapshot: MarketSnapshot): StrategySignal? {
        if (!canOpenNewLong(snapshot)) return null
        if (snapshot.marketRegime in setOf("CRASH", "BEAR_TREND")) return null
        if (snapshot.rsi14 <= BigDecimal.ZERO || snapshot.rsi14 > props.oversoldBounceMaxRsi) return null
        if (snapshot.trend1hPercent < props.oversoldBounceMinRecoveryTrendPercent) return null
        if (snapshot.trend4hPercent < props.strategyMinRecovery4hPercent) return null
        if (snapshot.priceTo24hLowPercent > props.oversoldBounceMaxPriceTo24hLowPercent) return null

        val rawScore = score(snapshot, base = 52, reasonCode = "OVERSOLD_BOUNCE")
        val reason = "Oversold bounce candidate: regime=${snapshot.marketRegime}, rsi=${snapshot.rsi14}, 1h=${snapshot.trend1hPercent}%, 4h=${snapshot.trend4hPercent}%, priceTo24hLow=${snapshot.priceTo24hLowPercent}%"
        return buySignal(
            snapshot = snapshot,
            strategyName = "oversold-bounce",
            reasonCode = "OVERSOLD_BOUNCE",
            priority = rawScore,
            reason = reason,
            thesis = "Asset is oversold but showing early recovery, creating a controlled mean-reversion setup.",
            invalidation = "Exit if bounce fails, stop-loss is hit, or market regime turns bearish.",
        )
    }

    private fun rotationSignals(snapshots: List<MarketSnapshot>, entries: List<StrategySignal>): List<StrategySignal> {
        if (props.maxRotationsPerRun <= 0) return emptyList()

        val targets = entries
            .filter { it.decision is TradingDecision.Buy && it.priority >= props.minRotationEdgeScore.toInt() }
            .sortedByDescending { it.priority }

        val fundingCandidates = snapshots
            .filter { it.cryptoBalance > BigDecimal.ZERO && it.cryptoValueUsd >= props.minRotationNotionalUsd }
            .filter { it.unrealizedPnlPercent <= props.rotationMaxFundingPnlPercent || it.marketRegime in setOf("BEAR_TREND", "SIDEWAYS", "UNKNOWN") }
            .sortedWith(compareBy<MarketSnapshot> { it.unrealizedPnlPercent }.thenBy { it.trend7dPercent })

        return targets.flatMap { targetSignal ->
            val targetBuy = targetSignal.decision as TradingDecision.Buy
            fundingCandidates
                .filter { it.productId != targetBuy.productId }
                .mapNotNull { funding ->
                    val fundingScore = score(funding, base = 35, reasonCode = funding.activeReasonCodeForScoring())
                    val scoreGap = BigDecimal(targetSignal.priority - fundingScore)
                    if (scoreGap < props.minRotationScoreGap) return@mapNotNull null

                    val sellPercent = props.maxRotationSellPercent.divide(BigDecimal("100"), 6, RoundingMode.HALF_UP)
                    val baseToSell = funding.cryptoBalance.multiply(sellPercent).setScale(12, RoundingMode.HALF_UP).maxBase(funding.cryptoBalance)
                    val notionalUsd = baseToSell.multiply(funding.price)
                    if (notionalUsd < props.minRotationNotionalUsd) return@mapNotNull null

                    val quoteSize = notionalUsd.min(targetBuy.quoteSizeUsd).min(props.maxBuyQuoteSizeUsd).min(props.maxTotalBuyUsdPerRun)
                    StrategySignal(
                        decision = TradingDecision.Rotate(
                            sell = TradingDecision.Sell(
                                productId = funding.productId,
                                baseSize = baseToSell,
                                reason = "Deterministic rotation funding: weaker position fundingScore=$fundingScore targetScore=${targetSignal.priority}",
                                reasonCode = "RELATIVE_STRENGTH_ROTATION_SELL",
                            ),
                            buy = targetBuy.copy(
                                quoteSizeUsd = quoteSize,
                                reason = "Relative-strength rotation target: ${targetBuy.reason}",
                                reasonCode = "RELATIVE_STRENGTH_ROTATION_BUY",
                            ),
                            reason = "Rotate from ${funding.productId} into ${targetBuy.productId}: targetScore=${targetSignal.priority}, fundingScore=$fundingScore, gap=$scoreGap",
                        ),
                        strategyName = "relative-strength-rotation",
                        priority = (targetSignal.priority + 5).coerceAtMost(99),
                        hardExit = false,
                        requiresAiApproval = props.strategyAiVetoEnabled,
                        rationale = "Relative-strength rotation: sell partial ${funding.productId} to fund ${targetBuy.productId}; targetScore=${targetSignal.priority}, fundingScore=$fundingScore, gap=$scoreGap",
                    )
                }
        }.take(props.maxRotationsPerRun)
    }

    private fun buySignal(
        snapshot: MarketSnapshot,
        strategyName: String,
        reasonCode: String,
        priority: Int,
        reason: String,
        thesis: String,
        invalidation: String,
    ): StrategySignal = StrategySignal(
        decision = TradingDecision.Buy(
            productId = snapshot.productId,
            quoteSizeUsd = quoteSizeFor(priority),
            reason = "Deterministic strategy engine: $reason",
            reasonCode = reasonCode,
            thesis = thesis,
            invalidationCondition = invalidation,
            profitTargetPercent = props.strategyTakeProfitPercent,
            stopLossPercent = props.strategyStopLossPercent,
            maxHoldHours = props.strategyMaxHoldHours,
        ),
        strategyName = strategyName,
        priority = priority,
        hardExit = false,
        requiresAiApproval = props.strategyAiVetoEnabled,
        rationale = reason,
    )

    private fun canOpenNewLong(snapshot: MarketSnapshot): Boolean {
        if (props.maxBuysPerRun <= 0 || props.maxTotalBuyUsdPerRun <= BigDecimal.ZERO) return false
        if (snapshot.usdAvailable - props.strategyQuoteSizeUsd < props.minUsdCashReserve) return false
        if (snapshot.cryptoValueUsd > BigDecimal.ZERO && snapshot.portfolioAllocationPercent >= props.maxAssetAllocationPercent) return false
        return true
    }

    private fun quoteSizeFor(score: Int): BigDecimal {
        val desired = if (score >= props.strategyHighConvictionMinScore) {
            props.strategyHighConvictionQuoteSizeUsd
        } else {
            props.strategyQuoteSizeUsd
        }
        return desired.min(props.maxBuyQuoteSizeUsd).min(props.maxTotalBuyUsdPerRun)
    }

    private fun score(snapshot: MarketSnapshot, base: Int, reasonCode: String): Int {
        var score = base
        if (snapshot.marketRegime == "BULL_TREND") score += 20
        if (snapshot.marketRegime == "MOMENTUM") score += 15
        if (snapshot.marketRegime == "RECOVERY") score += 10
        if (snapshot.trend7dPercent >= BigDecimal("8")) score += 10
        if (snapshot.trend4hPercent >= BigDecimal.ZERO) score += 8
        if (snapshot.trend1hPercent >= BigDecimal.ZERO) score += 4
        if (snapshot.rsi14 in BigDecimal("35")..BigDecimal("55")) score += 8
        if (snapshot.reasonCode30dCount >= props.strategyPerformanceMinSample) {
            when {
                snapshot.reasonCode30dWinRate >= BigDecimal("60") -> score += 5
                snapshot.reasonCode30dWinRate < BigDecimal("40") -> score -= 8
            }
        }
        if (reasonCode == "OVERSOLD_BOUNCE" && snapshot.volatility24hPercent >= BigDecimal("4")) score -= 5
        return score.coerceIn(0, 100)
    }

    private fun List<StrategySignal>.bestSignalPerProduct(): List<StrategySignal> = this
        .groupBy { signal ->
            when (val decision = signal.decision) {
                is TradingDecision.Buy -> decision.productId
                is TradingDecision.Sell -> decision.productId
                is TradingDecision.Rotate -> decision.buy.productId
                is TradingDecision.Skip -> "SKIP"
            }
        }
        .map { (_, signals) -> signals.maxBy { it.priority } }

    private fun MarketSnapshot.activeReasonCodeForScoring(): String = activeThesis.ifBlank { "NO_CLEAR_EDGE" }

    private operator fun ClosedRange<BigDecimal>.contains(value: BigDecimal): Boolean =
        value >= start && value <= endInclusive

    private fun BigDecimal.maxBase(available: BigDecimal): BigDecimal =
        if (this <= BigDecimal.ZERO || this > available) available else this
}
