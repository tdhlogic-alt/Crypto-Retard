package com.example.cryptobot.persistence

import com.example.cryptobot.scheduler.PortfolioRunReport
import com.example.cryptobot.strategy.MarketSnapshot
import com.google.cloud.Timestamp
import com.google.cloud.firestore.Firestore
import com.google.cloud.firestore.Query
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant

@Component
class TradeLedgerClient(
    private val firestore: Firestore,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val decisions = firestore.collection("trade_decisions")
    private val positions = firestore.collection("positions")

    fun record(
        snapshot: MarketSnapshot,
        decisionType: String,
        reason: String,
        dryRun: Boolean,
        quoteSizeUsd: BigDecimal? = null,
        coinbaseOrderId: String? = null,
        coinbaseSuccess: Boolean? = null,
        errorMessage: String? = null,
        baseSize: BigDecimal? = null,
        reasonCode: String? = null,
        thesis: String? = null,
        invalidationCondition: String? = null,
        profitTargetPercent: BigDecimal? = null,
        stopLossPercent: BigDecimal? = null,
        maxHoldHours: Long? = null,
    ): Mono<Void> {
        return Mono.fromCallable {
            val doc = mapOf(
                "createdAt" to Timestamp.now(),
                "productId" to snapshot.productId,
                "decisionType" to decisionType,
                "reason" to reason,
                "dryRun" to dryRun,
                "price" to snapshot.price.toPlainString(),
                "change24hPercent" to snapshot.change24hPercent.toPlainString(),
                "usdAvailable" to snapshot.usdAvailable.toPlainString(),
                "quoteSizeUsd" to quoteSizeUsd?.toPlainString(),
                "coinbaseOrderId" to coinbaseOrderId,
                "coinbaseSuccess" to coinbaseSuccess,
                "errorMessage" to errorMessage,
                "baseSize" to baseSize?.toPlainString(),
                "avgCostBasis" to snapshot.avgCostBasis.toPlainString(),
                "unrealizedPnlUsd" to snapshot.unrealizedPnlUsd.toPlainString(),
                "unrealizedPnlPercent" to snapshot.unrealizedPnlPercent.toPlainString(),
                "highestPriceSeen" to snapshot.highestPriceSeen.toPlainString(),
                "drawdownFromHighPercent" to snapshot.drawdownFromHighPercent.toPlainString(),
                "marketRegime" to snapshot.marketRegime,
                "reasonCode" to reasonCode,
                "thesis" to thesis,
                "invalidationCondition" to invalidationCondition,
                "profitTargetPercent" to profitTargetPercent?.toPlainString(),
                "stopLossPercent" to stopLossPercent?.toPlainString(),
                "maxHoldHours" to maxHoldHours,
                "outcomeScored" to (decisionType != "BUY" && decisionType != "SELL"),
            )

            decisions.add(doc).get()
            log.info("Recorded trade ledger entry: decisionType={} product={}", decisionType, snapshot.productId)
            null
        }
            .subscribeOn(Schedulers.boundedElastic())
            .then()
    }


    fun recordPortfolioRun(report: PortfolioRunReport): Mono<Void> {
        return Mono.fromCallable {
            val doc = mapOf(
                "createdAt" to Timestamp.ofTimeSecondsAndNanos(report.createdAt.epochSecond, report.createdAt.nano),
                "dryRun" to report.dryRun,
                "liveTradingEnabled" to report.liveTradingEnabled,
                "proposedActionCount" to report.proposedActionCount,
                "executedActionCount" to report.executedActionCount,
                "skippedActionCount" to report.skippedActionCount,
                "actions" to report.actions.map { it.asMap() },
                "portfolioBefore" to report.portfolioBefore.map { it.asMap() },
                "projectedPortfolio" to report.projectedPortfolio.map { it.asMap() },
            )

            firestore.collection("portfolio_runs").add(doc).get()
            log.info(
                "Recorded portfolio run: proposed={} executed={} skipped={}",
                report.proposedActionCount,
                report.executedActionCount,
                report.skippedActionCount,
            )
            null
        }
            .subscribeOn(Schedulers.boundedElastic())
            .then()
    }

    fun getPosition(productId: String, currentPrice: BigDecimal): Mono<PositionSnapshot> {
        return Mono.fromCallable {
            val doc = positions.document(productId).get().get()
            if (!doc.exists()) return@fromCallable PositionSnapshot.empty(productId)

            val quantity = doc.getString("quantity")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
            val avgCostBasis = doc.getString("avgCostBasis")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
            val totalInvested = doc.getString("totalInvested")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
            val realizedPnlUsd = doc.getString("realizedPnlUsd")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
            val highestPriceSeenStored = doc.getString("highestPriceSeen")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
            val highestPriceSeen = maxOf(highestPriceSeenStored, currentPrice)
            val buyCount = doc.getLong("buyCount") ?: 0L
            val sellCount = doc.getLong("sellCount") ?: 0L
            val activeReasonCode = doc.getString("activeReasonCode") ?: "NO_CLEAR_EDGE"
            val activeThesis = doc.getString("activeThesis") ?: ""
            val activeInvalidationCondition = doc.getString("activeInvalidationCondition") ?: ""
            val activeProfitTargetPercent = doc.getString("activeProfitTargetPercent")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
            val activeStopLossPercent = doc.getString("activeStopLossPercent")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
            val activeMaxHoldHours = doc.getLong("activeMaxHoldHours") ?: 0L

            val marketValue = quantity.multiply(currentPrice)
            val costValue = quantity.multiply(avgCostBasis)
            val unrealizedPnlUsd = marketValue.subtract(costValue)
            val unrealizedPnlPercent = if (costValue > BigDecimal.ZERO) {
                unrealizedPnlUsd.divide(costValue, 6, RoundingMode.HALF_UP).multiply(BigDecimal("100"))
            } else BigDecimal.ZERO

            val drawdownFromHighPercent = if (highestPriceSeen > BigDecimal.ZERO) {
                highestPriceSeen.subtract(currentPrice)
                    .divide(highestPriceSeen, 6, RoundingMode.HALF_UP)
                    .multiply(BigDecimal("100"))
            } else BigDecimal.ZERO

            PositionSnapshot(
                productId = productId,
                quantity = quantity,
                avgCostBasis = avgCostBasis,
                totalInvested = totalInvested,
                realizedPnlUsd = realizedPnlUsd,
                unrealizedPnlUsd = unrealizedPnlUsd,
                unrealizedPnlPercent = unrealizedPnlPercent,
                highestPriceSeen = highestPriceSeen,
                drawdownFromHighPercent = drawdownFromHighPercent,
                buyCount = buyCount,
                sellCount = sellCount,
                activeReasonCode = activeReasonCode,
                activeThesis = activeThesis,
                activeInvalidationCondition = activeInvalidationCondition,
                activeProfitTargetPercent = activeProfitTargetPercent,
                activeStopLossPercent = activeStopLossPercent,
                activeMaxHoldHours = activeMaxHoldHours,
            )
        }.subscribeOn(Schedulers.boundedElastic())
    }

    fun updateHighestPriceSeen(productId: String, currentPrice: BigDecimal): Mono<Void> {
        return getPosition(productId, currentPrice)
            .flatMap { position ->
                if (currentPrice <= position.highestPriceSeen) return@flatMap Mono.empty<Void>()
                Mono.fromCallable {
                    positions.document(productId).set(
                        mapOf(
                            "productId" to productId,
                            "highestPriceSeen" to currentPrice.toPlainString(),
                            "updatedAt" to Timestamp.now(),
                        ),
                        com.google.cloud.firestore.SetOptions.merge()
                    ).get()
                    null
                }.subscribeOn(Schedulers.boundedElastic()).then()
            }
    }

    fun applyLiveBuy(
        snapshot: MarketSnapshot,
        quoteSizeUsd: BigDecimal,
        reasonCode: String,
        thesis: String,
        invalidationCondition: String,
        profitTargetPercent: BigDecimal,
        stopLossPercent: BigDecimal,
        maxHoldHours: Long,
    ): Mono<Void> {
        return Mono.fromCallable {
            firestore.runTransaction { tx ->
                val ref = positions.document(snapshot.productId)
                val doc = tx.get(ref).get()

                val oldQuantity = doc.getString("quantity")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
                val oldTotalInvested = doc.getString("totalInvested")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
                val oldRealizedPnl = doc.getString("realizedPnlUsd")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
                val oldBuyCount = doc.getLong("buyCount") ?: 0L
                val oldSellCount = doc.getLong("sellCount") ?: 0L
                val oldHighestPriceSeen = doc.getString("highestPriceSeen")?.toBigDecimalOrNull() ?: BigDecimal.ZERO

                val boughtQuantity = if (snapshot.price > BigDecimal.ZERO) {
                    quoteSizeUsd.divide(snapshot.price, 12, RoundingMode.HALF_UP)
                } else BigDecimal.ZERO

                val newQuantity = oldQuantity + boughtQuantity
                val newTotalInvested = oldTotalInvested + quoteSizeUsd
                val newAvgCostBasis = if (newQuantity > BigDecimal.ZERO) {
                    newTotalInvested.divide(newQuantity, 12, RoundingMode.HALF_UP)
                } else BigDecimal.ZERO

                tx.set(
                    ref,
                    mapOf(
                        "productId" to snapshot.productId,
                        "quantity" to newQuantity.toPlainString(),
                        "avgCostBasis" to newAvgCostBasis.toPlainString(),
                        "totalInvested" to newTotalInvested.toPlainString(),
                        "realizedPnlUsd" to oldRealizedPnl.toPlainString(),
                        "highestPriceSeen" to maxOf(oldHighestPriceSeen, snapshot.price).toPlainString(),
                        "activeReasonCode" to reasonCode,
                        "activeThesis" to thesis,
                        "activeInvalidationCondition" to invalidationCondition,
                        "activeProfitTargetPercent" to profitTargetPercent.toPlainString(),
                        "activeStopLossPercent" to stopLossPercent.toPlainString(),
                        "activeMaxHoldHours" to maxHoldHours,
                        "buyCount" to oldBuyCount + 1,
                        "sellCount" to oldSellCount,
                        "lastBuyAt" to Timestamp.now(),
                        "updatedAt" to Timestamp.now(),
                    ),
                    com.google.cloud.firestore.SetOptions.merge(),
                )
                null
            }.get()
            null
        }.subscribeOn(Schedulers.boundedElastic()).then()
    }

    fun applyLiveSell(snapshot: MarketSnapshot, baseSize: BigDecimal, reasonCode: String): Mono<Void> {
        return Mono.fromCallable {
            firestore.runTransaction { tx ->
                val ref = positions.document(snapshot.productId)
                val doc = tx.get(ref).get()

                val oldQuantity = doc.getString("quantity")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
                val oldAvgCostBasis = doc.getString("avgCostBasis")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
                val oldTotalInvested = doc.getString("totalInvested")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
                val oldRealizedPnl = doc.getString("realizedPnlUsd")?.toBigDecimalOrNull() ?: BigDecimal.ZERO
                val oldBuyCount = doc.getLong("buyCount") ?: 0L
                val oldSellCount = doc.getLong("sellCount") ?: 0L

                val sellQuantity = minOf(baseSize, oldQuantity)
                val proceeds = sellQuantity.multiply(snapshot.price)
                val costRemoved = sellQuantity.multiply(oldAvgCostBasis)
                val realizedPnl = proceeds.subtract(costRemoved)
                val newQuantity = oldQuantity.subtract(sellQuantity).max(BigDecimal.ZERO)
                val newTotalInvested = oldTotalInvested.subtract(costRemoved).max(BigDecimal.ZERO)
                val newAvgCostBasis = if (newQuantity > BigDecimal.ZERO) {
                    newTotalInvested.divide(newQuantity, 12, RoundingMode.HALF_UP)
                } else BigDecimal.ZERO

                tx.set(
                    ref,
                    mapOf(
                        "productId" to snapshot.productId,
                        "quantity" to newQuantity.toPlainString(),
                        "avgCostBasis" to newAvgCostBasis.toPlainString(),
                        "totalInvested" to newTotalInvested.toPlainString(),
                        "realizedPnlUsd" to oldRealizedPnl.add(realizedPnl).toPlainString(),
                        "lastSellReasonCode" to reasonCode,
                        "buyCount" to oldBuyCount,
                        "sellCount" to oldSellCount + 1,
                        "lastSellAt" to Timestamp.now(),
                        "updatedAt" to Timestamp.now(),
                    ).plus(
                        if (newQuantity <= BigDecimal.ZERO) {
                            mapOf(
                                "activeReasonCode" to "NO_CLEAR_EDGE",
                                "activeThesis" to "",
                                "activeInvalidationCondition" to "",
                                "activeProfitTargetPercent" to BigDecimal.ZERO.toPlainString(),
                                "activeStopLossPercent" to BigDecimal.ZERO.toPlainString(),
                                "activeMaxHoldHours" to 0L,
                            )
                        } else emptyMap()
                    ),
                    com.google.cloud.firestore.SetOptions.merge(),
                )
                null
            }.get()
            null
        }.subscribeOn(Schedulers.boundedElastic()).then()
    }



    fun scorePendingOutcomes(productId: String, currentPrice: BigDecimal): Mono<Void> {
        if (currentPrice <= BigDecimal.ZERO) return Mono.empty()

        return Mono.fromCallable {
            listOf("BUY", "SELL").forEach { decisionType ->
                val snapshot = decisions
                    .whereEqualTo("productId", productId)
                    .whereEqualTo("decisionType", decisionType)
                    .whereEqualTo("dryRun", false)
                    .whereEqualTo("coinbaseSuccess", true)
                    .whereEqualTo("outcomeScored", false)
                    .limit(20)
                    .get()
                    .get()

                snapshot.documents.forEach { doc ->
                    val entryPrice = doc.getString("price")?.toBigDecimalOrNull() ?: return@forEach
                    if (entryPrice <= BigDecimal.ZERO) return@forEach

                    val rawOutcomePercent = currentPrice.subtract(entryPrice)
                        .divide(entryPrice, 6, RoundingMode.HALF_UP)
                        .multiply(BigDecimal("100"))

                    val outcomePnlPercent = if (decisionType == "BUY") {
                        rawOutcomePercent
                    } else {
                        rawOutcomePercent.negate()
                    }

                    doc.reference.update(
                        mapOf(
                            "outcomeScored" to true,
                            "outcomeScoredAt" to Timestamp.now(),
                            "outcomePrice" to currentPrice.toPlainString(),
                            "outcomePnlPercent" to outcomePnlPercent.toPlainString(),
                        )
                    ).get()
                }
            }
            null
        }.subscribeOn(Schedulers.boundedElastic()).then()
    }

    fun getReasonCodeStats(reasonCode: String, since: Instant): Mono<ReasonCodeStats> {
        if (reasonCode.isBlank() || reasonCode == "NO_CLEAR_EDGE") return Mono.just(ReasonCodeStats.empty())

        return Mono.fromCallable {
            val snapshot = decisions
                .whereEqualTo("reasonCode", reasonCode)
                .whereGreaterThanOrEqualTo("createdAt", Timestamp.ofTimeSecondsAndNanos(since.epochSecond, since.nano))
                .orderBy("createdAt", Query.Direction.ASCENDING)
                .get()
                .get()

            val outcomes = snapshot.documents.mapNotNull { doc ->
                doc.getString("outcomePnlPercent")?.toBigDecimalOrNull()
            }

            if (outcomes.isEmpty()) return@fromCallable ReasonCodeStats.empty()

            val wins = outcomes.count { it > BigDecimal.ZERO }.toLong()
            ReasonCodeStats(
                count = outcomes.size.toLong(),
                wins = wins,
                winRatePercent = BigDecimal(wins)
                    .divide(BigDecimal(outcomes.size), 6, RoundingMode.HALF_UP)
                    .multiply(BigDecimal("100")),
            )
        }.subscribeOn(Schedulers.boundedElastic())
    }

    fun hasRecentLiveBuy(productId: String, since: Instant): Mono<Boolean> {
        return Mono.fromCallable {
            val snapshot = decisions
                .whereEqualTo("productId", productId)
                .whereEqualTo("decisionType", "BUY")
                .whereEqualTo("dryRun", false)
                .whereEqualTo("coinbaseSuccess", true)
                .whereGreaterThanOrEqualTo("createdAt", Timestamp.ofTimeSecondsAndNanos(since.epochSecond, since.nano))
                .limit(1)
                .get()
                .get()

            !snapshot.isEmpty
        }.subscribeOn(Schedulers.boundedElastic())
    }

    fun liveBuyTotalSince(since: Instant): Mono<BigDecimal> {
        return Mono.fromCallable {
            val snapshot = decisions
                .whereEqualTo("decisionType", "BUY")
                .whereEqualTo("dryRun", false)
                .whereEqualTo("coinbaseSuccess", true)
                .whereGreaterThanOrEqualTo("createdAt", Timestamp.ofTimeSecondsAndNanos(since.epochSecond, since.nano))
                .orderBy("createdAt", Query.Direction.ASCENDING)
                .get()
                .get()

            snapshot.documents
                .mapNotNull { it.getString("quoteSizeUsd") }
                .fold(BigDecimal.ZERO) { acc, value -> acc + value.toBigDecimal() }
        }.subscribeOn(Schedulers.boundedElastic())
    }
}

data class PositionSnapshot(
    val productId: String,
    val quantity: BigDecimal,
    val avgCostBasis: BigDecimal,
    val totalInvested: BigDecimal,
    val realizedPnlUsd: BigDecimal,
    val unrealizedPnlUsd: BigDecimal,
    val unrealizedPnlPercent: BigDecimal,
    val highestPriceSeen: BigDecimal,
    val drawdownFromHighPercent: BigDecimal,
    val buyCount: Long,
    val sellCount: Long,
    val activeReasonCode: String,
    val activeThesis: String,
    val activeInvalidationCondition: String,
    val activeProfitTargetPercent: BigDecimal,
    val activeStopLossPercent: BigDecimal,
    val activeMaxHoldHours: Long,
) {
    companion object {
        fun empty(productId: String) = PositionSnapshot(
            productId = productId,
            quantity = BigDecimal.ZERO,
            avgCostBasis = BigDecimal.ZERO,
            totalInvested = BigDecimal.ZERO,
            realizedPnlUsd = BigDecimal.ZERO,
            unrealizedPnlUsd = BigDecimal.ZERO,
            unrealizedPnlPercent = BigDecimal.ZERO,
            highestPriceSeen = BigDecimal.ZERO,
            drawdownFromHighPercent = BigDecimal.ZERO,
            buyCount = 0,
            sellCount = 0,
            activeReasonCode = "NO_CLEAR_EDGE",
            activeThesis = "",
            activeInvalidationCondition = "",
            activeProfitTargetPercent = BigDecimal.ZERO,
            activeStopLossPercent = BigDecimal.ZERO,
            activeMaxHoldHours = 0,
        )
    }
}


data class ReasonCodeStats(
    val count: Long,
    val wins: Long,
    val winRatePercent: BigDecimal,
) {
    companion object {
        fun empty() = ReasonCodeStats(
            count = 0,
            wins = 0,
            winRatePercent = BigDecimal.ZERO,
        )
    }
}
