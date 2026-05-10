package com.example.cryptobot.persistence

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
            )

            decisions.add(doc).get()
            log.info("Recorded trade ledger entry: decisionType={} product={}", decisionType, snapshot.productId)
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

    fun applyLiveBuy(snapshot: MarketSnapshot, quoteSizeUsd: BigDecimal): Mono<Void> {
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

    fun applyLiveSell(snapshot: MarketSnapshot, baseSize: BigDecimal): Mono<Void> {
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
                        "buyCount" to oldBuyCount,
                        "sellCount" to oldSellCount + 1,
                        "lastSellAt" to Timestamp.now(),
                        "updatedAt" to Timestamp.now(),
                    ),
                    com.google.cloud.firestore.SetOptions.merge(),
                )
                null
            }.get()
            null
        }.subscribeOn(Schedulers.boundedElastic()).then()
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
        )
    }
}
