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
import java.time.Instant

@Component
class TradeLedgerClient(
    private val firestore: Firestore,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val collection = firestore.collection("trade_decisions")

    fun record(
        snapshot: MarketSnapshot,
        decisionType: String,
        reason: String,
        dryRun: Boolean,
        quoteSizeUsd: BigDecimal? = null,
        coinbaseOrderId: String? = null,
        coinbaseSuccess: Boolean? = null,
        errorMessage: String? = null,
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
            )

            collection.add(doc).get()
            log.info("Recorded trade ledger entry: decisionType={} product={}", decisionType, snapshot.productId)
            null
        }
            .subscribeOn(Schedulers.boundedElastic())
            .then()
    }

    fun hasRecentLiveBuy(productId: String, since: Instant): Mono<Boolean> {
        return Mono.fromCallable {
            val snapshot = collection
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
            val snapshot = collection
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