package com.example.cryptobot.persistence

import org.springframework.data.r2dbc.repository.Query
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.OffsetDateTime

interface TradeDecisionLogRepository : ReactiveCrudRepository<TradeDecisionLog, Long> {
    @Query(
        """
        SELECT COALESCE(SUM(quote_size_usd), 0)
        FROM trade_decision_log
        WHERE decision_type = 'BUY'
          AND dry_run = false
          AND coinbase_success = true
          AND created_at >= :since
        """
    )
    fun liveBuyTotalSince(since: OffsetDateTime): Mono<BigDecimal>
}