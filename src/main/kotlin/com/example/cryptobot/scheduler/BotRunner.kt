package com.example.cryptobot.scheduler

import com.example.cryptobot.agent.AgentDecisionValidator
import com.example.cryptobot.agent.OpenAiAgentClient
import com.example.cryptobot.alerts.DiscordAlertClient
import com.example.cryptobot.coinbase.CoinbaseClient
import com.example.cryptobot.config.BotProperties
import com.example.cryptobot.persistence.TradeLedgerClient
import com.example.cryptobot.strategy.MarketSnapshot
import com.example.cryptobot.strategy.SimpleDipBuyStrategy
import com.example.cryptobot.strategy.TradingDecision
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

@Component
class BotRunner(
    private val props: BotProperties,
    private val coinbaseClient: CoinbaseClient,
    private val strategy: SimpleDipBuyStrategy,
    private val alerts: DiscordAlertClient,
    private val ledger: TradeLedgerClient,
    private val agentClient: OpenAiAgentClient,
    private val agentValidator: AgentDecisionValidator,
) : CommandLineRunner {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun run(vararg args: String) {
        if (!props.enabled) {
            log.info("Bot disabled.")
            return
        }

        buildSnapshots()
            .flatMap { snapshots ->
                if (props.agentEnabled) {
                    agentClient.decidePortfolio(snapshots)
                        .flatMap { agentDecision ->
                            log.info(
                                "Portfolio agent decision: product={} action={} score={} confidence={} fundingProduct={} reason={}",
                                agentDecision.productId,
                                agentDecision.action,
                                agentDecision.score,
                                agentDecision.confidence,
                                agentDecision.fundingProductId,
                                agentDecision.reason
                            )

                            alerts.send(
                                """
                                🤖 Portfolio Agent Decision

                                Product: ${agentDecision.productId}
                                Action: ${agentDecision.action}
                                Score: ${agentDecision.score}
                                Confidence: ${agentDecision.confidence}
                                Funding Product: ${agentDecision.fundingProductId.ifBlank { "N/A" }}

                                Reason:
                                ${agentDecision.reason}
                                """.trimIndent()
                            ).then(Mono.defer {
                                val validatedDecision = agentValidator.validatePortfolio(snapshots, agentDecision)
                                val primarySnapshot = snapshots.firstOrNull { it.productId == agentDecision.productId }
                                    ?: snapshots.first()
                                execute(primarySnapshot, validatedDecision, snapshots.associateBy { it.productId })
                            })
                        }
                } else {
                    Flux.fromIterable(snapshots)
                        .flatMap { snapshot ->
                            val decision = strategy.decide(snapshot)
                            execute(snapshot, decision)
                        }
                        .then()
                }
            }
            .retryWhen(
                Retry.backoff(2, Duration.ofSeconds(3))
                    .maxBackoff(Duration.ofSeconds(15))
                    .jitter(0.25)
                    .filter { ex ->
                        ex is java.util.concurrent.TimeoutException ||
                                ex is java.net.ConnectException ||
                                ex is java.nio.channels.ClosedChannelException ||
                                ex is javax.net.ssl.SSLException ||
                                ex is org.springframework.web.reactive.function.client.WebClientRequestException ||
                                ex is IllegalStateException && ex.message?.contains("response body has been released") == true
                    }
                    .doBeforeRetry { signal ->
                        log.warn(
                            "Retrying bot tick. attempt={} error={}",
                            signal.totalRetries() + 1,
                            signal.failure().message
                        )
                    }
            )
            .doOnSubscribe { log.info("Bot tick started") }
            .doOnSuccess { log.info("Bot tick completed") }
            .doOnError { ex ->
                log.error("Bot tick failed", ex)
                alerts.send("🚨 Crypto bot failed: `${ex.message ?: ex::class.simpleName}`").block()
            }
            .block()

        log.info("Bot job finished successfully")
    }

    private fun buildSnapshots(): Mono<List<MarketSnapshot>> {
        log.info("Building market snapshots for {}", props.productIds)

        return coinbaseClient.listAccounts()
            .flatMap { accountsResponse ->
                val accounts = accountsResponse.accounts

                val usdAvailable = accounts
                    .filter { it.currency == "USD" }
                    .sumOf { it.availableBalance.decimal() }

                Flux.fromIterable(props.productIds)
                    .flatMap { productId ->
                        val now = Instant.now()
                        val start = now.minus(7, ChronoUnit.DAYS)

                        coinbaseClient.getProduct(productId)
                            .zipWith(
                                coinbaseClient.getCandles(
                                    productId = productId,
                                    granularity = "ONE_HOUR",
                                    start = start,
                                    end = now,
                                )
                            )
                            .flatMap { tuple ->
                                val product = tuple.t1
                                val candles = tuple.t2.candles
                                    .sortedBy { it.start.toLongOrNull() ?: 0L }

                                val closes = candles.mapNotNull { it.close.toBigDecimalOrNull() }
                                val highs = candles.mapNotNull { it.high.toBigDecimalOrNull() }
                                val lows = candles.mapNotNull { it.low.toBigDecimalOrNull() }

                                val price = product.price.toBigDecimalOrNull() ?: BigDecimal.ZERO
                                val baseCurrency = productId.substringBefore("-")

                                val cryptoBalance = accounts
                                    .filter { it.currency == baseCurrency }
                                    .sumOf { it.availableBalance.decimal() }

                                val cryptoValueUsd = cryptoBalance.multiply(price)

                                val close1hAgo = closes.getOrNull((closes.size - 2).coerceAtLeast(0)) ?: price
                                val close4hAgo = closes.getOrNull((closes.size - 5).coerceAtLeast(0)) ?: price
                                val close24hAgo = closes.getOrNull((closes.size - 25).coerceAtLeast(0)) ?: closes.firstOrNull() ?: price
                                val close7dAgo = closes.firstOrNull() ?: price

                                val candleHigh24h = highs.maxOrNull() ?: BigDecimal.ZERO
                                val candleLow24h = lows.minOrNull() ?: BigDecimal.ZERO

                                val candleHigh7d = highs.maxOrNull() ?: BigDecimal.ZERO
                                val candleLow7d = lows.minOrNull() ?: BigDecimal.ZERO

                                val trend1hPercent = pctChange(close1hAgo, price)
                                val trend4hPercent = pctChange(close4hAgo, price)
                                val trend24hPercent = pctChange(close24hAgo, price)
                                val trend7dPercent = pctChange(close7dAgo, price)
                                val rsi14 = calculateRsi(closes)
                                val volatility24hPercent = calculateVolatilityPercent(closes)
                                val marketRegime = classifyMarketRegime(
                                    trend24hPercent = trend24hPercent,
                                    trend7dPercent = trend7dPercent,
                                    rsi14 = rsi14,
                                    volatility24hPercent = volatility24hPercent,
                                )

                                ledger.scorePendingOutcomes(productId, price)
                                    .then(ledger.getPosition(productId, price))
                                    .flatMap { position ->
                                        ledger.getReasonCodeStats(position.activeReasonCode, Instant.now().minus(30, ChronoUnit.DAYS))
                                            .map { reasonStats ->
                                                productId to MarketSnapshot(
                                            productId = productId,
                                            price = price,
                                            change24hPercent = product.pricePercentageChange24h?.toBigDecimalOrNull() ?: BigDecimal.ZERO,
                                            usdAvailable = usdAvailable,
                                            volume24h = product.volume24h?.toBigDecimalOrNull() ?: BigDecimal.ZERO,

                                            high24h = candleHigh24h,
                                            low24h = candleLow24h,
                                            priceChange24h = price.subtract(close24hAgo),
                                            priceTo24hHighPercent = if (candleHigh24h > BigDecimal.ZERO) {
                                                price.divide(candleHigh24h, 4, RoundingMode.HALF_UP).multiply(BigDecimal("100"))
                                            } else BigDecimal.ZERO,
                                            priceTo24hLowPercent = if (candleLow24h > BigDecimal.ZERO) {
                                                price.divide(candleLow24h, 4, RoundingMode.HALF_UP).multiply(BigDecimal("100"))
                                            } else BigDecimal.ZERO,

                                            cryptoBalance = cryptoBalance,
                                            cryptoValueUsd = cryptoValueUsd,
                                            portfolioUsdValue = BigDecimal.ZERO,
                                            portfolioAllocationPercent = BigDecimal.ZERO,

                                            trend1hPercent = trend1hPercent,
                                            trend4hPercent = trend4hPercent,
                                            trend24hPercent = trend24hPercent,
                                            rsi14 = rsi14,
                                            volatility24hPercent = volatility24hPercent,
                                            candleHigh24h = candleHigh24h,
                                            candleLow24h = candleLow24h,
                                            trend7dPercent = trend7dPercent,
                                            candleHigh7d = candleHigh7d,
                                            candleLow7d = candleLow7d,
                                            avgCostBasis = position.avgCostBasis,
                                            totalInvested = position.totalInvested,
                                            realizedPnlUsd = position.realizedPnlUsd,
                                            unrealizedPnlUsd = position.unrealizedPnlUsd,
                                            unrealizedPnlPercent = position.unrealizedPnlPercent,
                                            highestPriceSeen = position.highestPriceSeen,
                                            drawdownFromHighPercent = position.drawdownFromHighPercent,
                                            buyCount = position.buyCount,
                                            sellCount = position.sellCount,
                                            marketRegime = marketRegime,
                                            reasonCode30dWinRate = reasonStats.winRatePercent,
                                            reasonCode30dCount = reasonStats.count,
                                            activeThesis = position.activeThesis,
                                            activeInvalidationCondition = position.activeInvalidationCondition,
                                            activeProfitTargetPercent = position.activeProfitTargetPercent,
                                            activeStopLossPercent = position.activeStopLossPercent,
                                            activeMaxHoldHours = position.activeMaxHoldHours,
                                        )
                                    }
                                }
                            }
                    }
                    .collectList()
                    .map { pairs ->
                        val snapshots = pairs.map { it.second }

                        val cryptoPortfolioValue = snapshots
                            .sumOf { it.cryptoValueUsd }

                        val totalPortfolioValue = usdAvailable + cryptoPortfolioValue

                        snapshots.map { snapshot ->
                            val allocationPercent =
                                if (totalPortfolioValue > BigDecimal.ZERO) {
                                    snapshot.cryptoValueUsd
                                        .divide(totalPortfolioValue, 4, RoundingMode.HALF_UP)
                                        .multiply(BigDecimal("100"))
                                } else {
                                    BigDecimal.ZERO
                                }

                            snapshot.copy(
                                portfolioUsdValue = totalPortfolioValue,
                                portfolioAllocationPercent = allocationPercent,
                            )
                        }
                    }
            }
    }

    private fun execute(
        snapshot: MarketSnapshot,
        decision: TradingDecision,
        snapshotsByProduct: Map<String, MarketSnapshot> = mapOf(snapshot.productId to snapshot),
    ): Mono<Unit> = when (decision) {
        is TradingDecision.Skip -> {
            log.info("SKIP: {}", decision.reason)

            ledger.record(
                snapshot = snapshot,
                decisionType = "SKIP",
                reason = decision.reason,
                dryRun = props.dryRun,
            ).thenReturn(Unit)
        }

        is TradingDecision.Rotate -> {
            val fundingSnapshot = snapshotsByProduct[decision.sell.productId]
            val targetSnapshot = snapshotsByProduct[decision.buy.productId]

            if (fundingSnapshot == null || targetSnapshot == null) {
                val message = "🛑 ROTATE BLOCKED: missing snapshots for sell=${decision.sell.productId} buy=${decision.buy.productId}"
                log.warn(message)
                ledger.record(
                    snapshot = snapshot,
                    decisionType = "BLOCKED_ROTATE",
                    reason = message,
                    dryRun = props.dryRun,
                ).then(alerts.send(message)).thenReturn(Unit)
            } else if (props.dryRun) {
                val message = "🧪 DRY RUN: would ROTATE by selling ${decision.sell.baseSize} of ${decision.sell.productId}, then buying ${decision.buy.quoteSizeUsd} of ${decision.buy.productId}. Reason: ${decision.reason}"
                log.warn(message)
                ledger.record(
                    snapshot = fundingSnapshot,
                    decisionType = "ROTATE_SELL",
                    reason = decision.sell.reason,
                    dryRun = true,
                    baseSize = decision.sell.baseSize,
                    reasonCode = decision.sell.reasonCode,
                ).then(
                    ledger.record(
                        snapshot = targetSnapshot,
                        decisionType = "ROTATE_BUY",
                        reason = decision.buy.reason,
                        dryRun = true,
                        quoteSizeUsd = decision.buy.quoteSizeUsd,
                        reasonCode = decision.buy.reasonCode,
                        thesis = decision.buy.thesis,
                        invalidationCondition = decision.buy.invalidationCondition,
                        profitTargetPercent = decision.buy.profitTargetPercent,
                        stopLossPercent = decision.buy.stopLossPercent,
                        maxHoldHours = decision.buy.maxHoldHours,
                    )
                ).then(alerts.send(message)).thenReturn(Unit)
            } else if (!props.liveTradingEnabled) {
                val message = "🛑 LIVE ROTATE BLOCKED: liveTradingEnabled=false. Would have sold ${decision.sell.baseSize} of ${decision.sell.productId}, then bought ${decision.buy.quoteSizeUsd} of ${decision.buy.productId}"
                log.warn(message)
                ledger.record(
                    snapshot = snapshot,
                    decisionType = "BLOCKED_ROTATE",
                    reason = message,
                    dryRun = false,
                    baseSize = decision.sell.baseSize,
                    quoteSizeUsd = decision.buy.quoteSizeUsd,
                    reasonCode = "REBALANCE",
                ).then(alerts.send(message)).thenReturn(Unit)
            } else {
                val message = "🚨 LIVE ROTATE: SELL ${decision.sell.baseSize} of ${decision.sell.productId}, then BUY ${decision.buy.quoteSizeUsd} of ${decision.buy.productId}. Reason: ${decision.reason}"
                log.warn(message)

                alerts.send(message)
                    .then(coinbaseClient.createMarketSell(decision.sell.productId, decision.sell.baseSize))
                    .flatMap { sellResponse ->
                        ledger.record(
                            snapshot = fundingSnapshot,
                            decisionType = "ROTATE_SELL",
                            reason = decision.sell.reason,
                            dryRun = false,
                            baseSize = decision.sell.baseSize,
                            coinbaseSuccess = sellResponse.success,
                            reasonCode = decision.sell.reasonCode,
                            errorMessage = sellResponse.errorResponse?.toString(),
                        ).then(
                            if (sellResponse.success) {
                                ledger.applyLiveSell(fundingSnapshot, decision.sell.baseSize, decision.sell.reasonCode)
                                    .then(coinbaseClient.createMarketBuy(decision.buy.productId, decision.buy.quoteSizeUsd))
                                    .flatMap { buyResponse ->
                                        ledger.record(
                                            snapshot = targetSnapshot,
                                            decisionType = "ROTATE_BUY",
                                            reason = decision.buy.reason,
                                            dryRun = false,
                                            quoteSizeUsd = decision.buy.quoteSizeUsd,
                                            coinbaseSuccess = buyResponse.success,
                                            reasonCode = decision.buy.reasonCode,
                                            thesis = decision.buy.thesis,
                                            invalidationCondition = decision.buy.invalidationCondition,
                                            profitTargetPercent = decision.buy.profitTargetPercent,
                                            stopLossPercent = decision.buy.stopLossPercent,
                                            maxHoldHours = decision.buy.maxHoldHours,
                                            errorMessage = buyResponse.errorResponse?.toString(),
                                        ).then(
                                            if (buyResponse.success) {
                                                ledger.applyLiveBuy(
                                                    snapshot = targetSnapshot,
                                                    quoteSizeUsd = decision.buy.quoteSizeUsd,
                                                    reasonCode = decision.buy.reasonCode,
                                                    thesis = decision.buy.thesis,
                                                    invalidationCondition = decision.buy.invalidationCondition,
                                                    profitTargetPercent = decision.buy.profitTargetPercent,
                                                    stopLossPercent = decision.buy.stopLossPercent,
                                                    maxHoldHours = decision.buy.maxHoldHours,
                                                )
                                            } else Mono.empty()
                                        )
                                    }
                            } else Mono.empty()
                        )
                    }
                    .thenReturn(Unit)
            }
        }

        is TradingDecision.Buy -> {
            when {
                props.dryRun -> {
                    val message = "🧪 DRY RUN: would BUY ${decision.quoteSizeUsd} of ${decision.productId}. Reason: ${decision.reason}"
                    log.warn(message)

                    ledger.record(
                        snapshot = snapshot,
                        decisionType = "BUY",
                        reason = decision.reason,
                        dryRun = true,
                        quoteSizeUsd = decision.quoteSizeUsd,
                        reasonCode = decision.reasonCode,
                        thesis = decision.thesis,
                        invalidationCondition = decision.invalidationCondition,
                        profitTargetPercent = decision.profitTargetPercent,
                        stopLossPercent = decision.stopLossPercent,
                        maxHoldHours = decision.maxHoldHours,
                    )
                        .then(alerts.send(message))
                        .thenReturn(Unit)
                }

                !props.liveTradingEnabled -> {
                    val message = "🛑 LIVE TRADE BLOCKED: liveTradingEnabled=false. Would have bought ${decision.quoteSizeUsd} of ${decision.productId}"
                    log.warn(message)

                    ledger.record(
                        snapshot = snapshot,
                        decisionType = "BLOCKED_BUY",
                        reason = message,
                        dryRun = false,
                        quoteSizeUsd = decision.quoteSizeUsd,
                    )
                        .then(alerts.send(message))
                        .thenReturn(Unit)
                }

                decision.quoteSizeUsd > props.maxBuyQuoteSizeUsd -> {
                    val message = "🛑 LIVE TRADE BLOCKED: quoteSizeUsd=${decision.quoteSizeUsd} exceeds max=${props.maxBuyQuoteSizeUsd}"
                    log.warn(message)

                    ledger.record(
                        snapshot = snapshot,
                        decisionType = "BLOCKED_BUY",
                        reason = message,
                        dryRun = false,
                        quoteSizeUsd = decision.quoteSizeUsd,
                    )
                        .then(alerts.send(message))
                        .thenReturn(Unit)
                }

                else -> {
                    val recentSince = Instant.now().minus(30, ChronoUnit.MINUTES)
                    val todaySince = Instant.now().truncatedTo(ChronoUnit.DAYS)

                    ledger.hasRecentLiveBuy(decision.productId, recentSince)
                        .flatMap { hasRecentBuy ->
                            if (hasRecentBuy) {
                                val message = "🛑 LIVE TRADE BLOCKED: recent live BUY already exists for ${decision.productId}"
                                log.warn(message)

                                ledger.record(
                                    snapshot = snapshot,
                                    decisionType = "BLOCKED_BUY",
                                    reason = message,
                                    dryRun = false,
                                    quoteSizeUsd = decision.quoteSizeUsd,
                                )
                                    .then(alerts.send(message))
                                    .thenReturn(Unit)
                            } else {
                                ledger.liveBuyTotalSince(todaySince)
                                    .flatMap { spentToday ->
                                        val projectedSpend = spentToday + decision.quoteSizeUsd

                                        if (projectedSpend > props.maxDailyBuyUsd) {
                                            val message =
                                                "🛑 LIVE TRADE BLOCKED: daily buy limit exceeded. spentToday=$spentToday projected=$projectedSpend max=${props.maxDailyBuyUsd}"
                                            log.warn(message)

                                            ledger.record(
                                                snapshot = snapshot,
                                                decisionType = "BLOCKED_BUY",
                                                reason = message,
                                                dryRun = false,
                                                quoteSizeUsd = decision.quoteSizeUsd,
                                            )
                                                .then(alerts.send(message))
                                                .thenReturn(Unit)
                                        } else {
                                            val message =
                                                "🚨 LIVE TRADE: BUY ${decision.quoteSizeUsd} of ${decision.productId}. Reason: ${decision.reason}"
                                            log.warn(message)

                                            alerts.send(message)
                                                .then(
                                                    coinbaseClient.createMarketBuy(
                                                        decision.productId,
                                                        decision.quoteSizeUsd
                                                    )
                                                )
                                                .flatMap { response ->
                                                    ledger.record(
                                                        snapshot = snapshot,
                                                        decisionType = "BUY",
                                                        reason = decision.reason,
                                                        dryRun = false,
                                                        quoteSizeUsd = decision.quoteSizeUsd,
                                                        coinbaseSuccess = response.success,
                                                        reasonCode = decision.reasonCode,
                                                        thesis = decision.thesis,
                                                        invalidationCondition = decision.invalidationCondition,
                                                        profitTargetPercent = decision.profitTargetPercent,
                                                        stopLossPercent = decision.stopLossPercent,
                                                        maxHoldHours = decision.maxHoldHours,
                                                        errorMessage = response.errorResponse?.toString(),
                                                    ).then(
                                                        if (response.success) {
                                                            ledger.applyLiveBuy(
                                                                snapshot = snapshot,
                                                                quoteSizeUsd = decision.quoteSizeUsd,
                                                                reasonCode = decision.reasonCode,
                                                                thesis = decision.thesis,
                                                                invalidationCondition = decision.invalidationCondition,
                                                                profitTargetPercent = decision.profitTargetPercent,
                                                                stopLossPercent = decision.stopLossPercent,
                                                                maxHoldHours = decision.maxHoldHours,
                                                            )
                                                        } else Mono.empty()
                                                    )
                                                }
                                                .thenReturn(Unit)
                                        }
                                    }
                            }
                        }
                }
            }
        }
        is TradingDecision.Sell -> {
            when {
                props.dryRun -> {
                    val message = "🧪 DRY RUN: would SELL ${decision.baseSize} of ${decision.productId}. Reason: ${decision.reason}"
                    log.warn(message)

                    ledger.record(
                        snapshot = snapshot,
                        decisionType = "SELL",
                        reason = decision.reason,
                        dryRun = true,
                        baseSize = decision.baseSize,
                        reasonCode = decision.reasonCode,
                    )
                        .then(alerts.send(message))
                        .thenReturn(Unit)
                }

                !props.liveTradingEnabled -> {
                    val message = "🛑 LIVE SELL BLOCKED: liveTradingEnabled=false. Would have sold ${decision.baseSize} of ${decision.productId}"
                    log.warn(message)

                    ledger.record(
                        snapshot = snapshot,
                        decisionType = "BLOCKED_SELL",
                        reason = message,
                        dryRun = false,
                        baseSize = decision.baseSize,
                    )
                        .then(alerts.send(message))
                        .thenReturn(Unit)
                }

                decision.baseSize > snapshot.cryptoBalance -> {
                    val message = "🛑 LIVE SELL BLOCKED: baseSize=${decision.baseSize} exceeds available balance=${snapshot.cryptoBalance}"
                    log.warn(message)

                    ledger.record(
                        snapshot = snapshot,
                        decisionType = "BLOCKED_SELL",
                        reason = message,
                        dryRun = false,
                        baseSize = decision.baseSize,
                    )
                        .then(alerts.send(message))
                        .thenReturn(Unit)
                }

                else -> {
                    val message = "🚨 LIVE TRADE: SELL ${decision.baseSize} of ${decision.productId}. Reason: ${decision.reason}"
                    log.warn(message)

                    alerts.send(message)
                        .then(coinbaseClient.createMarketSell(decision.productId, decision.baseSize))
                        .flatMap { response ->
                            ledger.record(
                                snapshot = snapshot,
                                decisionType = "SELL",
                                reason = decision.reason,
                                dryRun = false,
                                baseSize = decision.baseSize,
                                coinbaseSuccess = response.success,
                                reasonCode = decision.reasonCode,
                                errorMessage = response.errorResponse?.toString(),
                            ).then(
                                if (response.success) ledger.applyLiveSell(snapshot, decision.baseSize, decision.reasonCode) else Mono.empty()
                            )
                        }
                        .thenReturn(Unit)
                }
            }
        }
    }

    private fun classifyMarketRegime(
        trend24hPercent: BigDecimal,
        trend7dPercent: BigDecimal,
        rsi14: BigDecimal,
        volatility24hPercent: BigDecimal,
    ): String {
        return when {
            trend24hPercent <= BigDecimal("-7.0") || trend7dPercent <= BigDecimal("-15.0") -> "CRASH"
            volatility24hPercent >= BigDecimal("5.0") -> "HIGH_VOLATILITY"
            trend7dPercent >= BigDecimal("8.0") && trend24hPercent >= BigDecimal("1.0") -> "BULL_TREND"
            trend7dPercent <= BigDecimal("-8.0") && trend24hPercent <= BigDecimal("-1.0") -> "BEAR_TREND"
            trend7dPercent < BigDecimal.ZERO && trend24hPercent > BigDecimal("2.0") && rsi14 < BigDecimal("60") -> "RECOVERY"
            trend7dPercent.abs() <= BigDecimal("4.0") -> "SIDEWAYS"
            else -> "UNKNOWN"
        }
    }

    private fun pctChange(old: BigDecimal, current: BigDecimal): BigDecimal {
        if (old <= BigDecimal.ZERO) return BigDecimal.ZERO
        return current.subtract(old)
            .divide(old, 6, RoundingMode.HALF_UP)
            .multiply(BigDecimal("100"))
    }

    private fun calculateRsi(closes: List<BigDecimal>, period: Int = 14): BigDecimal {
        if (closes.size <= period) return BigDecimal.ZERO

        val recent = closes.takeLast(period + 1)
        var gains = BigDecimal.ZERO
        var losses = BigDecimal.ZERO

        for (i in 1 until recent.size) {
            val diff = recent[i].subtract(recent[i - 1])
            if (diff >= BigDecimal.ZERO) gains += diff else losses += diff.abs()
        }

        if (losses == BigDecimal.ZERO) return BigDecimal("100")
        val rs = gains.divide(losses, 6, RoundingMode.HALF_UP)

        return BigDecimal("100").subtract(
            BigDecimal("100").divide(BigDecimal.ONE + rs, 2, RoundingMode.HALF_UP)
        )
    }

    private fun calculateVolatilityPercent(closes: List<BigDecimal>): BigDecimal {
        if (closes.size < 2) return BigDecimal.ZERO

        val returns = closes.zipWithNext { a, b ->
            if (a > BigDecimal.ZERO) {
                b.subtract(a).divide(a, 8, RoundingMode.HALF_UP).toDouble()
            } else {
                0.0
            }
        }

        val mean = returns.average()
        val variance = returns.map { (it - mean) * (it - mean) }.average()
        return BigDecimal(Math.sqrt(variance) * 100).setScale(2, RoundingMode.HALF_UP)
    }
}