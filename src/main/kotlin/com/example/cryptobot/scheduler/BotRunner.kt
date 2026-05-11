package com.example.cryptobot.scheduler

import com.example.cryptobot.agent.AgentDecisionValidator
import com.example.cryptobot.agent.AgentTradeDecision
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
import org.springframework.boot.SpringApplication
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.system.exitProcess

@Component
class BotRunner(
    private val props: BotProperties,
    private val coinbaseClient: CoinbaseClient,
    private val strategy: SimpleDipBuyStrategy,
    private val alerts: DiscordAlertClient,
    private val ledger: TradeLedgerClient,
    private val agentClient: OpenAiAgentClient,
    private val agentValidator: AgentDecisionValidator,
    private val applicationContext: ConfigurableApplicationContext,
) : CommandLineRunner {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun run(vararg args: String) {
        var exitCode = 0

        try {
            if (!props.enabled) {
                log.info("Bot disabled.")
                return
            }

            buildSnapshots()
                .flatMap { snapshots ->
                    if (props.agentEnabled) {
                        agentClient.decidePortfolioPlan(snapshots)
                            .flatMap { agentPlan ->
                                val decisions = agentPlan.decisions.take(props.maxActionsPerRun)
                                decisions.forEachIndexed { index, agentDecision ->
                                    log.info(
                                        "Portfolio agent decision[{}]: product={} action={} score={} confidence={} fundingProduct={} reason={}",
                                        index + 1,
                                        agentDecision.productId,
                                        agentDecision.action,
                                        agentDecision.score,
                                        agentDecision.confidence,
                                        agentDecision.fundingProductId,
                                        agentDecision.reason
                                    )
                                }

                                executeAgentPlan(snapshots, decisions)
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
        } catch (ex: Throwable) {
            exitCode = 1
            throw ex
        } finally {
            if (props.exitOnCompletion) {
                log.info("Exiting application after bot run with code {}", exitCode)
                val springExitCode = SpringApplication.exit(applicationContext) { exitCode }
                exitProcess(springExitCode)
            }
        }
    }

    private fun formatAgentPlanAlert(decisions: List<AgentTradeDecision>): String {
        if (decisions.isEmpty()) {
            return "🤖 Portfolio Agent Plan: no decisions returned"
        }

        val lines = decisions.mapIndexed { index, decision ->
            val funding = decision.fundingProductId.ifBlank { "N/A" }
            "${index + 1}. ${decision.action} ${decision.productId} score=${decision.score} confidence=${decision.confidence} funding=$funding reason=${decision.reason}"
        }

        return """
            🤖 Portfolio Agent Plan

            Proposed actions: ${decisions.size}
            Max executable this run: ${props.maxActionsPerRun}

            ${lines.joinToString("\n")}
        """.trimIndent()
    }

    private fun executeAgentPlan(
        snapshots: List<MarketSnapshot>,
        agentDecisions: List<AgentTradeDecision>,
    ): Mono<Unit> {
        val snapshotsByProduct = snapshots.associateBy { it.productId }
        val validatedActions = agentDecisions
            .take(props.maxActionsPerRun)
            .map { agentDecision ->
                val primarySnapshot = snapshotsByProduct[agentDecision.productId] ?: snapshots.first()
                ValidatedPlanAction(
                    agentDecision = agentDecision,
                    snapshot = primarySnapshot,
                    decision = agentValidator.validatePortfolio(snapshots, agentDecision),
                )
            }

        val selection = selectExecutableActions(validatedActions, snapshotsByProduct)
        val planStartedMessage = formatAgentPlanAlert(agentDecisions.take(props.maxActionsPerRun))

        val executionMono = if (selection.executable.isEmpty()) {
            val fallbackSnapshot = snapshots.first()
            val reason = selection.skipped.firstOrNull()?.detail ?: "Agent plan contained no executable actions"
            execute(fallbackSnapshot, TradingDecision.Skip(reason), snapshotsByProduct)
        } else {
            Flux.fromIterable(selection.executable)
                .concatMap { action -> execute(action.snapshot, action.decision, snapshotsByProduct) }
                .then()
        }

        val report = buildPlanExecutionReport(snapshots, selection)

        return alerts.send(planStartedMessage)
            .then(executionMono)
            .then(ledger.recordPortfolioRun(report))
            .then(alerts.send(formatPortfolioExecutionReport(report)))
            .thenReturn(Unit)
    }

    private fun selectExecutableActions(
        validatedActions: List<ValidatedPlanAction>,
        snapshotsByProduct: Map<String, MarketSnapshot>,
    ): PlanExecutionSelection {
        var projectedUsd = snapshotsByProduct.values.firstOrNull()?.usdAvailable ?: BigDecimal.ZERO
        var projectedBuyUsd = BigDecimal.ZERO
        var buyCount = 0
        var sellCount = 0
        var rotateCount = 0

        val projectedBalances = snapshotsByProduct.mapValues { it.value.cryptoBalance }.toMutableMap()
        val boughtProducts = mutableSetOf<String>()
        val soldProducts = mutableSetOf<String>()
        val accepted = mutableListOf<ValidatedPlanAction>()
        val skipped = mutableListOf<PlanActionReport>()

        fun canSpend(amount: BigDecimal): Boolean =
            amount > BigDecimal.ZERO &&
                projectedUsd - amount >= props.minUsdCashReserve &&
                projectedBuyUsd + amount <= props.maxTotalBuyUsdPerRun

        fun reject(action: ValidatedPlanAction, detail: String) {
            skipped += PlanActionReport.from(action, "SKIPPED", detail)
        }

        actionLoop@ for (action in validatedActions) {
            val snapshot = action.snapshot
            val decision = action.decision

            when (decision) {
                is TradingDecision.Skip -> reject(action, decision.reason)

                is TradingDecision.Buy -> {
                    if (buyCount >= props.maxBuysPerRun) { reject(action, "max buys per run reached"); continue@actionLoop }
                    if (decision.productId in boughtProducts) { reject(action, "already buying ${decision.productId} this run"); continue@actionLoop }
                    if (!canSpend(decision.quoteSizeUsd)) { reject(action, "insufficient projected USD after reserve or max total buy USD reached"); continue@actionLoop }

                    projectedUsd -= decision.quoteSizeUsd
                    projectedBuyUsd += decision.quoteSizeUsd
                    buyCount += 1
                    boughtProducts += decision.productId
                    accepted += action
                }

                is TradingDecision.Sell -> {
                    if (sellCount >= props.maxSellsPerRun) { reject(action, "max sells per run reached"); continue@actionLoop }
                    if (decision.productId in soldProducts) { reject(action, "already selling ${decision.productId} this run"); continue@actionLoop }

                    val availableBase = projectedBalances[decision.productId] ?: BigDecimal.ZERO
                    if (decision.baseSize <= BigDecimal.ZERO || decision.baseSize > availableBase) { reject(action, "sell size exceeds projected available balance"); continue@actionLoop }

                    val sellSnapshot = snapshotsByProduct[decision.productId] ?: snapshot
                    projectedBalances[decision.productId] = availableBase - decision.baseSize
                    projectedUsd += decision.baseSize.multiply(sellSnapshot.price)
                    sellCount += 1
                    soldProducts += decision.productId
                    accepted += action.copy(snapshot = sellSnapshot)
                }

                is TradingDecision.Rotate -> {
                    if (rotateCount >= props.maxRotationsPerRun) { reject(action, "max rotations per run reached"); continue@actionLoop }
                    if (buyCount >= props.maxBuysPerRun) { reject(action, "max buys per run reached"); continue@actionLoop }
                    if (sellCount >= props.maxSellsPerRun) { reject(action, "max sells per run reached"); continue@actionLoop }
                    if (decision.buy.productId in boughtProducts) { reject(action, "already buying ${decision.buy.productId} this run"); continue@actionLoop }
                    if (decision.sell.productId in soldProducts) { reject(action, "already selling ${decision.sell.productId} this run"); continue@actionLoop }

                    val fundingSnapshot = snapshotsByProduct[decision.sell.productId]
                    if (fundingSnapshot == null) { reject(action, "missing funding snapshot ${decision.sell.productId}"); continue@actionLoop }
                    val targetSnapshot = snapshotsByProduct[decision.buy.productId]
                    if (targetSnapshot == null) { reject(action, "missing target snapshot ${decision.buy.productId}"); continue@actionLoop }
                    val availableBase = projectedBalances[decision.sell.productId] ?: BigDecimal.ZERO
                    if (decision.sell.baseSize <= BigDecimal.ZERO || decision.sell.baseSize > availableBase) { reject(action, "rotation sell size exceeds projected available balance"); continue@actionLoop }

                    val fundingNotionalUsd = decision.sell.baseSize.multiply(fundingSnapshot.price)
                    val projectedUsdAfterRotation = projectedUsd + fundingNotionalUsd - decision.buy.quoteSizeUsd
                    if (projectedUsdAfterRotation < props.minUsdCashReserve) { reject(action, "rotation would violate min USD cash reserve"); continue@actionLoop }
                    if (projectedBuyUsd + decision.buy.quoteSizeUsd > props.maxTotalBuyUsdPerRun) { reject(action, "rotation would exceed max total buy USD per run"); continue@actionLoop }

                    projectedBalances[decision.sell.productId] = availableBase - decision.sell.baseSize
                    projectedUsd = projectedUsdAfterRotation
                    projectedBuyUsd += decision.buy.quoteSizeUsd
                    rotateCount += 1
                    buyCount += 1
                    sellCount += 1
                    boughtProducts += decision.buy.productId
                    soldProducts += decision.sell.productId
                    accepted += action.copy(snapshot = targetSnapshot)
                }
            }
        }

        return PlanExecutionSelection(executable = accepted, skipped = skipped)
    }

    private fun buildPlanExecutionReport(
        snapshots: List<MarketSnapshot>,
        selection: PlanExecutionSelection,
    ): PortfolioRunReport {
        val proposed = selection.executable.map { PlanActionReport.from(it, "EXECUTED", "Submitted to execution pipeline") } + selection.skipped
        return PortfolioRunReport(
            createdAt = Instant.now(),
            dryRun = props.dryRun,
            liveTradingEnabled = props.liveTradingEnabled,
            proposedActionCount = proposed.size,
            executedActionCount = selection.executable.size,
            skippedActionCount = selection.skipped.size,
            actions = proposed,
            portfolioBefore = snapshots.sortedByDescending { it.cryptoValueUsd }.map { snapshot ->
                PortfolioHoldingReport(
                    productId = snapshot.productId,
                    cryptoValueUsd = snapshot.cryptoValueUsd,
                    allocationPercent = snapshot.portfolioAllocationPercent,
                    unrealizedPnlUsd = snapshot.unrealizedPnlUsd,
                    unrealizedPnlPercent = snapshot.unrealizedPnlPercent,
                )
            },
            projectedPortfolio = projectPortfolioAfter(snapshots, selection.executable),
        )
    }

    private fun projectPortfolioAfter(
        snapshots: List<MarketSnapshot>,
        executableActions: List<ValidatedPlanAction>,
    ): List<PortfolioHoldingReport> {
        val byProduct = snapshots.associateBy { it.productId }
        val projectedBalances = snapshots.associate { it.productId to it.cryptoBalance }.toMutableMap()
        var projectedUsd = snapshots.firstOrNull()?.usdAvailable ?: BigDecimal.ZERO

        for (action in executableActions) {
            when (val decision = action.decision) {
                is TradingDecision.Buy -> {
                    val snapshot = byProduct[decision.productId] ?: continue
                    projectedUsd -= decision.quoteSizeUsd
                    projectedBalances[decision.productId] = (projectedBalances[decision.productId] ?: BigDecimal.ZERO) +
                        decision.quoteSizeUsd.divide(snapshot.price, 12, RoundingMode.HALF_UP)
                }

                is TradingDecision.Sell -> {
                    val snapshot = byProduct[decision.productId] ?: continue
                    projectedUsd += decision.baseSize.multiply(snapshot.price)
                    projectedBalances[decision.productId] = (projectedBalances[decision.productId] ?: BigDecimal.ZERO) - decision.baseSize
                }

                is TradingDecision.Rotate -> {
                    val fundingSnapshot = byProduct[decision.sell.productId] ?: continue
                    val targetSnapshot = byProduct[decision.buy.productId] ?: continue
                    projectedUsd += decision.sell.baseSize.multiply(fundingSnapshot.price)
                    projectedBalances[decision.sell.productId] = (projectedBalances[decision.sell.productId] ?: BigDecimal.ZERO) - decision.sell.baseSize
                    projectedUsd -= decision.buy.quoteSizeUsd
                    projectedBalances[decision.buy.productId] = (projectedBalances[decision.buy.productId] ?: BigDecimal.ZERO) +
                        decision.buy.quoteSizeUsd.divide(targetSnapshot.price, 12, RoundingMode.HALF_UP)
                }

                is TradingDecision.Skip -> Unit
            }
        }

        val cryptoValues = snapshots.map { snapshot ->
            val value = (projectedBalances[snapshot.productId] ?: BigDecimal.ZERO).multiply(snapshot.price)
            snapshot.productId to value
        }
        val total = cryptoValues.sumOf { it.second } + projectedUsd

        return cryptoValues
            .map { (productId, value) ->
                PortfolioHoldingReport(
                    productId = productId,
                    cryptoValueUsd = value,
                    allocationPercent = if (total > BigDecimal.ZERO) {
                        value.divide(total, 6, RoundingMode.HALF_UP).multiply(BigDecimal("100"))
                    } else BigDecimal.ZERO,
                    unrealizedPnlUsd = byProduct[productId]?.unrealizedPnlUsd ?: BigDecimal.ZERO,
                    unrealizedPnlPercent = byProduct[productId]?.unrealizedPnlPercent ?: BigDecimal.ZERO,
                )
            }
            .sortedByDescending { it.cryptoValueUsd }
    }

    private fun formatPortfolioExecutionReport(report: PortfolioRunReport): String {
        val executed = report.actions.filter { it.status == "EXECUTED" }
        val skipped = report.actions.filter { it.status != "EXECUTED" }

        val executedLines = if (executed.isEmpty()) {
            "None"
        } else {
            executed.joinToString("\n") { "- ${it.action} ${it.productId}: ${it.detail}" }
        }

        val skippedLines = if (skipped.isEmpty()) {
            "None"
        } else {
            skipped.take(8).joinToString("\n") { "- ${it.action} ${it.productId}: ${it.detail}" }
        }

        val projectedLines = report.projectedPortfolio
            .filter { it.cryptoValueUsd > BigDecimal.ZERO }
            .take(8)
            .joinToString("\n") { "- ${it.productId}: \$${it.cryptoValueUsd.setScale(2, RoundingMode.HALF_UP)} (${it.allocationPercent.setScale(1, RoundingMode.HALF_UP)}%)" }
            .ifBlank { "No crypto holdings projected" }

        val mode = if (report.dryRun) "DRY_RUN" else if (report.liveTradingEnabled) "LIVE" else "LIVE_BLOCKED"

        return """
            📊 Portfolio Execution Report [$mode]

            Proposed: ${report.proposedActionCount}
            Execution attempts: ${report.executedActionCount}
            Skipped/rejected: ${report.skippedActionCount}

            EXECUTED / ATTEMPTED:
            $executedLines

            SKIPPED / REJECTED:
            $skippedLines

            Projected portfolio after accepted actions:
            $projectedLines
        """.trimIndent()
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

internal data class ValidatedPlanAction(
    val agentDecision: AgentTradeDecision,
    val snapshot: MarketSnapshot,
    val decision: TradingDecision,
)

private data class PlanExecutionSelection(
    val executable: List<ValidatedPlanAction>,
    val skipped: List<PlanActionReport>,
)

data class PortfolioRunReport(
    val createdAt: Instant,
    val dryRun: Boolean,
    val liveTradingEnabled: Boolean,
    val proposedActionCount: Int,
    val executedActionCount: Int,
    val skippedActionCount: Int,
    val actions: List<PlanActionReport>,
    val portfolioBefore: List<PortfolioHoldingReport>,
    val projectedPortfolio: List<PortfolioHoldingReport>,
)

data class PlanActionReport(
    val action: String,
    val productId: String,
    val status: String,
    val detail: String,
    val score: BigDecimal,
    val confidence: BigDecimal,
    val quoteSizeUsd: BigDecimal,
    val baseSize: BigDecimal,
    val fundingProductId: String,
    val fundingBaseSize: BigDecimal,
) {
    fun asMap(): Map<String, Any?> = mapOf(
        "action" to action,
        "productId" to productId,
        "status" to status,
        "detail" to detail,
        "score" to score.toPlainString(),
        "confidence" to confidence.toPlainString(),
        "quoteSizeUsd" to quoteSizeUsd.toPlainString(),
        "baseSize" to baseSize.toPlainString(),
        "fundingProductId" to fundingProductId,
        "fundingBaseSize" to fundingBaseSize.toPlainString(),
    )

    companion object {
        internal fun from(action: ValidatedPlanAction, status: String, detail: String): PlanActionReport {
            val agentDecision = action.agentDecision
            return PlanActionReport(
                action = agentDecision.action,
                productId = agentDecision.productId,
                status = status,
                detail = detail,
                score = agentDecision.score,
                confidence = agentDecision.confidence,
                quoteSizeUsd = agentDecision.quoteSizeUsd,
                baseSize = agentDecision.baseSize,
                fundingProductId = agentDecision.fundingProductId,
                fundingBaseSize = agentDecision.fundingBaseSize,
            )
        }
    }
}

data class PortfolioHoldingReport(
    val productId: String,
    val cryptoValueUsd: BigDecimal,
    val allocationPercent: BigDecimal,
    val unrealizedPnlUsd: BigDecimal,
    val unrealizedPnlPercent: BigDecimal,
) {
    fun asMap(): Map<String, Any?> = mapOf(
        "productId" to productId,
        "cryptoValueUsd" to cryptoValueUsd.toPlainString(),
        "allocationPercent" to allocationPercent.toPlainString(),
        "unrealizedPnlUsd" to unrealizedPnlUsd.toPlainString(),
        "unrealizedPnlPercent" to unrealizedPnlPercent.toPlainString(),
    )
}
