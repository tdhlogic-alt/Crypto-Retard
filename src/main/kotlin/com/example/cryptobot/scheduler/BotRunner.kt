package com.example.cryptobot.scheduler

import com.example.cryptobot.agent.AgentDecisionValidator
import com.example.cryptobot.agent.AgentTradeDecision
import com.example.cryptobot.agent.OpenAiAgentClient
import com.example.cryptobot.alerts.DiscordAlertClient
import com.example.cryptobot.coinbase.CoinbaseClient
import com.example.cryptobot.config.BotProperties
import com.example.cryptobot.config.CoinbaseProperties
import com.example.cryptobot.persistence.TradeLedgerClient
import com.example.cryptobot.strategy.MarketSnapshot
import com.example.cryptobot.strategy.DeterministicStrategyEngine
import com.example.cryptobot.strategy.SimpleDipBuyStrategy
import com.example.cryptobot.strategy.StrategySignal
import com.example.cryptobot.strategy.TradingDecision
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.ExitCodeGenerator
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
    private val coinbaseProps: CoinbaseProperties,
    private val strategy: SimpleDipBuyStrategy,
    private val deterministicStrategyEngine: DeterministicStrategyEngine,
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
                    ledger.recordDailyPaperSummary(snapshots)
                        .then(Mono.defer {
                            when {
                                props.strategyModeEnabled -> executeStrategyFirstPlan(snapshots)
                                props.agentEnabled -> {
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
                                }
                                else -> {
                                    Flux.fromIterable(snapshots)
                                        .flatMap { snapshot ->
                                            val decision = strategy.decide(snapshot)
                                            execute(snapshot, decision)
                                        }
                                        .then(Mono.just(Unit))
                                }
                            }
                        })
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
                val springExitCode = SpringApplication.exit(
                    applicationContext,
                    { exitCode }
                )
                exitProcess(springExitCode)
            }
        }
    }


    private fun executeStrategyFirstPlan(snapshots: List<MarketSnapshot>): Mono<Unit> {
        val signals = deterministicStrategyEngine.propose(snapshots)
        val snapshotsByProduct = snapshots.associateBy { it.productId }

        if (signals.isEmpty()) {
            val reason = "Strategy-first mode: no deterministic strategy edge. AI was not asked to invent a trade."
            val report = buildStrategyFirstReport(
                snapshots = snapshots,
                signals = emptyList(),
                executedSignals = emptyList(),
                skippedActions = listOf(
                    PlanActionReport.strategySkip(
                        productId = snapshots.first().productId,
                        detail = reason,
                    ),
                ),
            )
            return execute(snapshots.first(), TradingDecision.Skip(reason), snapshotsByProduct)
                .then(ledger.recordPortfolioRun(report))
                .then(alerts.send("🧠 Strategy-first plan: SKIP. $reason"))
                .then(Mono.just(Unit))
        }

        val hardExits = signals.filter { it.hardExit || !it.requiresAiApproval }
        val aiGated = signals.filter { it.requiresAiApproval && !it.hardExit }

        val hardExitExecution = if (hardExits.isEmpty()) {
            Mono.empty()
        } else {
            alerts.send(formatStrategySignalAlert("Deterministic hard exits", hardExits))
                .thenMany(Flux.fromIterable(hardExits))
                .concatMap { signal -> execute(signal.snapshotOrFallback(snapshotsByProduct, snapshots), signal.decision, snapshotsByProduct) }
                .then()
        }

        val aiGatedExecution = if (aiGated.isEmpty()) {
            Mono.empty()
        } else if (!props.strategyAiVetoEnabled || !props.agentEnabled) {
            alerts.send(formatStrategySignalAlert("Deterministic entries without AI veto", aiGated))
                .thenMany(Flux.fromIterable(aiGated))
                .concatMap { signal -> execute(signal.snapshotOrFallback(snapshotsByProduct, snapshots), signal.decision, snapshotsByProduct) }
                .then()
        } else {
            agentClient.decideStrategyVetoPlan(snapshots, aiGated)
                .flatMap { vetoPlan ->
                    val approved = applyAiVeto(aiGated, vetoPlan.decisions)
                    val blockedSignals = aiGated.filter { it !in approved }
                    val blockedCount = blockedSignals.size
                    val missedLedger = Flux.fromIterable(blockedSignals)
                        .concatMap { signal ->
                            val blockedSnapshot = signal.snapshotOrFallback(snapshotsByProduct, snapshots)
                            ledger.recordMissedTrade(
                                snapshot = blockedSnapshot,
                                decisionType = signal.decision.actionName(),
                                reason = "AI veto blocked deterministic strategy signal: ${signal.rationale}",
                                quoteSizeUsd = (signal.decision as? TradingDecision.Buy)?.quoteSizeUsd,
                                baseSize = (signal.decision as? TradingDecision.Sell)?.baseSize,
                                reasonCode = signal.decision.reasonCodeOrDefault(),
                            )
                        }
                        .then()
                    val alert = formatStrategySignalAlert(
                        title = "Strategy entries after AI veto: approved=${approved.size}, blocked=$blockedCount",
                        signals = approved,
                    )
                    missedLedger
                        .then(alerts.send(alert))
                        .thenMany(Flux.fromIterable(approved))
                        .concatMap { signal -> execute(signal.snapshotOrFallback(snapshotsByProduct, snapshots), signal.decision, snapshotsByProduct) }
                        .then()
                }
        }

        val report = buildStrategyFirstReport(
            snapshots = snapshots,
            signals = signals,
            executedSignals = hardExits + if (!props.strategyAiVetoEnabled || !props.agentEnabled) aiGated else emptyList(),
            skippedActions = emptyList(),
        )

        return hardExitExecution
            .then(aiGatedExecution)
            .then(ledger.recordPortfolioRun(report))
            .then(Mono.just(Unit))
    }

    private fun applyAiVeto(
        proposedSignals: List<StrategySignal>,
        vetoDecisions: List<AgentTradeDecision>,
    ): List<StrategySignal> {
        val approvals = vetoDecisions
            .filter { it.action != "SKIP" && it.confidence >= props.agentMinConfidence }
            .map { it.productId to it.action }
            .toSet()

        return proposedSignals.filter { signal ->
            val decision = signal.decision
            val key = when (decision) {
                is TradingDecision.Buy -> decision.productId to "BUY"
                is TradingDecision.Sell -> decision.productId to "SELL"
                is TradingDecision.Rotate -> decision.buy.productId to "ROTATE"
                is TradingDecision.Skip -> "" to "SKIP"
            }
            key in approvals
        }
    }

    private fun StrategySignal.snapshotOrFallback(
        snapshotsByProduct: Map<String, MarketSnapshot>,
        snapshots: List<MarketSnapshot>,
    ): MarketSnapshot {
        val productId = when (val decision = decision) {
            is TradingDecision.Buy -> decision.productId
            is TradingDecision.Sell -> decision.productId
            is TradingDecision.Rotate -> decision.buy.productId
            is TradingDecision.Skip -> snapshots.first().productId
        }
        return snapshotsByProduct[productId] ?: snapshots.first()
    }

    private fun formatStrategySignalAlert(title: String, signals: List<StrategySignal>): String {
        val lines = if (signals.isEmpty()) "None" else signals.joinToString("\n") { signal ->
            val action = when (val decision = signal.decision) {
                is TradingDecision.Buy -> "BUY ${decision.productId} ${'$'}${decision.quoteSizeUsd}"
                is TradingDecision.Sell -> "SELL ${decision.productId} base=${decision.baseSize}"
                is TradingDecision.Rotate -> "ROTATE ${decision.sell.productId} -> ${decision.buy.productId}"
                is TradingDecision.Skip -> "SKIP"
            }
            "- $action strategy=${signal.strategyName} hardExit=${signal.hardExit} priority=${signal.priority}: ${signal.rationale}"
        }

        return """
            🧠 $title

            $lines
        """.trimIndent()
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

        fun buyRejectionReason(amount: BigDecimal): String? = when {
            amount <= BigDecimal.ZERO -> "buy size must be positive"
            projectedUsd - amount < props.minUsdCashReserve ->
                "insufficient projected USD after reserve. projectedUsd=$projectedUsd amount=$amount reserve=${props.minUsdCashReserve}"
            projectedBuyUsd + amount > props.maxTotalBuyUsdPerRun ->
                "max total buy USD per run reached. projectedBuyUsd=$projectedBuyUsd amount=$amount max=${props.maxTotalBuyUsdPerRun}"
            else -> null
        }

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
                    if (decision.productId in soldProducts) { reject(action, "not rebuying ${decision.productId} in same run after a sell"); continue@actionLoop }
                    val buyRejection = buyRejectionReason(decision.quoteSizeUsd)
                    if (buyRejection != null) { reject(action, buyRejection); continue@actionLoop }

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
                    if (decision.buy.productId in soldProducts) { reject(action, "not rebuying ${decision.buy.productId} in same run after a sell"); continue@actionLoop }
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
                    if (decision.buy.quoteSizeUsd <= BigDecimal.ZERO) { reject(action, "rotation buy size must be positive"); continue@actionLoop }
                    if (projectedBuyUsd + decision.buy.quoteSizeUsd > props.maxTotalBuyUsdPerRun) {
                        reject(action, "rotation buy rejected: max total buy USD per run reached. projectedBuyUsd=$projectedBuyUsd amount=${decision.buy.quoteSizeUsd} max=${props.maxTotalBuyUsdPerRun}")
                        continue@actionLoop
                    }

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

    private fun buildStrategyFirstReport(
        snapshots: List<MarketSnapshot>,
        signals: List<StrategySignal>,
        executedSignals: List<StrategySignal>,
        skippedActions: List<PlanActionReport>,
    ): PortfolioRunReport {
        val executedSet = executedSignals.toSet()
        val signalActions = signals.map { signal ->
            PlanActionReport.fromStrategySignal(
                signal = signal,
                status = if (signal in executedSet) "EXECUTED" else "PROPOSED",
                detail = signal.rationale,
            )
        }
        val actions = signalActions + skippedActions
        return PortfolioRunReport(
            createdAt = Instant.now(),
            dryRun = props.dryRun,
            liveTradingEnabled = props.liveTradingEnabled,
            proposedActionCount = actions.size,
            executedActionCount = actions.count { it.status == "EXECUTED" },
            skippedActionCount = actions.count { it.status != "EXECUTED" },
            actions = actions,
            portfolioBefore = snapshots.sortedByDescending { it.cryptoValueUsd }.map { snapshot ->
                PortfolioHoldingReport(
                    productId = snapshot.productId,
                    cryptoValueUsd = snapshot.cryptoValueUsd,
                    allocationPercent = snapshot.portfolioAllocationPercent,
                    unrealizedPnlUsd = snapshot.unrealizedPnlUsd,
                    unrealizedPnlPercent = snapshot.unrealizedPnlPercent,
                    change24hPercent = snapshot.change24hPercent,
                    marketRegime = snapshot.marketRegime,
                )
            },
            projectedPortfolio = projectPortfolioAfter(snapshots, emptyList()),
            summary = buildPortfolioRunSummary(snapshots, actions),
            topOpenLosers = buildTopOpenLosers(snapshots),
            skipReasonCounts = buildSkipReasonCounts(actions),
            baseline24h = buildBaseline24h(snapshots),
        )
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
                    change24hPercent = snapshot.change24hPercent,
                    marketRegime = snapshot.marketRegime,
                )
            },
            projectedPortfolio = projectPortfolioAfter(snapshots, selection.executable),
            summary = buildPortfolioRunSummary(snapshots, proposed),
            topOpenLosers = buildTopOpenLosers(snapshots),
            skipReasonCounts = buildSkipReasonCounts(proposed),
            baseline24h = buildBaseline24h(snapshots),
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
                    change24hPercent = byProduct[productId]?.change24hPercent ?: BigDecimal.ZERO,
                    marketRegime = byProduct[productId]?.marketRegime ?: "UNKNOWN",
                )
            }
            .sortedByDescending { it.cryptoValueUsd }
    }

    private fun buildPortfolioRunSummary(
        snapshots: List<MarketSnapshot>,
        actions: List<PlanActionReport>,
    ): PortfolioRunSummary {
        val cashUsd = snapshots.firstOrNull()?.usdAvailable ?: BigDecimal.ZERO
        val cryptoValueUsd = snapshots.sumOf { it.cryptoValueUsd }
        val totalValueUsd = cashUsd + cryptoValueUsd
        val unrealizedPnlUsd = snapshots.sumOf { it.unrealizedPnlUsd }
        val realizedPnlUsd = snapshots.sumOf { it.realizedPnlUsd }
        val buyUsd = actions.sumOf { if (it.action == "BUY" || it.action == "ROTATE") it.quoteSizeUsd else BigDecimal.ZERO }
        val sellUsd = actions.sumOf { action ->
            if (action.action == "SELL" || action.action == "ROTATE") {
                val snapshot = snapshots.firstOrNull { it.productId == action.productId }
                action.baseSize.multiply(snapshot?.price ?: BigDecimal.ZERO)
            } else BigDecimal.ZERO
        }

        return PortfolioRunSummary(
            totalValueUsd = totalValueUsd,
            cashUsd = cashUsd,
            cryptoValueUsd = cryptoValueUsd,
            realizedPnlUsd = realizedPnlUsd,
            unrealizedPnlUsd = unrealizedPnlUsd,
            buyUsd = buyUsd,
            sellUsd = sellUsd,
            heldPositionCount = snapshots.count { it.cryptoValueUsd > BigDecimal.ZERO },
        )
    }

    private fun buildTopOpenLosers(snapshots: List<MarketSnapshot>): List<PortfolioLoserReport> {
        return snapshots
            .filter { it.cryptoValueUsd > BigDecimal.ZERO && it.unrealizedPnlUsd < BigDecimal.ZERO }
            .sortedBy { it.unrealizedPnlPercent }
            .take(5)
            .map {
                PortfolioLoserReport(
                    productId = it.productId,
                    cryptoValueUsd = it.cryptoValueUsd,
                    unrealizedPnlUsd = it.unrealizedPnlUsd,
                    unrealizedPnlPercent = it.unrealizedPnlPercent,
                    drawdownFromHighPercent = it.drawdownFromHighPercent,
                    marketRegime = it.marketRegime,
                )
            }
    }

    private fun buildSkipReasonCounts(actions: List<PlanActionReport>): List<SkipReasonCount> {
        return actions
            .filter { it.status != "EXECUTED" }
            .groupingBy { it.detail.take(140) }
            .eachCount()
            .entries
            .sortedByDescending { it.value }
            .take(8)
            .map { SkipReasonCount(reason = it.key, count = it.value) }
    }

    private fun buildBaseline24h(snapshots: List<MarketSnapshot>): List<Baseline24hReport> {
        val held = snapshots.filter { it.cryptoValueUsd > BigDecimal.ZERO }
        val heldValue = held.sumOf { it.cryptoValueUsd }
        val heldWeighted24h = if (heldValue > BigDecimal.ZERO) {
            held.sumOf { it.change24hPercent.multiply(it.cryptoValueUsd) }
                .divide(heldValue, 6, RoundingMode.HALF_UP)
        } else BigDecimal.ZERO

        val baselines = mutableListOf<Baseline24hReport>()
        snapshots.firstOrNull { it.productId == "BTC-USD" }?.let {
            baselines += Baseline24hReport("BTC-USD", it.change24hPercent)
        }
        snapshots.firstOrNull { it.productId == "ETH-USD" }?.let {
            baselines += Baseline24hReport("ETH-USD", it.change24hPercent)
        }
        if (held.isNotEmpty()) {
            baselines += Baseline24hReport("HELD_WEIGHTED", heldWeighted24h)
        }
        val watchlistWeighted = if (snapshots.isNotEmpty()) {
            snapshots.sumOf { it.change24hPercent }.divide(BigDecimal(snapshots.size), 6, RoundingMode.HALF_UP)
        } else BigDecimal.ZERO
        baselines += Baseline24hReport("WATCHLIST_AVG", watchlistWeighted)
        return baselines
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
        val loserLines = report.topOpenLosers
            .joinToString("\n") { "- ${it.productId}: pnl=${it.unrealizedPnlPercent.setScale(2, RoundingMode.HALF_UP)}% (${'$'}${it.unrealizedPnlUsd.setScale(2, RoundingMode.HALF_UP)}) drawdown=${it.drawdownFromHighPercent.setScale(2, RoundingMode.HALF_UP)}% regime=${it.marketRegime}" }
            .ifBlank { "None" }
        val baselineLines = report.baseline24h
            .joinToString("\n") { "- ${it.name}: ${it.change24hPercent.setScale(2, RoundingMode.HALF_UP)}%" }
            .ifBlank { "None" }
        val skipReasonLines = report.skipReasonCounts
            .joinToString("\n") { "- ${it.count}x ${it.reason}" }
            .ifBlank { "None" }

        val mode = if (report.dryRun) "DRY_RUN" else if (report.liveTradingEnabled) "LIVE" else "LIVE_BLOCKED"

        return """
            📊 Portfolio Execution Report [$mode]

            Proposed: ${report.proposedActionCount}
            Execution attempts: ${report.executedActionCount}
            Skipped/rejected: ${report.skippedActionCount}
            Portfolio value: ${'$'}${report.summary.totalValueUsd.setScale(2, RoundingMode.HALF_UP)} cash=${'$'}${report.summary.cashUsd.setScale(2, RoundingMode.HALF_UP)} crypto=${'$'}${report.summary.cryptoValueUsd.setScale(2, RoundingMode.HALF_UP)} unrealized=${'$'}${report.summary.unrealizedPnlUsd.setScale(2, RoundingMode.HALF_UP)}
            Paper flow this run: buys=${'$'}${report.summary.buyUsd.setScale(2, RoundingMode.HALF_UP)} sells=${'$'}${report.summary.sellUsd.setScale(2, RoundingMode.HALF_UP)} heldPositions=${report.summary.heldPositionCount}
            Buy budget this run: max=${'$'}${props.maxTotalBuyUsdPerRun}, maxBuys=${props.maxBuysPerRun}, maxDailyBuys=${props.maxDailyLiveBuys}, maxDailyPerProduct=${props.maxDailyLiveBuysPerProduct}

            24h baselines:
            $baselineLines

            Top open losers:
            $loserLines

            Top skip/rejection reasons:
            $skipReasonLines

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
                    .flatMap({ productId ->
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
                                    .then(ledger.getEffectivePosition(productId, price, props.dryRun))
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
                    }, coinbaseProps.snapshotConcurrency.coerceAtLeast(1))
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


    private fun TradingDecision.actionName(): String = when (this) {
        is TradingDecision.Buy -> "BUY"
        is TradingDecision.Sell -> "SELL"
        is TradingDecision.Rotate -> "ROTATE"
        is TradingDecision.Skip -> "SKIP"
    }

    private fun TradingDecision.reasonCodeOrDefault(): String = when (this) {
        is TradingDecision.Buy -> reasonCode
        is TradingDecision.Sell -> reasonCode
        is TradingDecision.Rotate -> buy.reasonCode
        is TradingDecision.Skip -> "NO_CLEAR_EDGE"
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
                )
                    .then(ledger.applyPaperSell(fundingSnapshot, decision.sell.baseSize, decision.sell.reasonCode, "DRY_RUN_ROTATE"))
                    .then(ledger.applyPaperBuy(
                        snapshot = targetSnapshot,
                        quoteSizeUsd = decision.buy.quoteSizeUsd,
                        reasonCode = decision.buy.reasonCode,
                        thesis = decision.buy.thesis,
                        invalidationCondition = decision.buy.invalidationCondition,
                        profitTargetPercent = decision.buy.profitTargetPercent,
                        stopLossPercent = decision.buy.stopLossPercent,
                        maxHoldHours = decision.buy.maxHoldHours,
                        source = "DRY_RUN_ROTATE",
                    ))
                    .then(alerts.send(message)).thenReturn(Unit)
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
                        .then(ledger.applyPaperBuy(
                            snapshot = snapshot,
                            quoteSizeUsd = decision.quoteSizeUsd,
                            reasonCode = decision.reasonCode,
                            thesis = decision.thesis,
                            invalidationCondition = decision.invalidationCondition,
                            profitTargetPercent = decision.profitTargetPercent,
                            stopLossPercent = decision.stopLossPercent,
                            maxHoldHours = decision.maxHoldHours,
                        ))
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
                    val recentBuySince = Instant.now().minus(props.buyCooldownHours, ChronoUnit.HOURS)
                    val recentSellSince = Instant.now().minus(props.postSellCooldownHours, ChronoUnit.HOURS)
                    val todaySince = Instant.now().truncatedTo(ChronoUnit.DAYS)

                    ledger.hasRecentLiveBuy(decision.productId, recentBuySince)
                        .zipWith(ledger.hasRecentLiveSell(decision.productId, recentSellSince))
                        .flatMap { cooldowns ->
                            val hasRecentBuy = cooldowns.t1
                            val hasRecentSell = cooldowns.t2

                            if (hasRecentBuy) {
                                blockBuy(snapshot, decision, "${decision.productId} is in ${props.buyCooldownHours}h buy cooldown")
                            } else if (hasRecentSell) {
                                blockBuy(snapshot, decision, "${decision.productId} is in ${props.postSellCooldownHours}h post-sell cooldown")
                            } else {
                                ledger.liveBuyCountSince(todaySince)
                                    .zipWith(ledger.liveBuyCountSince(decision.productId, todaySince))
                                    .flatMap { counts ->
                                        val buyCountToday = counts.t1
                                        val productBuyCountToday = counts.t2

                                        when {
                                            buyCountToday >= props.maxDailyLiveBuys -> {
                                                blockBuy(snapshot, decision, "daily live buy count limit reached. buysToday=$buyCountToday max=${props.maxDailyLiveBuys}")
                                            }
                                            productBuyCountToday >= props.maxDailyLiveBuysPerProduct -> {
                                                blockBuy(snapshot, decision, "daily live buy count limit reached for ${decision.productId}. productBuysToday=$productBuyCountToday max=${props.maxDailyLiveBuysPerProduct}")
                                            }
                                            else -> {
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
                        .then(ledger.applyPaperSell(snapshot, decision.baseSize, decision.reasonCode))
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
                    val sellNotionalUsd = decision.baseSize.multiply(snapshot.price)
                    val fullPositionNotionalUsd = snapshot.cryptoBalance.multiply(snapshot.price)

                    when {
                        sellNotionalUsd < props.minSellNotionalUsd && fullPositionNotionalUsd >= props.minSellNotionalUsd -> {
                            val message = "🛑 LIVE SELL BLOCKED: sell notional $sellNotionalUsd is below min=${props.minSellNotionalUsd}"
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
        }
    }

    private fun blockBuy(
        snapshot: MarketSnapshot,
        decision: TradingDecision.Buy,
        reason: String,
    ): Mono<Unit> {
        val message = "🛑 LIVE TRADE BLOCKED: $reason"
        log.warn(message)

        return ledger.record(
            snapshot = snapshot,
            decisionType = "BLOCKED_BUY",
            reason = message,
            dryRun = false,
            quoteSizeUsd = decision.quoteSizeUsd,
            reasonCode = decision.reasonCode,
        )
            .then(ledger.recordMissedTrade(snapshot, "BUY", message, quoteSizeUsd = decision.quoteSizeUsd, reasonCode = decision.reasonCode))
            .then(alerts.send(message))
            .thenReturn(Unit)
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
    val summary: PortfolioRunSummary,
    val topOpenLosers: List<PortfolioLoserReport>,
    val skipReasonCounts: List<SkipReasonCount>,
    val baseline24h: List<Baseline24hReport>,
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
        fun fromStrategySignal(signal: StrategySignal, status: String, detail: String): PlanActionReport {
            return when (val decision = signal.decision) {
                is TradingDecision.Buy -> PlanActionReport(
                    action = "BUY",
                    productId = decision.productId,
                    status = status,
                    detail = detail,
                    score = BigDecimal.ZERO,
                    confidence = BigDecimal.ZERO,
                    quoteSizeUsd = decision.quoteSizeUsd,
                    baseSize = BigDecimal.ZERO,
                    fundingProductId = "",
                    fundingBaseSize = BigDecimal.ZERO,
                )
                is TradingDecision.Sell -> PlanActionReport(
                    action = "SELL",
                    productId = decision.productId,
                    status = status,
                    detail = detail,
                    score = BigDecimal.ZERO,
                    confidence = BigDecimal.ZERO,
                    quoteSizeUsd = BigDecimal.ZERO,
                    baseSize = decision.baseSize,
                    fundingProductId = "",
                    fundingBaseSize = BigDecimal.ZERO,
                )
                is TradingDecision.Rotate -> PlanActionReport(
                    action = "ROTATE",
                    productId = decision.buy.productId,
                    status = status,
                    detail = detail,
                    score = BigDecimal.ZERO,
                    confidence = BigDecimal.ZERO,
                    quoteSizeUsd = decision.buy.quoteSizeUsd,
                    baseSize = decision.sell.baseSize,
                    fundingProductId = decision.sell.productId,
                    fundingBaseSize = decision.sell.baseSize,
                )
                is TradingDecision.Skip -> strategySkip(productId = "UNKNOWN", detail = detail)
            }
        }

        fun strategySkip(productId: String, detail: String): PlanActionReport = PlanActionReport(
            action = "SKIP",
            productId = productId,
            status = "SKIPPED",
            detail = detail,
            score = BigDecimal.ZERO,
            confidence = BigDecimal.ZERO,
            quoteSizeUsd = BigDecimal.ZERO,
            baseSize = BigDecimal.ZERO,
            fundingProductId = "",
            fundingBaseSize = BigDecimal.ZERO,
        )

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
    val change24hPercent: BigDecimal,
    val marketRegime: String,
) {
    fun asMap(): Map<String, Any?> = mapOf(
        "productId" to productId,
        "cryptoValueUsd" to cryptoValueUsd.toPlainString(),
        "allocationPercent" to allocationPercent.toPlainString(),
        "unrealizedPnlUsd" to unrealizedPnlUsd.toPlainString(),
        "unrealizedPnlPercent" to unrealizedPnlPercent.toPlainString(),
        "change24hPercent" to change24hPercent.toPlainString(),
        "marketRegime" to marketRegime,
    )
}

data class PortfolioRunSummary(
    val totalValueUsd: BigDecimal,
    val cashUsd: BigDecimal,
    val cryptoValueUsd: BigDecimal,
    val realizedPnlUsd: BigDecimal,
    val unrealizedPnlUsd: BigDecimal,
    val buyUsd: BigDecimal,
    val sellUsd: BigDecimal,
    val heldPositionCount: Int,
) {
    fun asMap(): Map<String, Any?> = mapOf(
        "totalValueUsd" to totalValueUsd.toPlainString(),
        "cashUsd" to cashUsd.toPlainString(),
        "cryptoValueUsd" to cryptoValueUsd.toPlainString(),
        "realizedPnlUsd" to realizedPnlUsd.toPlainString(),
        "unrealizedPnlUsd" to unrealizedPnlUsd.toPlainString(),
        "buyUsd" to buyUsd.toPlainString(),
        "sellUsd" to sellUsd.toPlainString(),
        "heldPositionCount" to heldPositionCount,
    )
}

data class PortfolioLoserReport(
    val productId: String,
    val cryptoValueUsd: BigDecimal,
    val unrealizedPnlUsd: BigDecimal,
    val unrealizedPnlPercent: BigDecimal,
    val drawdownFromHighPercent: BigDecimal,
    val marketRegime: String,
) {
    fun asMap(): Map<String, Any?> = mapOf(
        "productId" to productId,
        "cryptoValueUsd" to cryptoValueUsd.toPlainString(),
        "unrealizedPnlUsd" to unrealizedPnlUsd.toPlainString(),
        "unrealizedPnlPercent" to unrealizedPnlPercent.toPlainString(),
        "drawdownFromHighPercent" to drawdownFromHighPercent.toPlainString(),
        "marketRegime" to marketRegime,
    )
}

data class SkipReasonCount(
    val reason: String,
    val count: Int,
) {
    fun asMap(): Map<String, Any?> = mapOf(
        "reason" to reason,
        "count" to count,
    )
}

data class Baseline24hReport(
    val name: String,
    val change24hPercent: BigDecimal,
) {
    fun asMap(): Map<String, Any?> = mapOf(
        "name" to name,
        "change24hPercent" to change24hPercent.toPlainString(),
    )
}
