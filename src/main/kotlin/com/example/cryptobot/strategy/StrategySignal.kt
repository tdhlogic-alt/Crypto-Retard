package com.example.cryptobot.strategy

/**
 * A strategy-produced trade candidate.
 *
 * Hard exits are deterministic risk controls and must not wait for AI approval.
 * Entry/rotation candidates may be sent to the AI only as a veto/reduce-risk layer.
 */
data class StrategySignal(
    val decision: TradingDecision,
    val strategyName: String,
    val priority: Int,
    val hardExit: Boolean = false,
    val requiresAiApproval: Boolean = true,
    val rationale: String,
)
