package com.example.cryptobot.strategy

import com.example.cryptobot.config.BotProperties
import org.springframework.stereotype.Component

@Component
class SimpleDipBuyStrategy(private val props: BotProperties) {
    fun decide(snapshot: MarketSnapshot): TradingDecision {
        if (props.buyQuoteSizeUsd > props.maxSingleBuyUsd) {
            return TradingDecision.Skip("Configured buy size exceeds max single-buy cap")
        }
        if (snapshot.usdAvailable - props.buyQuoteSizeUsd < props.minUsdCashReserve) {
            return TradingDecision.Skip("USD reserve would fall below configured minimum")
        }
        if (snapshot.change24hPercent > props.dipThresholdPercent.negate()) {
            return TradingDecision.Skip("24h move ${snapshot.change24hPercent}% has not crossed dip threshold -${props.dipThresholdPercent}%")
        }
        return TradingDecision.Buy(
            productId = snapshot.productId,
            quoteSizeUsd = props.buyQuoteSizeUsd,
            reason = "24h drop ${snapshot.change24hPercent}% crossed dip threshold -${props.dipThresholdPercent}%"
        )
    }
}
