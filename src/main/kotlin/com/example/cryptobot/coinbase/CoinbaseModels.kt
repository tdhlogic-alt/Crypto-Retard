package com.example.cryptobot.coinbase

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import java.math.BigDecimal
import java.util.UUID

@JsonIgnoreProperties(ignoreUnknown = true)
data class ProductResponse(
    @JsonProperty("product_id")
    val productId: String? = null,

    val price: String = "0",

    @JsonProperty("price_percentage_change_24h")
    val pricePercentageChange24h: String? = null,

    @JsonProperty("volume_24h")
    val volume24h: String? = null,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class AccountsResponse(
    val accounts: List<CoinbaseAccount> = emptyList(),
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CoinbaseAccount(
    val uuid: String = "",
    val name: String = "",
    val currency: String = "",
    @JsonProperty("available_balance") val availableBalance: Money = Money(),
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class Money(
    val value: String = "0",
    val currency: String = "",
) {
    fun decimal(): BigDecimal = value.toBigDecimalOrNull() ?: BigDecimal.ZERO
}

data class CreateOrderRequest(
    @JsonProperty("client_order_id") val clientOrderId: String,
    @JsonProperty("product_id") val productId: String,
    val side: String,
    @JsonProperty("order_configuration") val orderConfiguration: OrderConfiguration,
) {
    companion object {
        fun marketBuy(productId: String, quoteSizeUsd: BigDecimal) = CreateOrderRequest(
            clientOrderId = UUID.randomUUID().toString(),
            productId = productId,
            side = "BUY",
            orderConfiguration = OrderConfiguration(
                marketMarketIoc = MarketMarketIoc(quoteSize = quoteSizeUsd.toPlainString())            )
        )

        fun marketSell(productId: String, baseSize: BigDecimal) = CreateOrderRequest(
            clientOrderId = UUID.randomUUID().toString(),
            productId = productId,
            side = "SELL",
            orderConfiguration = OrderConfiguration(
                marketMarketIoc = MarketMarketIoc(baseSize = baseSize.toPlainString())
            )
        )
    }
}

data class OrderConfiguration(
    @JsonProperty("market_market_ioc") val marketMarketIoc: MarketMarketIoc,
)

data class MarketMarketIoc(
    @JsonProperty("quote_size") val quoteSize: String? = null,
    @JsonProperty("base_size") val baseSize: String? = null,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CreateOrderResponse(
    val success: Boolean = false,
    @JsonProperty("success_response") val successResponse: Map<String, Any>? = null,
    @JsonProperty("error_response") val errorResponse: Map<String, Any>? = null,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CandlesResponse(
    val candles: List<Candle> = emptyList(),
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class Candle(
    val start: String = "0",
    val low: String = "0",
    val high: String = "0",
    val open: String = "0",
    val close: String = "0",
    val volume: String = "0",
)