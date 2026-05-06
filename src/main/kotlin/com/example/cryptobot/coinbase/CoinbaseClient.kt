package com.example.cryptobot.coinbase

import com.example.cryptobot.config.CoinbaseProperties
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.math.BigDecimal

@Component
class CoinbaseClient(
    props: CoinbaseProperties,
    private val signer: CoinbaseJwtSigner,
) {
    private val webClient = WebClient.builder()
        .baseUrl(props.baseUrl)
        .build()

    fun getProduct(productId: String): Mono<ProductResponse> {
        val path = "/api/v3/brokerage/products/$productId"
        return webClient.get()
            .uri(path)
            .headers { it.setBearerAuth(signer.sign("GET", path)) }
            .retrieve()
            .bodyToMono(ProductResponse::class.java)
    }

    fun listAccounts(): Mono<AccountsResponse> {
        val path = "/api/v3/brokerage/accounts"
        return webClient.get()
            .uri(path)
            .headers { it.setBearerAuth(signer.sign("GET", path)) }
            .retrieve()
            .bodyToMono(AccountsResponse::class.java)
    }

    fun createMarketBuy(productId: String, quoteSizeUsd: BigDecimal): Mono<CreateOrderResponse> {
        val path = "/api/v3/brokerage/orders"
        val request = CreateOrderRequest.marketBuy(productId, quoteSizeUsd)
        return webClient.post()
            .uri(path)
            .contentType(MediaType.APPLICATION_JSON)
            .headers { it.setBearerAuth(signer.sign("POST", path)) }
            .bodyValue(request)
            .retrieve()
            .bodyToMono(CreateOrderResponse::class.java)
    }
}
