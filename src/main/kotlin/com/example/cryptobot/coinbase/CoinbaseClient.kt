package com.example.cryptobot.coinbase

import com.example.cryptobot.config.CoinbaseProperties
import io.netty.channel.ChannelOption
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatusCode
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import java.math.BigDecimal
import java.time.Duration

@Component
class CoinbaseClient(
    props: CoinbaseProperties,
    private val signer: CoinbaseJwtSigner,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val httpClient = HttpClient.create()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10_000)
        .responseTimeout(Duration.ofSeconds(30))

    private val webClient = WebClient.builder()
        .baseUrl(props.baseUrl)
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .build()

    fun getProduct(productId: String): Mono<ProductResponse> {
        val path = "/api/v3/brokerage/products/$productId"

        return webClient.get()
            .uri(path)
            .headers { it.setBearerAuth(signer.sign("GET", path)) }
            .retrieve()
            .onStatus(HttpStatusCode::isError) { response ->
                response.bodyToMono(String::class.java)
                    .defaultIfEmpty("")
                    .flatMap { body ->
                        Mono.error(RuntimeException("Coinbase getProduct failed: status=${response.statusCode()} body=$body"))
                    }
            }
            .bodyToMono(ProductResponse::class.java)
            .doOnSubscribe { log.info("Fetching Coinbase product {}", productId) }
            .doOnSuccess { log.info("Fetched Coinbase product {}", productId) }
    }

    fun listAccounts(): Mono<AccountsResponse> {
        val path = "/api/v3/brokerage/accounts"

        return webClient.get()
            .uri(path)
            .headers { it.setBearerAuth(signer.sign("GET", path)) }
            .retrieve()
            .onStatus(HttpStatusCode::isError) { response ->
                response.bodyToMono(String::class.java)
                    .defaultIfEmpty("")
                    .flatMap { body ->
                        Mono.error(RuntimeException("Coinbase listAccounts failed: status=${response.statusCode()} body=$body"))
                    }
            }
            .bodyToMono(AccountsResponse::class.java)
            .doOnSubscribe { log.info("Fetching Coinbase accounts") }
            .doOnSuccess { log.info("Fetched Coinbase accounts") }
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
            .onStatus(HttpStatusCode::isError) { response ->
                response.bodyToMono(String::class.java)
                    .defaultIfEmpty("")
                    .flatMap { body ->
                        Mono.error(RuntimeException("Coinbase createMarketBuy failed: status=${response.statusCode()} body=$body"))
                    }
            }
            .bodyToMono(CreateOrderResponse::class.java)
            .doOnSubscribe { log.warn("Submitting Coinbase market buy: product={} quoteSizeUsd={}", productId, quoteSizeUsd) }
    }
}