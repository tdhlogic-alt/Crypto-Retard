package com.example.cryptobot.coinbase

import com.example.cryptobot.config.CoinbaseProperties
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RequestNotPermitted
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.reactor.ratelimiter.operator.RateLimiterOperator
import io.netty.channel.ChannelOption
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientRequestException
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import reactor.util.retry.Retry
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeoutException

@Component
class CoinbaseClient(
    private val props: CoinbaseProperties,
    private val signer: CoinbaseJwtSigner,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val productCache = ConcurrentHashMap<String, CachedProduct>()

    private val coinbaseRateLimiter: RateLimiter = RateLimiter.of(
        "coinbaseApi",
        RateLimiterConfig.custom()
            .limitForPeriod(props.rateLimitForPeriod)
            .limitRefreshPeriod(props.rateLimitRefreshPeriod)
            .timeoutDuration(props.rateLimitTimeout)
            .build()
    )

    private val httpClient = HttpClient.create()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10_000)
        .responseTimeout(Duration.ofSeconds(30))

    private val webClient = WebClient.builder()
        .baseUrl(props.baseUrl)
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .build()

    fun getProduct(productId: String): Mono<ProductResponse> {
        val cached = productCache[productId]
        val now = Instant.now()
        if (cached != null && now.isBefore(cached.expiresAt)) {
            log.debug("Using cached Coinbase product {}", productId)
            return Mono.just(cached.product)
        }

        val path = "/api/v3/brokerage/products/$productId"

        return coinbaseGet(path, ProductResponse::class.java, "getProduct")
            .doOnSubscribe { log.info("Fetching Coinbase product {}", productId) }
            .doOnSuccess { product ->
                productCache[productId] = CachedProduct(
                    product = product,
                    expiresAt = Instant.now().plus(props.productCacheTtl)
                )
                log.info("Fetched Coinbase product {}", productId)
            }
    }

    fun listAccounts(): Mono<AccountsResponse> {
        val path = "/api/v3/brokerage/accounts"

        return coinbaseGet(path, AccountsResponse::class.java, "listAccounts")
            .doOnSubscribe { log.info("Fetching Coinbase accounts") }
            .doOnSuccess { log.info("Fetched Coinbase accounts") }
    }

    fun createMarketBuy(productId: String, quoteSizeUsd: BigDecimal): Mono<CreateOrderResponse> {
        val path = "/api/v3/brokerage/orders"
        val request = CreateOrderRequest.marketBuy(productId, quoteSizeUsd)

        return coinbasePost(path, request, CreateOrderResponse::class.java, "createMarketBuy")
            .doOnSubscribe { log.warn("Submitting Coinbase market buy: product={} quoteSizeUsd={}", productId, quoteSizeUsd) }
    }

    fun createMarketSell(productId: String, baseSize: BigDecimal): Mono<CreateOrderResponse> {
        val path = "/api/v3/brokerage/orders"
        val request = CreateOrderRequest.marketSell(productId, baseSize)

        return coinbasePost(path, request, CreateOrderResponse::class.java, "createMarketSell")
            .doOnSubscribe { log.warn("Submitting Coinbase market sell: product={} baseSize={}", productId, baseSize) }
    }

    fun getCandles(
        productId: String,
        granularity: String,
        start: Instant,
        end: Instant,
    ): Mono<CandlesResponse> {
        val path = "/api/v3/brokerage/products/$productId/candles"

        return Mono.defer {
            webClient.get()
                .uri { builder ->
                    builder
                        .path(path)
                        .queryParam("granularity", granularity)
                        .queryParam("start", start.epochSecond)
                        .queryParam("end", end.epochSecond)
                        .build()
                }
                .headers { it.setBearerAuth(signer.sign("GET", path)) }
                .retrieve()
                .onStatus(HttpStatusCode::isError) { response -> coinbaseError("getCandles", response) }
                .bodyToMono(CandlesResponse::class.java)
        }
            .withCoinbasePolicies("getCandles")
            .doOnSubscribe { log.info("Fetching Coinbase candles product={} granularity={}", productId, granularity) }
    }

    private fun <T : Any> coinbaseGet(
        path: String,
        responseType: Class<T>,
        operation: String,
    ): Mono<T> = Mono.defer {
        webClient.get()
            .uri(path)
            .headers { it.setBearerAuth(signer.sign("GET", path)) }
            .retrieve()
            .onStatus(HttpStatusCode::isError) { response -> coinbaseError(operation, response) }
            .bodyToMono(responseType)
    }.withCoinbasePolicies(operation)

    private fun <T : Any> coinbasePost(
        path: String,
        request: Any,
        responseType: Class<T>,
        operation: String,
    ): Mono<T> = Mono.defer {
        webClient.post()
            .uri(path)
            .contentType(MediaType.APPLICATION_JSON)
            .headers { it.setBearerAuth(signer.sign("POST", path)) }
            .bodyValue(request)
            .retrieve()
            .onStatus(HttpStatusCode::isError) { response -> coinbaseError(operation, response) }
            .bodyToMono(responseType)
    }.withCoinbasePolicies(operation)

    private fun coinbaseError(operation: String, response: ClientResponse): Mono<Throwable> =
        response.bodyToMono(String::class.java)
            .defaultIfEmpty("")
            .map<Throwable> { body -> CoinbaseApiException(operation, response.statusCode(), body) }

    private fun <T> Mono<T>.withCoinbasePolicies(operation: String): Mono<T> =
        this.transformDeferred(RateLimiterOperator.of(coinbaseRateLimiter))
            .retryWhen(
                Retry.backoff(props.retryMaxAttempts, props.retryInitialBackoff)
                    .maxBackoff(props.retryMaxBackoff)
                    .jitter(0.35)
                    .filter(::isRetryableCoinbaseFailure)
                    .doBeforeRetry { signal ->
                        log.warn(
                            "Retrying Coinbase {} request. attempt={} error={}",
                            operation,
                            signal.totalRetries() + 1,
                            signal.failure().message
                        )
                    }
            )
            .doOnError { ex -> log.error("Coinbase {} request failed: {}", operation, ex.message, ex) }

    private fun isRetryableCoinbaseFailure(ex: Throwable): Boolean =
        when (ex) {
            is CoinbaseApiException -> ex.statusCode == HttpStatus.TOO_MANY_REQUESTS || ex.statusCode.is5xxServerError
            is WebClientRequestException -> true
            is RequestNotPermitted -> true
            is TimeoutException -> true
            is java.net.ConnectException -> true
            is java.nio.channels.ClosedChannelException -> true
            is javax.net.ssl.SSLException -> true
            else -> false
        }

    private data class CachedProduct(
        val product: ProductResponse,
        val expiresAt: Instant,
    )
}

class CoinbaseApiException(
    val operation: String,
    val statusCode: HttpStatusCode,
    val responseBody: String,
) : RuntimeException("Coinbase $operation failed: status=$statusCode body=$responseBody")
