package com.example.cryptobot.alerts

import com.example.cryptobot.config.BotProperties
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono

@Component
class DiscordAlertClient(
    private val props: BotProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val webClient = WebClient.builder().build()

    fun send(message: String): Mono<Void> {
        if (props.discordWebhookUrl.isBlank()) {
            log.debug("Discord webhook not configured; skipping alert")
            return Mono.empty()
        }

        return webClient.post()
            .uri(props.discordWebhookUrl)
            .bodyValue(mapOf("content" to message.take(1900)))
            .retrieve()
            .bodyToMono(Void::class.java)
            .doOnError { ex -> log.warn("Failed to send Discord alert: {}", ex.message) }
            .onErrorResume { Mono.empty() }
    }
}