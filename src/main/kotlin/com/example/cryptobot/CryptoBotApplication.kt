package com.example.cryptobot

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@ConfigurationPropertiesScan
@SpringBootApplication
class CryptoBotApplication

fun main(args: Array<String>) {
    runApplication<CryptoBotApplication>(*args)
}
