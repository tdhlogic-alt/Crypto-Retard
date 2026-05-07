plugins {
    kotlin("jvm") version "2.1.10"
    kotlin("plugin.spring") version "2.1.10"
    id("org.springframework.boot") version "3.3.7"
    id("io.spring.dependency-management") version "1.1.6"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"

java { toolchain { languageVersion.set(JavaLanguageVersion.of(26)) } }



dependencies {
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("com.nimbusds:nimbus-jose-jwt:9.39")
    implementation("org.bouncycastle:bcpkix-jdk18on:1.78")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.projectreactor:reactor-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> { useJUnitPlatform() }

// Ensure Kotlin compiler targets a supported JVM and align Java bytecode
tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions.jvmTarget = "21"
}

tasks.withType<org.gradle.api.tasks.compile.JavaCompile> {
    // Produce bytecode compatible with Java 21 so Kotlin and Java align
    options.release.set(21)
}
