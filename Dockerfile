# Multi-stage Dockerfile for Spring Boot bootJar
# Build stage
FROM eclipse-temurin:26-jdk-jammy AS build
WORKDIR /workspace
# Copy project and build the fat jar (skip tests to speed up CI builds)
COPY . .
RUN chmod +x ./gradlew || true
RUN ./gradlew bootJar -x test --no-daemon --console=plain

# Run stage
FROM eclipse-temurin:26-jre-jammy
WORKDIR /app
ARG JAR_FILE=build/libs/*.jar
COPY --from=build /workspace/${JAR_FILE} app.jar

# Health check to detect if app is responsive
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/actuator/health || exit 1

EXPOSE 8080

# Use exec form to ensure signals are properly received for graceful shutdown
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
