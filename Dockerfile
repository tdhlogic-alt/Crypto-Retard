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
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/app.jar"]
