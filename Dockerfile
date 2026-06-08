# Runtime image only.
# GitHub Actions builds build/libs/app.jar before docker build, so the Docker build
# does not need to download Gradle or rebuild the app inside the container.
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY build/libs/app.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
