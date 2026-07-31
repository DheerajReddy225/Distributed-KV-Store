# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
# Download dependencies first (cached layer unless pom changes)
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn package -DskipTests -q

# ── Stage 2: Run ─────────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/distributed-kv-store-1.0.0.jar app.jar

# WAL logs live on a Docker volume for persistence across restarts
VOLUME ["/data"]

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
