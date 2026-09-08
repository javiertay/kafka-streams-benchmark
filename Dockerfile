FROM maven:3.9.11-eclipse-temurin-25 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src src
RUN mvn -B verify

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /src/target/benchmark.jar /app/benchmark.jar
RUN mkdir -p /app/results
ENV BENCHMARK_RESULTS_DIR=/app/results
EXPOSE 8080
ENTRYPOINT ["java", "-XX:+UseG1GC", "-jar", "/app/benchmark.jar"]
