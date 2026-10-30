FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:17-jre
ARG JMX_EXPORTER_VERSION=1.7.0
WORKDIR /app
ADD --chmod=644 https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar /app/jmx/agent.jar
COPY --from=build /src/target/lab-app.jar /app/lab-app.jar
COPY --from=build /src/target/lib /app/lib
ENTRYPOINT ["java", "-cp", "/app/lab-app.jar:/app/lib/*", "lab.LabApp"]
