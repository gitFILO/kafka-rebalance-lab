ARG KAFKA_VERSION=4.1.2
FROM apache/kafka:${KAFKA_VERSION}
ARG JMX_EXPORTER_VERSION=1.7.0
USER root
RUN mkdir -p /opt/jmx /var/lib/kafka-data && chown appuser:appuser /var/lib/kafka-data
ADD --chmod=644 https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar /opt/jmx/agent.jar
USER appuser
