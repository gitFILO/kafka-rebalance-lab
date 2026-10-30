package lab;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

final class LabConsumer {

    private static final String COORDINATOR_METRICS = "consumer-coordinator-metrics";

    private final String member = Env.str("MEMBER_NAME", Env.str("HOSTNAME", "consumer"));
    private final Map<TopicPartition, OffsetAndMetadata> pending = new HashMap<>();
    private volatile boolean running = true;

    private KafkaConsumer<String, String> consumer;
    private BufferedWriter processed;
    private BufferedWriter events;
    private BufferedWriter metrics;
    private Path controlDir;

    void run() throws Exception {
        Path runDir = Env.runDir();
        controlDir = runDir.resolve("control");
        processed = Env.appender(runDir.resolve("processed-" + member + ".csv"), "ts,partition,offset,produce_ts");
        events = Env.appender(runDir.resolve("events-" + member + ".csv"), "ts,member,event,partitions,detail");
        metrics = Env.appender(runDir.resolve("metrics-" + member + ".csv"), "ts,member,name,value");

        String topic = Env.str("TOPIC", "lab-events");
        String protocol = readControl(member + ".protocol").orElse(Env.str("GROUP_PROTOCOL", "classic"));
        boolean staticMember = Env.bool("STATIC_MEMBERSHIP", false);
        int processMs = Env.integer("PROCESS_MS", 2);
        Properties props = consumerProps(protocol, staticMember);

        event("START", List.of(), "protocol=" + protocol
            + " assignor=" + ("classic".equals(protocol) ? Env.str("ASSIGNOR", "range") : Env.str("REMOTE_ASSIGNOR", "broker-default"))
            + " static=" + staticMember
            + " max.poll.interval.ms=" + props.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));

        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            if (consumer != null) {
                consumer.wakeup();
            }
            try {
                main.join(15_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic), new Listener());
        long nextMetricsDump = 0;
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    maybeStall();
                    for (ConsumerRecord<String, String> record : records) {
                        if (processMs > 0) {
                            Thread.sleep(processMs);
                        }
                        processed.write(System.currentTimeMillis() + "," + record.partition() + ","
                            + record.offset() + "," + record.timestamp());
                        processed.newLine();
                        pending.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                    }
                    commitAsync();
                }
                long now = System.currentTimeMillis();
                if (now >= nextMetricsDump) {
                    dumpMetrics(now);
                    nextMetricsDump = now + 5_000;
                }
            }
        } catch (WakeupException e) {
            // 종료 신호로 poll이 깨어난 경우
        } catch (Exception e) {
            event("ERROR", List.of(), e.getClass().getSimpleName() + ": " + e.getMessage());
            throw e;
        } finally {
            shutdown();
        }
    }

    private Properties consumerProps(String protocol, boolean staticMember) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.required("BOOTSTRAP_SERVERS"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Env.required("GROUP_ID"));
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, member);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Env.integer("MAX_POLL_INTERVAL_MS", 30_000));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Env.integer("MAX_POLL_RECORDS", 100));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        if (staticMember) {
            props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, member);
        }
        // consumer 프로토콜에서 아래 classic 전용 설정을 넣으면 ConfigException이 발생한다.
        if ("classic".equals(protocol)) {
            props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignorClass(Env.str("ASSIGNOR", "range")));
            String sessionTimeout = Env.str("SESSION_TIMEOUT_MS", null);
            if (sessionTimeout != null) {
                props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
            }
        } else {
            String remoteAssignor = Env.str("REMOTE_ASSIGNOR", null);
            if (remoteAssignor != null) {
                props.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, remoteAssignor);
            }
        }
        return props;
    }

    private static String assignorClass(String name) {
        return switch (name) {
            case "range" -> RangeAssignor.class.getName();
            case "roundrobin" -> RoundRobinAssignor.class.getName();
            case "sticky" -> StickyAssignor.class.getName();
            case "cooperative-sticky" -> CooperativeStickyAssignor.class.getName();
            default -> throw new IllegalArgumentException("unknown ASSIGNOR " + name);
        };
    }

    private void maybeStall() throws IOException, InterruptedException {
        Optional<String> stall = readControl(member + ".stall");
        if (stall.isEmpty()) {
            return;
        }
        Files.deleteIfExists(controlDir.resolve(member + ".stall"));
        long stallMs = Long.parseLong(stall.get());
        event("STALL_BEGIN", consumer.assignment(), "ms=" + stallMs);
        Thread.sleep(stallMs);
        event("STALL_END", consumer.assignment(), "");
    }

    private void commitAsync() throws IOException {
        if (pending.isEmpty()) {
            return;
        }
        // 처리 기록을 먼저 내려써야 커밋된 오프셋보다 처리 기록이 뒤처지지 않는다.
        processed.flush();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(pending);
        pending.clear();
        // consumer 프로토콜(AsyncKafkaConsumer)은 실패 시 콜백의 offsets 인자를 null로 넘긴다.
        consumer.commitAsync(offsets, (ignored, e) -> {
            if (e != null) {
                event("COMMIT_FAILED", offsets.keySet(), e.getClass().getSimpleName());
            }
        });
    }

    private void commitSync(Collection<TopicPartition> partitions, String reason) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition tp : partitions) {
            OffsetAndMetadata offset = pending.remove(tp);
            if (offset != null) {
                offsets.put(tp, offset);
            }
        }
        if (offsets.isEmpty()) {
            return;
        }
        try {
            processed.flush();
            consumer.commitSync(offsets, Duration.ofSeconds(5));
        } catch (Exception e) {
            event("COMMIT_FAILED", offsets.keySet(), reason + " " + e.getClass().getSimpleName());
        }
    }

    private void shutdown() {
        try {
            if (consumer != null) {
                commitSync(List.copyOf(pending.keySet()), "close");
                event("CLOSE", consumer.assignment(), "");
                consumer.close(CloseOptions.timeout(Duration.ofSeconds(10)));
            }
        } finally {
            try {
                processed.flush();
                metrics.flush();
                events.flush();
            } catch (IOException ignored) {
                // 종료 경로의 flush 실패는 무시한다.
            }
        }
    }

    private void dumpMetrics(long now) throws IOException {
        for (Map.Entry<MetricName, ? extends Metric> entry : consumer.metrics().entrySet()) {
            MetricName name = entry.getKey();
            if (!COORDINATOR_METRICS.equals(name.group())) {
                continue;
            }
            Object value = entry.getValue().metricValue();
            if (value instanceof Number number) {
                metrics.write(now + "," + member + "," + name.name() + "," + number.doubleValue());
                metrics.newLine();
            }
        }
        metrics.flush();
    }

    private Optional<String> readControl(String fileName) throws IOException {
        Path file = controlDir.resolve(fileName);
        if (Files.notExists(file)) {
            return Optional.empty();
        }
        String value = Files.readString(file, StandardCharsets.UTF_8).trim();
        return value.isEmpty() ? Optional.empty() : Optional.of(value);
    }

    private synchronized void event(String type, Collection<TopicPartition> partitions, String detail) {
        String parts = partitions.stream()
            .map(TopicPartition::partition)
            .sorted()
            .map(String::valueOf)
            .collect(Collectors.joining(";"));
        try {
            events.write(System.currentTimeMillis() + "," + member + "," + type + "," + parts + ","
                + detail.replace(',', ' '));
            events.newLine();
            events.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        System.out.printf("[%s] %s partitions=[%s] %s%n", member, type, parts, detail);
    }

    private final class Listener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            event("REVOKED", partitions, "");
            commitSync(partitions, "revoke");
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            event("ASSIGNED", partitions, "owned=" + consumer.assignment().size());
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            event("LOST", partitions, "");
            partitions.forEach(pending::remove);
        }
    }
}
