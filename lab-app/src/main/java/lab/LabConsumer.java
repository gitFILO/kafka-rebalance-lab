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
import org.apache.kafka.common.utils.AppInfoParser;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

final class LabConsumer {

    private static final String COORDINATOR_METRICS = "consumer-coordinator-metrics";

    private final String member = Env.str("MEMBER_NAME", Env.str("HOSTNAME", "consumer"));
    private final Map<TopicPartition, OffsetAndMetadata> pending = new HashMap<>();
    private final Map<Integer, Long> lastOffset = new HashMap<>();
    private final LabMetrics stats = LabMetrics.of("type=consumer");
    private final String poisonPolicy = Env.str("POISON_POLICY", "retry");
    // 새 버전의 결함을 흉내 내므로 이 파일은 같은 kafka-clients 버전의 멤버에게만 걸린다.
    private final String regressionFile = "regression." + AppInfoParser.getVersion();
    private volatile boolean running = true;
    private long processMs;
    private long regressionMs;
    private Control ctl;

    private KafkaConsumer<String, String> consumer;
    private BufferedWriter processed;
    private BufferedWriter events;
    private BufferedWriter metrics;
    private Path controlDir;

    void run() throws Exception {
        Path runDir = Env.runDir();
        controlDir = runDir.resolve("control");
        ctl = new Control(controlDir);
        for (String counter : List.of("ProcessedTotal", "CommitFailedTotal", "ErrorTotal", "PoisonFailTotal",
                "PoisonSkippedTotal", "SkippedOffsetsTotal", "RevokedTotal", "AssignedTotal", "LostTotal", "Stalled")) {
            stats.set(counter, 0);
        }
        processed = Env.appender(runDir.resolve("processed-" + member + ".csv"), "ts,partition,offset,produce_ts");
        events = Env.appender(runDir.resolve("events-" + member + ".csv"), "ts,member,event,partitions,detail");
        metrics = Env.appender(runDir.resolve("metrics-" + member + ".csv"), "ts,member,name,value");

        String topic = Env.str("TOPIC", "lab-events");
        String protocol = readControl(member + ".protocol").orElse(Env.str("GROUP_PROTOCOL", "classic"));
        boolean staticMember = Env.bool("STATIC_MEMBERSHIP", false);
        processMs = Env.integer("PROCESS_MS", 2);
        Properties props = consumerProps(protocol, staticMember);

        event("START", List.of(), "protocol=" + protocol
            + " assignor=" + ("classic".equals(protocol) ? Env.str("ASSIGNOR", "range") : Env.str("REMOTE_ASSIGNOR", "broker-default"))
            + " static=" + staticMember
            + " max.poll.interval.ms=" + props.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)
            + " poison.policy=" + poisonPolicy);

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
        long nextControl = 0;
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                long now = System.currentTimeMillis();
                if (now >= nextControl) {
                    processMs = ctl.readLong(member, "process_ms", Env.integer("PROCESS_MS", 2));
                    regressionMs = ctl.readLong(member, regressionFile, 0);
                    stats.set("ProcessMs", processMs);
                    nextControl = now + 1_000;
                }
                if (!records.isEmpty()) {
                    maybeStall();
                    process(records);
                    commitAsync();
                }
                stats.set("AssignedPartitions", consumer.assignment().size());
                if (now >= nextMetricsDump) {
                    dumpMetrics(now);
                    nextMetricsDump = now + 5_000;
                }
            }
        } catch (WakeupException e) {
            // 종료 신호로 poll이 깨어난 경우
        } catch (Exception e) {
            stats.add("ErrorTotal", 1);
            event("ERROR", List.of(), e.getClass().getSimpleName() + ": " + e.getMessage());
            throw e;
        } finally {
            shutdown();
        }
    }

    private void process(ConsumerRecords<String, String> records) throws IOException, InterruptedException {
        Set<TopicPartition> blocked = new HashSet<>();
        for (ConsumerRecord<String, String> record : records) {
            // 배치가 길면 종료 훅의 대기(15초)가 먼저 끝나 close와 flush 없이 내려간다.
            if (!running) {
                break;
            }
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            if (blocked.contains(tp)) {
                continue;
            }
            if (record.value() != null && record.value().startsWith("POISON")) {
                if (!handlePoison(record, tp)) {
                    blocked.add(tp);
                    continue;
                }
            } else if (processMs + regressionMs > 0) {
                Thread.sleep(processMs + regressionMs);
            }
            Long last = lastOffset.put(record.partition(), record.offset());
            // 트랜잭션 마커는 오프셋을 1씩 건너뛰므로 큰 점프만 셈한다.
            if (last != null && record.offset() - last > 100) {
                stats.add("SkippedOffsetsTotal", record.offset() - last - 1);
                event("OFFSET_JUMP", List.of(tp), "from=" + last + " to=" + record.offset());
            }
            processed.write(System.currentTimeMillis() + "," + record.partition() + ","
                + record.offset() + "," + record.timestamp());
            processed.newLine();
            pending.put(tp, new OffsetAndMetadata(record.offset() + 1));
            stats.add("ProcessedTotal", 1);
            LabMetrics.of("type=consumer-partition,partition=" + record.partition()).add("ProcessedTotal", 1);
        }
    }

    // true면 레코드를 처리한 것으로 보고 진행한다.
    private boolean handlePoison(ConsumerRecord<String, String> record, TopicPartition tp) throws InterruptedException {
        switch (poisonPolicy) {
            case "skip" -> {
                stats.add("PoisonSkippedTotal", 1);
                event("POISON_SKIPPED", List.of(tp), "offset=" + record.offset());
                return true;
            }
            case "inline" -> {
                int retries = Env.integer("POISON_RETRIES", 10);
                long backoff = Env.integer("POISON_BACKOFF_MS", 5000);
                for (int i = 1; i <= retries && running; i++) {
                    stats.add("PoisonFailTotal", 1);
                    event("POISON_FAIL", List.of(tp), "offset=" + record.offset() + " attempt=" + i);
                    Thread.sleep(backoff);
                }
                throw new IllegalStateException("poison record " + tp + "@" + record.offset() + " retries exhausted");
            }
            default -> {
                stats.add("PoisonFailTotal", 1);
                event("POISON_FAIL", List.of(tp), "offset=" + record.offset());
                consumer.seek(tp, record.offset());
                Thread.sleep(Env.integer("POISON_BACKOFF_MS", 1000));
                return false;
            }
        }
    }

    private Properties consumerProps(String protocol, boolean staticMember) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.required("BOOTSTRAP_SERVERS"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Env.required("GROUP_ID"));
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, member);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Env.str("AUTO_OFFSET_RESET", "latest"));
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, Env.str("ISOLATION_LEVEL", "read_uncommitted"));
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
        stats.set("Stalled", 1);
        Thread.sleep(stallMs);
        stats.set("Stalled", 0);
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
                stats.add("CommitFailedTotal", 1);
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
            stats.add("CommitFailedTotal", 1);
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
            stats.add("RevokedTotal", 1);
            event("REVOKED", partitions, "");
            commitSync(partitions, "revoke");
            partitions.forEach(tp -> lastOffset.remove(tp.partition()));
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            stats.add("AssignedTotal", 1);
            event("ASSIGNED", partitions, "owned=" + consumer.assignment().size());
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            stats.add("LostTotal", 1);
            event("LOST", partitions, "");
            partitions.forEach(pending::remove);
            partitions.forEach(tp -> lastOffset.remove(tp.partition()));
        }
    }
}
