package lab;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

final class LabProducer {

    private final LabMetrics stats = LabMetrics.of("type=producer");
    private volatile boolean running = true;

    private Control control;
    private int partitions;
    private int rate;
    private int valueBytes;
    private int hotPartition = -1;
    private int hotPercent;
    private long lastLarge;

    void run() throws Exception {
        String topic = Env.str("TOPIC", "lab-events");
        control = new Control(Env.runDir().resolve("control"));
        rate = Env.integer("RATE", 600);
        valueBytes = Env.integer("VALUE_BYTES", 0);
        boolean transactional = Env.bool("TRANSACTIONAL", false);
        for (String counter : new String[] {"SentTotal", "AckedTotal", "FailedTotal", "SkippedTotal",
                "SendBlockedMsTotal", "PoisonSentTotal", "LargeSentTotal"}) {
            stats.set(counter, 0);
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.required("BOOTSTRAP_SERVERS"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "lab-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Env.str("BUFFER_MEMORY", "33554432"));
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Env.str("MAX_REQUEST_SIZE", "1048576"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        if (transactional) {
            // 인스턴스마다 다른 id를 쓰는 잘못된 구성이다. 같은 id로 재기동하면 이전 트랜잭션이 정리되어 D11이 재현되지 않는다.
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "lab-tx-" + UUID.randomUUID().toString().substring(0, 8));
            props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, Env.str("TRANSACTION_TIMEOUT_MS", "60000"));
        }

        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            try {
                main.join(10_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));

        Callback callback = (metadata, e) -> {
            if (e == null) {
                stats.add("AckedTotal", 1);
            } else {
                stats.add("FailedTotal", 1);
                LabMetrics.of("type=producer-error,error=" + e.getClass().getSimpleName()).add("Count", 1);
            }
        };

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            partitions = producer.partitionsFor(topic).size();
            if (transactional) {
                producer.initTransactions();
            }
            System.out.printf("producer topic=%s partitions=%d rate=%d/s transactional=%s%n",
                topic, partitions, rate, transactional);

            long base = System.currentTimeMillis();
            long baseSeq = 0;
            long seq = 0;
            long lastControl = 0;
            long lastReport = base;
            long txStart = 0;
            boolean inTx = false;
            while (running) {
                long now = System.currentTimeMillis();
                if (now - lastControl >= 1_000) {
                    lastControl = now;
                    int oldRate = rate;
                    applyControls();
                    if (rate != oldRate) {
                        base = now;
                        baseSeq = seq;
                    }
                    oneShots(producer, topic, callback, seq);
                }
                long target = baseSeq + (now - base) * rate / 1000;
                // 1초 이상 밀린 분량은 보내지 않는다. 상류가 막혀 받지 못한 요청으로 본다.
                if (target - seq > rate) {
                    stats.add("SkippedTotal", target - seq - rate);
                    seq = target - rate;
                }
                if (transactional && !inTx && seq < target) {
                    producer.beginTransaction();
                    inTx = true;
                    txStart = now;
                }
                while (seq < target && running) {
                    long t0 = System.nanoTime();
                    producer.send(new ProducerRecord<>(topic, partitionFor(seq), null, value(seq)), callback);
                    long blockedMs = (System.nanoTime() - t0) / 1_000_000;
                    if (blockedMs > 0) {
                        stats.add("SendBlockedMsTotal", blockedMs);
                    }
                    stats.add("SentTotal", 1);
                    seq++;
                }
                if (inTx && now - txStart >= 200) {
                    producer.commitTransaction();
                    inTx = false;
                }
                stats.set("TargetRate", rate);
                if (now - lastReport >= 10_000) {
                    System.out.printf("producer sent=%d rate=%d hot=%d:%d%%%n", seq, rate, hotPartition, hotPercent);
                    lastReport = now;
                }
                Thread.sleep(2);
            }
            if (inTx) {
                producer.commitTransaction();
            }
            producer.flush();
            System.out.printf("producer stopped sent=%d%n", seq);
        }
    }

    private void applyControls() {
        rate = (int) control.readLong("producer", "rate", Env.integer("RATE", 600));
        valueBytes = (int) control.readLong("producer", "value_bytes", Env.integer("VALUE_BYTES", 0));
        Optional<String> skew = control.read("producer", "skew");
        if (skew.isPresent()) {
            String[] parts = skew.get().split(":");
            hotPartition = Integer.parseInt(parts[0]);
            hotPercent = Integer.parseInt(parts[1]);
        } else {
            hotPartition = -1;
            hotPercent = 0;
        }
    }

    private void oneShots(KafkaProducer<String, String> producer, String topic, Callback callback, long seq) {
        control.take("producer.poison").ifPresent(p -> {
            producer.send(new ProducerRecord<>(topic, Integer.parseInt(p), null, "POISON|" + seq), callback);
            stats.add("PoisonSentTotal", 1);
        });
        // 형식: "<bytes> <intervalMs>". 상류 서비스가 간헐적으로 큰 페이로드를 보내는 상황이다.
        control.read("producer", "large").ifPresent(spec -> {
            String[] parts = spec.split("\\s+");
            long interval = parts.length > 1 ? Long.parseLong(parts[1]) : 10_000;
            long now = System.currentTimeMillis();
            if (now - lastLarge >= interval) {
                lastLarge = now;
                producer.send(new ProducerRecord<>(topic, (int) (seq % partitions), null,
                    seq + "|" + "x".repeat(Integer.parseInt(parts[0]))), callback);
                stats.add("LargeSentTotal", 1);
            }
        });
    }

    private int partitionFor(long seq) {
        if (hotPartition >= 0 && seq % 100 < hotPercent) {
            return hotPartition;
        }
        return (int) (seq % partitions);
    }

    private String value(long seq) {
        return valueBytes > 0 ? seq + "|" + "x".repeat(valueBytes) : Long.toString(seq);
    }
}
