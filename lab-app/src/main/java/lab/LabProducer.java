package lab;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

final class LabProducer {

    private volatile boolean running = true;

    void run() throws Exception {
        String topic = Env.str("TOPIC", "lab-events");
        int rate = Env.integer("RATE", 600);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.required("BOOTSTRAP_SERVERS"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "lab-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            try {
                main.join(10_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));

        AtomicLong acked = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int partitions = producer.partitionsFor(topic).size();
            System.out.printf("producer topic=%s partitions=%d rate=%d/s%n", topic, partitions, rate);

            long start = System.currentTimeMillis();
            long lastReport = start;
            long seq = 0;
            while (running) {
                long now = System.currentTimeMillis();
                long target = (now - start) * rate / 1000;
                while (seq < target && running) {
                    // 파티션을 순환 지정해 파티션별 유입 속도를 균일하게 만든다.
                    int partition = (int) (seq % partitions);
                    producer.send(new ProducerRecord<>(topic, partition, null, Long.toString(seq)), (metadata, e) -> {
                        if (e == null) {
                            acked.incrementAndGet();
                        } else {
                            failed.incrementAndGet();
                        }
                    });
                    seq++;
                }
                if (now - lastReport >= 10_000) {
                    System.out.printf("producer sent=%d acked=%d failed=%d%n", seq, acked.get(), failed.get());
                    lastReport = now;
                }
                Thread.sleep(2);
            }
            producer.flush();
            System.out.printf("producer stopped sent=%d acked=%d failed=%d%n", seq, acked.get(), failed.get());
        }
    }
}
