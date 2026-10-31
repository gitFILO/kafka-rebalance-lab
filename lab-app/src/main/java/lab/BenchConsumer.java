package lab;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.AppInfoParser;

// 쌓여 있는 레코드를 처리 없이 끝까지 읽으며 poll 호출 비용(CPU, 처리량)을 잰다. Stage 3 버그 재현용.
final class BenchConsumer {

    void run() throws Exception {
        String protocol = Env.str("GROUP_PROTOCOL", "consumer");
        int maxPollRecords = Env.integer("MAX_POLL_RECORDS", 500);
        long maxRecords = Env.integer("MAX_RECORDS", 1_000_000);
        long idleStopMs = Env.integer("IDLE_STOP_MS", 3_000);

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.str("BOOTSTRAP_SERVERS", "broker-1:9092"));
        p.put(ConsumerConfig.GROUP_ID_CONFIG, Env.str("GROUP_ID", "bench-" + UUID.randomUUID()));
        p.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol);
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        com.sun.management.OperatingSystemMXBean os =
            (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        long records = 0;
        long polls = 0;
        long emptyPolls = 0;
        long firstAt = 0;
        long lastAt = 0;
        long cpuAtFirst = 0;
        long cpuAtLast = 0;
        Map<String, Long> threadCpuAtFirst = new TreeMap<>();
        Map<String, Long> threadCpuAtLast = new TreeMap<>();
        Map<String, Long> switchesAtFirst = new TreeMap<>();
        Map<String, Long> switchesAtLast = new TreeMap<>();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(Env.str("TOPIC", "bench")));
            long deadline = System.currentTimeMillis() + 120_000;
            while (records < maxRecords) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofMillis(100));
                polls++;
                long now = System.nanoTime();
                if (batch.isEmpty()) {
                    emptyPolls++;
                    if (firstAt == 0 && System.currentTimeMillis() > deadline) {
                        throw new IllegalStateException("no records within 120s");
                    }
                    if (firstAt != 0 && (now - lastAt) / 1_000_000 > idleStopMs) {
                        break;
                    }
                    continue;
                }
                if (firstAt == 0) {
                    firstAt = now;
                    cpuAtFirst = os.getProcessCpuTime();
                    threadCpuAtFirst = cpuByThread(threads);
                    switchesAtFirst = contextSwitchesByThread();
                    polls = 1;
                    emptyPolls = 0;
                }
                records += batch.count();
                lastAt = now;
                // CPU 시간 조회도 시스템 콜이다. 매 poll마다 부르면 측정 대상보다 커지므로 가끔만 읽는다.
                if ((polls & 1023) == 0) {
                    cpuAtLast = os.getProcessCpuTime();
                }
            }
            if (records >= maxRecords) {
                cpuAtLast = os.getProcessCpuTime();
            }
            threadCpuAtLast = cpuByThread(threads);
            switchesAtLast = contextSwitchesByThread();
        }

        double elapsedMs = (lastAt - firstAt) / 1e6;
        double cpuMs = (cpuAtLast - cpuAtFirst) / 1e6;
        StringBuilder threadJson = new StringBuilder();
        for (Map.Entry<String, Long> e : threadCpuAtLast.entrySet()) {
            long base = threadCpuAtFirst.getOrDefault(e.getKey(), 0L);
            if (threadJson.length() > 0) {
                threadJson.append(',');
            }
            threadJson.append(String.format("\"%s\":%.1f", e.getKey(), (e.getValue() - base) / 1e6));
        }
        StringBuilder switchJson = new StringBuilder();
        for (Map.Entry<String, Long> e : switchesAtLast.entrySet()) {
            long delta = e.getValue() - switchesAtFirst.getOrDefault(e.getKey(), 0L);
            if (delta <= 0) {
                continue;
            }
            if (switchJson.length() > 0) {
                switchJson.append(',');
            }
            switchJson.append(String.format("\"%s\":%.3f", e.getKey(), (double) delta / polls));
        }
        System.out.printf(
            "BENCH {\"client\":\"%s\",\"protocol\":\"%s\",\"maxPollRecords\":%d,\"records\":%d,\"polls\":%d,"
                + "\"emptyPolls\":%d,\"elapsedMs\":%.1f,\"recordsPerSec\":%.1f,\"cpuMs\":%.1f,"
                + "\"cpuMsPer10kRecords\":%.2f,\"cpuCores\":%.3f,\"threadCpuMs\":{%s},\"switchesPerPoll\":{%s}}%n",
            AppInfoParser.getVersion(), protocol, maxPollRecords, records, polls, emptyPolls, elapsedMs,
            records / (elapsedMs / 1000), cpuMs, cpuMs / records * 10_000, cpuMs / elapsedMs, threadJson, switchJson);
    }

    // 스레드가 잠들었다 깨어난 횟수. 스레드 간 대기 왕복이 poll마다 몇 번 일어나는지 보여 준다(리눅스 전용).
    private static Map<String, Long> contextSwitchesByThread() {
        Map<String, Long> out = new TreeMap<>();
        try (DirectoryStream<Path> tasks = Files.newDirectoryStream(Path.of("/proc/self/task"))) {
            for (Path task : tasks) {
                try {
                    String name = Files.readString(task.resolve("comm")).trim().replaceAll("-?\\d+$", "");
                    for (String line : Files.readAllLines(task.resolve("status"))) {
                        if (line.startsWith("voluntary_ctxt_switches") || line.startsWith("nonvoluntary_ctxt_switches")) {
                            out.merge(name, Long.parseLong(line.split(":")[1].trim()), Long::sum);
                        }
                    }
                } catch (IOException ignored) {
                    // 측정 중에 끝난 스레드다.
                }
            }
        } catch (IOException e) {
            return out;
        }
        return out;
    }

    // 스레드 이름에서 번호를 떼어 역할별로 합친다(예: kafka-producer-network-thread | x → 같은 키).
    private static Map<String, Long> cpuByThread(ThreadMXBean threads) {
        Map<String, Long> out = new TreeMap<>();
        for (ThreadInfo info : threads.getThreadInfo(threads.getAllThreadIds())) {
            if (info == null) {
                continue;
            }
            long cpu = threads.getThreadCpuTime(info.getThreadId());
            if (cpu < 0) {
                continue;
            }
            String name = info.getThreadName().split(" \\| ")[0].replaceAll("-?\\d+$", "");
            out.merge(name, cpu, Long::sum);
        }
        return out;
    }
}
