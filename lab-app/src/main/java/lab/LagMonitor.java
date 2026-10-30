package lab;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

// 그룹 상태·멤버·커밋 lag을 브로커 관점에서 1초 간격으로 기록한다.
final class LagMonitor {

    private volatile boolean running = true;

    void run() throws Exception {
        Path runDir = Env.runDir();
        String topic = Env.str("TOPIC", "lab-events");
        String groupId = Env.required("GROUP_ID");
        int sampleMs = Env.integer("SAMPLE_MS", 1000);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Env.required("BOOTSTRAP_SERVERS"));
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "lab-lag-monitor");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);

        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            try {
                main.join(5_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));

        try (Admin admin = Admin.create(props);
             BufferedWriter group = Env.appender(runDir.resolve("group.csv"),
                 "ts,type,state,members,group_epoch,target_epoch,assigned_partitions,consumer_protocol_members,classic_protocol_members");
             BufferedWriter members = Env.appender(runDir.resolve("members.csv"),
                 "ts,client_id,instance_id,member_id,member_epoch,upgraded,partitions");
             BufferedWriter lag = Env.appender(runDir.resolve("lag.csv"),
                 "ts,total_lag,max_partition_lag,uncommitted_partitions,per_partition")) {

            List<TopicPartition> partitions = admin.describeTopics(List.of(topic)).allTopicNames().get()
                .get(topic).partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .toList();
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
            partitions.forEach(tp -> latest.put(tp, OffsetSpec.latest()));
            System.out.printf("lag-monitor group=%s partitions=%d%n", groupId, partitions.size());

            long lastPrint = 0;
            while (running) {
                long ts = System.currentTimeMillis();
                String summary;
                try {
                    summary = sampleGroup(admin, groupId, ts, group, members);
                } catch (Exception e) {
                    summary = "group=N/A(" + rootCause(e) + ")";
                    group.write(ts + ",,N/A,0,,,0,0,0");
                    group.newLine();
                    group.flush();
                }
                try {
                    summary += " " + sampleLag(admin, groupId, partitions, latest, ts, lag);
                } catch (Exception e) {
                    summary += " lag=N/A(" + rootCause(e) + ")";
                }
                if (ts - lastPrint >= 5_000) {
                    System.out.println(summary);
                    lastPrint = ts;
                }
                long sleep = sampleMs - (System.currentTimeMillis() - ts);
                if (sleep > 0) {
                    Thread.sleep(sleep);
                }
            }
        }
    }

    private static String sampleGroup(Admin admin, String groupId, long ts,
                                      BufferedWriter group, BufferedWriter members) throws Exception {
        ConsumerGroupDescription d = admin.describeConsumerGroups(List.of(groupId))
            .describedGroups().get(groupId).get(5, TimeUnit.SECONDS);
        int assigned = 0;
        int upgraded = 0;
        int classic = 0;
        for (MemberDescription m : d.members()) {
            int size = m.assignment().topicPartitions().size();
            assigned += size;
            if (m.upgraded().isPresent()) {
                if (m.upgraded().get()) {
                    upgraded++;
                } else {
                    classic++;
                }
            }
            String parts = m.assignment().topicPartitions().stream()
                .map(TopicPartition::partition).sorted().map(String::valueOf)
                .collect(Collectors.joining(";"));
            members.write(ts + "," + m.clientId() + "," + m.groupInstanceId().orElse("") + "," + m.consumerId()
                + "," + m.memberEpoch().map(String::valueOf).orElse("") + ","
                + m.upgraded().map(String::valueOf).orElse("") + "," + parts);
            members.newLine();
        }
        members.flush();
        group.write(ts + "," + d.type() + "," + d.groupState() + "," + d.members().size() + ","
            + d.groupEpoch().map(String::valueOf).orElse("") + ","
            + d.targetAssignmentEpoch().map(String::valueOf).orElse("") + ","
            + assigned + "," + upgraded + "," + classic);
        group.newLine();
        group.flush();
        return "type=" + d.type() + " state=" + d.groupState() + " members=" + d.members().size()
            + " assigned=" + assigned;
    }

    private static String sampleLag(Admin admin, String groupId, List<TopicPartition> partitions,
                                    Map<TopicPartition, OffsetSpec> latest, long ts, BufferedWriter lag) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
            .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        Map<TopicPartition, ListOffsetsResultInfo> ends = admin.listOffsets(latest).all().get(5, TimeUnit.SECONDS);
        long total = 0;
        long max = 0;
        int uncommitted = 0;
        List<String> per = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            OffsetAndMetadata c = committed.get(tp);
            if (c == null) {
                uncommitted++;
                per.add(tp.partition() + ":");
                continue;
            }
            long l = Math.max(0, ends.get(tp).offset() - c.offset());
            total += l;
            max = Math.max(max, l);
            per.add(tp.partition() + ":" + l);
        }
        lag.write(ts + "," + total + "," + max + "," + uncommitted + "," + String.join(";", per));
        lag.newLine();
        lag.flush();
        return "lag=" + total + " uncommitted=" + uncommitted;
    }

    private static String rootCause(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName();
    }
}
