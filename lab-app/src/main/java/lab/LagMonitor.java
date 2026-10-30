package lab;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.IsolationLevel;
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

        LabMetrics.of("type=group").set("DescribeErrorsTotal", 0);
        LabMetrics.of("type=group").set("LagErrorsTotal", 0);

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
                    setState(GroupState.UNKNOWN);
                    LabMetrics.of("type=group").add("DescribeErrorsTotal", 1);
                    group.write(ts + ",,N/A,0,,,0,0,0");
                    group.newLine();
                    group.flush();
                }
                try {
                    summary += " " + sampleLag(admin, groupId, partitions, latest, ts, lag);
                } catch (Exception e) {
                    summary += " lag=N/A(" + rootCause(e) + ")";
                    LabMetrics.of("type=group").add("LagErrorsTotal", 1);
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
        LabMetrics g = LabMetrics.of("type=group");
        g.set("Members", d.members().size());
        g.set("GroupEpoch", d.groupEpoch().orElse(-1));
        g.set("TargetAssignmentEpoch", d.targetAssignmentEpoch().orElse(-1));
        g.set("AssignedPartitions", assigned);
        g.set("ConsumerProtocolMembers", upgraded);
        g.set("ClassicProtocolMembers", classic);
        setState(d.groupState());
        return "type=" + d.type() + " state=" + d.groupState() + " members=" + d.members().size()
            + " assigned=" + assigned;
    }

    private static String sampleLag(Admin admin, String groupId, List<TopicPartition> partitions,
                                    Map<TopicPartition, OffsetSpec> latest, long ts, BufferedWriter lag) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
            .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        Map<TopicPartition, ListOffsetsResultInfo> ends = admin.listOffsets(latest).all().get(5, TimeUnit.SECONDS);
        Map<TopicPartition, ListOffsetsResultInfo> stable = admin.listOffsets(latest,
            new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)).all().get(5, TimeUnit.SECONDS);
        long total = 0;
        long max = 0;
        int uncommitted = 0;
        List<String> per = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            OffsetAndMetadata c = committed.get(tp);
            LabMetrics p = LabMetrics.of("type=partition,partition=" + tp.partition());
            long end = ends.get(tp).offset();
            p.set("LogEndOffset", end);
            // HW와 LSO의 차이가 계속 벌어지면 열린 트랜잭션이 LSO를 붙잡고 있는 것이다.
            p.set("LastStableOffset", stable.get(tp).offset());
            p.set("CommittedOffset", c == null ? -1 : c.offset());
            if (c == null) {
                p.set("Lag", -1);
                uncommitted++;
                per.add(tp.partition() + ":");
                continue;
            }
            long l = Math.max(0, end - c.offset());
            p.set("Lag", l);
            total += l;
            max = Math.max(max, l);
            per.add(tp.partition() + ":" + l);
        }
        LabMetrics g = LabMetrics.of("type=group");
        g.set("TotalLag", total);
        g.set("MaxPartitionLag", max);
        g.set("UncommittedPartitions", uncommitted);
        lag.write(ts + "," + total + "," + max + "," + uncommitted + "," + String.join(";", per));
        lag.newLine();
        lag.flush();
        return "lag=" + total + " uncommitted=" + uncommitted;
    }

    private static void setState(GroupState current) {
        for (GroupState s : GroupState.values()) {
            LabMetrics.of("type=group-state,state=" + s).set("Active", s == current ? 1 : 0);
        }
    }

    private static String rootCause(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName();
    }
}
