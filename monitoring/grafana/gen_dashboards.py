#!/usr/bin/env python3
"""Grafana 대시보드 JSON 생성기. 수정 후 python3 monitoring/grafana/gen_dashboards.py 로 다시 만든다."""
import json
import os

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboards")
DS = {"type": "prometheus", "uid": "prometheus"}
TOPIC = 'topic="lab-events"'
W = "[30s]"


class Board:
    def __init__(self, uid, title, links):
        self.uid, self.title, self.links = uid, title, links
        self.panels, self.x, self.y, self.row_h, self.next_id = [], 0, 0, 0, 1

    def row(self, title):
        self._newline()
        self.panels.append({"type": "row", "title": title, "id": self._id(), "collapsed": False,
                            "gridPos": {"x": 0, "y": self.y, "w": 24, "h": 1}, "panels": []})
        self.y += 1

    def ts(self, title, targets, unit="short", w=8, h=7, desc="", stack=False, min0=True, steps=False):
        fc = {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 10, "showPoints": "never",
                                       "spanNulls": False, "lineInterpolation": "stepAfter" if steps else "linear",
                                       "stacking": {"mode": "normal" if stack else "none"}}}
        if min0:
            fc["min"] = 0
        self._add({"type": "timeseries", "title": title, "description": desc,
                   "fieldConfig": {"defaults": fc, "overrides": []},
                   "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                               "tooltip": {"mode": "multi", "sort": "desc"}},
                   "targets": self._targets(targets)}, w, h)

    def stat(self, title, expr, unit="short", w=3, h=4, thresholds=None, desc="", text_mode="value",
             legend="", color_mode="background"):
        steps = [{"color": "green", "value": None}]
        for value, color in (thresholds or []):
            steps.append({"color": color, "value": value})
        self._add({"type": "stat", "title": title, "description": desc,
                   "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps},
                                                "color": {"mode": "thresholds"}}, "overrides": []},
                   "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                               "colorMode": color_mode, "graphMode": "none", "textMode": text_mode,
                               "justifyMode": "center"},
                   "targets": self._targets([(expr, legend)])}, w, h)

    def state_timeline(self, title, expr, legend, w=24, h=5, desc=""):
        self._add({"type": "state-timeline", "title": title, "description": desc,
                   "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
                   "options": {"showValue": "never", "mergeValues": True, "rowHeight": 0.8},
                   "targets": self._targets([(expr, legend)])}, w, h)

    def _targets(self, targets):
        out = []
        for i, t in enumerate(targets):
            expr, legend = t if isinstance(t, tuple) else (t, "")
            out.append({"datasource": DS, "expr": expr, "legendFormat": legend, "refId": chr(65 + i),
                        "interval": "5s"})
        return out

    def _add(self, panel, w, h):
        if self.x + w > 24:
            self._newline()
        panel.update({"id": self._id(), "datasource": DS, "gridPos": {"x": self.x, "y": self.y, "w": w, "h": h}})
        self.panels.append(panel)
        self.x += w
        self.row_h = max(self.row_h, h)

    def _newline(self):
        if self.x:
            self.y += self.row_h
        self.x, self.row_h = 0, 0

    def _id(self):
        self.next_id += 1
        return self.next_id

    def write(self):
        board = {
            "uid": self.uid, "title": self.title, "tags": ["kafka-rebalance-lab"], "timezone": "browser",
            "schemaVersion": 41, "version": 1, "editable": True, "refresh": "5s",
            "time": {"from": "now-15m", "to": "now"},
            "annotations": {"list": [
                {"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True,
                 "hide": True, "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts",
                 "type": "dashboard"},
                {"datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True,
                 "iconColor": "orange", "name": "lab markers",
                 "target": {"type": "tags", "tags": ["lab"], "matchAny": True, "limit": 500}},
            ]},
            "links": [{"title": t, "type": "link", "url": "/d/" + u} for t, u in self.links],
            "panels": self.panels,
        }
        with open(os.path.join(OUT, self.uid + ".json"), "w") as f:
            json.dump(board, f, indent=1, ensure_ascii=False)
            f.write("\n")


LINKS = [("Overview", "lab-overview"), ("Cluster", "lab-cluster"), ("Clients", "lab-clients")]


def overview():
    b = Board("lab-overview", "Lab / Overview", LINKS)
    b.row("클러스터 상태")
    b.stat("Active controller", "sum(kafka_controller_kafkacontroller_activecontrollercount)",
           thresholds=[(0, "red"), (1, "green"), (2, "red")], desc="정상은 1. 0이면 쿼럼 상실")
    b.stat("Brokers up", 'sum(up{job="broker"})', thresholds=[(0, "red"), (3, "green")])
    b.stat("Offline partitions", "sum(kafka_controller_kafkacontroller_offlinepartitionscount)",
           thresholds=[(1, "red")])
    b.stat("Under-replicated", "sum(kafka_server_replicamanager_underreplicatedpartitions)",
           thresholds=[(1, "orange")])
    b.stat("Under min ISR", "sum(kafka_server_replicamanager_underminisrpartitioncount)",
           thresholds=[(1, "red")], desc="이 파티션들은 acks=all 쓰기가 NotEnoughReplicas로 실패한다")
    b.stat("Fenced brokers", "sum(kafka_controller_kafkacontroller_fencedbrokercount)", thresholds=[(1, "red")])
    b.stat("Offline log dirs", "sum(kafka_log_logmanager_offlinelogdirectorycount)", thresholds=[(1, "red")])
    b.stat("Min disk free", "min(lab_disk_usablebytes / lab_disk_totalbytes) * 100", unit="percent",
           thresholds=[(0, "red"), (10, "orange"), (25, "green")], desc="브로커 데이터 볼륨 중 가장 적은 여유율")

    b.row("흐름: 생산 → 브로커 → 소비")
    b.ts("Producer send / ack / fail / skip (msg/s)", [
        ("rate(lab_producer_senttotal" + W + ")", "sent"),
        ("rate(lab_producer_ackedtotal" + W + ")", "acked"),
        ("rate(lab_producer_failedtotal" + W + ")", "failed"),
        ("rate(lab_producer_skippedtotal" + W + ")", "skipped(backpressure)"),
    ], desc="skipped는 send()가 막혀 상류 요청을 받지 못한 분량")
    b.ts("Broker messages in (msg/s)", [
        ("sum by (instance) (rate(kafka_server_brokertopicmetrics_messagesin_total{" + TOPIC + "}" + W + "))",
         "{{instance}}")], stack=True)
    b.ts("Consumer processed (msg/s)", [
        ("sum by (instance) (rate(lab_consumer_processedtotal" + W + "))", "{{instance}}")], stack=True)

    b.row("컨슈머 그룹")
    b.state_timeline("Group state", "max by (state) (lab_group_state_active) == 1", "{{state}}", h=4)
    b.ts("Committed lag: total / max partition", [
        ("lab_group_totallag", "total"), ("lab_group_maxpartitionlag", "max partition")], w=8)
    b.ts("Committed lag by partition", [("lab_partition_lag", "p{{partition}}")], w=8)
    b.ts("Members / assigned partitions / epoch", [
        ("lab_group_members", "members"), ("lab_group_assignedpartitions", "assigned"),
        ("lab_group_groupepoch", "group epoch")], w=8, steps=True)
    b.ts("Assigned partitions per member", [("lab_consumer_assignedpartitions", "{{instance}}")],
         stack=True, steps=True)
    b.ts("Rebalance callbacks (per 1m)", [
        ("sum by (instance) (increase(lab_consumer_revokedtotal[1m]))", "revoked {{instance}}"),
        ("sum by (instance) (increase(lab_consumer_losttotal[1m]))", "lost {{instance}}")])
    b.ts("Commit failures / errors / poison (per 1m)", [
        ("sum by (instance) (increase(lab_consumer_commitfailedtotal[1m]))", "commit failed {{instance}}"),
        ("sum by (instance) (increase(lab_consumer_errortotal[1m]))", "error {{instance}}"),
        ("sum by (instance) (increase(lab_consumer_poisonfailtotal[1m]))", "poison fail {{instance}}")])

    b.row("지연")
    b.ts("Produce p99 by broker (ms)", [
        ('kafka_network_requestmetrics_totaltimems{request="Produce",quantile="0.99"}', "{{instance}}")],
         unit="ms", desc="브로커가 Produce 요청을 받아 응답할 때까지. acks=all이면 복제 대기 포함")
    b.ts("Consumer fetch p99 by broker (ms)", [
        ('kafka_network_requestmetrics_totaltimems{request="FetchConsumer",quantile="0.99"}', "{{instance}}")],
         unit="ms", desc="fetch.max.wait.ms(500ms) 대기가 포함된다")
    b.ts("Producer request latency (ms)", [
        ("kafka_producer_producer_metrics_request_latency_avg", "avg"),
        ("kafka_producer_producer_metrics_request_latency_max", "max"),
        ("kafka_producer_producer_metrics_record_queue_time_avg", "queue avg")], unit="ms")
    b.write()


def cluster():
    b = Board("lab-cluster", "Lab / Cluster", LINKS)
    b.row("처리량과 부하")
    b.ts("Bytes in by broker", [
        ("sum by (instance) (rate(kafka_server_brokertopicmetrics_bytesin_total{topic!=\"\"}" + W + "))",
         "{{instance}}")], unit="Bps")
    b.ts("Bytes out by broker (consumers + replication)", [
        ("sum by (instance) (rate(kafka_server_brokertopicmetrics_bytesout_total{topic!=\"\"}" + W + "))",
         "out {{instance}}"),
        ("rate(kafka_server_brokertopicmetrics_replicationbytesout_total{topic=\"\"}" + W + ")",
         "replication {{instance}}")], unit="Bps")
    b.ts("Failed produce / fetch requests (/s)", [
        ("sum by (instance) (rate(kafka_server_brokertopicmetrics_failedproducerequests_total{topic!=\"\"}" + W + "))",
         "produce {{instance}}"),
        ("sum by (instance) (rate(kafka_server_brokertopicmetrics_failedfetchrequests_total{topic!=\"\"}" + W + "))",
         "fetch {{instance}}")])
    b.ts("Request handler idle", [
        ('kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent{job="broker"}', "{{instance}}")],
         unit="percentunit", desc="0에 가까우면 I/O 스레드 포화")
    b.ts("Network processor idle", [
        ("kafka_network_socketserver_networkprocessoravgidlepercent", "{{instance}}")], unit="percentunit")
    b.ts("Request queue size", [("kafka_network_requestchannel_requestqueuesize", "{{instance}}")])

    b.row("요청 지연 (p99, ms)")
    for req in ("Produce", "FetchConsumer", "FetchFollower"):
        b.ts(req + " total time p99", [
            ('kafka_network_requestmetrics_totaltimems{request="%s",quantile="0.99"}' % req, "{{instance}}")],
             unit="ms")
    b.ts("Produce p99 breakdown: remote(복제 대기) / local / queue", [
        ('kafka_network_requestmetrics_remotetimems{request="Produce",quantile="0.99"}', "remote {{instance}}"),
        ('kafka_network_requestmetrics_localtimems{request="Produce",quantile="0.99"}', "local {{instance}}"),
        ('kafka_network_requestmetrics_requestqueuetimems{request="Produce",quantile="0.99"}',
         "queue {{instance}}")], unit="ms", w=12)
    b.ts("Purgatory size", [
        ("kafka_server_delayedoperationpurgatory_purgatorysize", "{{delayedoperation}} {{instance}}")], w=12)

    b.row("복제와 리더")
    b.ts("Under-replicated by broker", [("kafka_server_replicamanager_underreplicatedpartitions", "{{instance}}")],
         steps=True)
    b.ts("ISR shrink / expand (per 1m)", [
        ("increase(kafka_server_replicamanager_isrshrinks_total" + "[1m])", "shrink {{instance}}"),
        ("increase(kafka_server_replicamanager_isrexpands_total" + "[1m])", "expand {{instance}}")])
    b.ts("Leader count by broker", [("kafka_server_replicamanager_leadercount", "{{instance}}")], steps=True)
    b.ts("Partition count by broker", [("kafka_server_replicamanager_partitioncount", "{{instance}}")], steps=True)
    b.ts("At min ISR / under min ISR", [
        ("kafka_server_replicamanager_atminisrpartitioncount", "at min {{instance}}"),
        ("kafka_server_replicamanager_underminisrpartitioncount", "under min {{instance}}")], steps=True)
    b.ts("Offline replicas / failed ISR updates", [
        ("kafka_server_replicamanager_offlinereplicacount", "offline {{instance}}"),
        ("increase(kafka_server_replicamanager_failedisrupdates_total[1m])", "failed isr update {{instance}}")])

    b.row("디스크와 로그")
    b.ts("Disk free by broker", [("lab_disk_usablebytes", "{{broker}}")], unit="bytes")
    b.ts("Log size by broker (lab-events)", [
        ("sum by (instance) (kafka_log_log_size{" + TOPIC + "})", "{{instance}}")], unit="bytes")
    b.ts("Log start offset (lab-events, max by partition)", [
        ("max by (partition) (kafka_log_log_logstartoffset{" + TOPIC + "})", "p{{partition}}")],
         desc="값이 올라가면 retention이 세그먼트를 지운 것이다")
    b.ts("Log flush p99 (ms)", [('kafka_log_logflushstats_logflushrateandtimems{quantile="0.99"}', "{{instance}}")],
         unit="ms")
    b.ts("Offline log dirs", [("kafka_log_logmanager_offlinelogdirectorycount", "{{instance}}")], steps=True)

    b.row("KRaft 컨트롤러")
    b.ts("Raft current leader (as seen by node)", [("kafka_server_raft_metrics_current_leader", "{{instance}}")],
         steps=True, min0=False)
    b.ts("Raft epoch", [("kafka_server_raft_metrics_current_epoch", "{{instance}}")], steps=True)
    b.ts("Raft commit latency avg (ms)", [
        ('kafka_server_raft_metrics_commit_latency_avg{job="controller"}', "{{instance}}")], unit="ms")
    b.ts("Fenced brokers / offline partitions", [
        ("sum(kafka_controller_kafkacontroller_fencedbrokercount)", "fenced"),
        ("sum(kafka_controller_kafkacontroller_offlinepartitionscount)", "offline partitions")], steps=True)
    b.ts("Leader elections (per 1m)", [
        ("sum(increase(kafka_controller_controllerstats_uncleanleaderelections_total[1m]))", "unclean"),
        ("sum(increase(kafka_controller_controllerstats_electionfromeligibleleaderreplicas_total[1m]))", "ELR")])

    b.row("그룹 코디네이터")
    b.ts("Consumer groups by state", [
        ("sum by (state) (kafka_server_group_coordinator_metrics_consumer_group_count)", "{{state}}")], steps=True)
    b.ts("Classic groups by state", [
        ("sum by (state) (kafka_server_group_coordinator_metrics_group_count)", "{{state}}")], steps=True)
    b.ts("Rebalance rate (per s)", [
        ("sum(kafka_server_group_coordinator_metrics_consumer_group_rebalance_rate)", "consumer protocol"),
        ("sum(kafka_server_group_coordinator_metrics_group_completed_rebalance_rate)", "classic")])
    b.ts("Offset commit / expiration rate", [
        ("sum(kafka_server_group_coordinator_metrics_offset_commit_rate)", "commit"),
        ("sum(kafka_server_group_coordinator_metrics_offset_expiration_rate)", "expiration")])
    b.ts("Coordinator event queue", [
        ("kafka_server_group_coordinator_metrics_event_queue_size", "size {{instance}}"),
        ("kafka_server_group_coordinator_metrics_event_queue_time_ms_max", "time max {{instance}}")])

    b.row("JVM")
    b.ts("Heap used", [('jvm_memory_used_bytes{area="heap",job=~"broker|controller"}', "{{instance}}")],
         unit="bytes", w=12)
    b.ts("GC time (s/s)", [
        ('sum by (instance) (rate(jvm_gc_collection_seconds_sum{job=~"broker|controller"}' + W + "))",
         "{{instance}}")], unit="percentunit", w=12)
    b.write()


def clients():
    b = Board("lab-clients", "Lab / Clients", LINKS)
    b.row("컨슈머: 처리")
    b.ts("Processed per member (msg/s)", [
        ("sum by (instance) (rate(lab_consumer_processedtotal" + W + "))", "{{instance}}")])
    b.ts("Processed per partition (msg/s)", [
        ("sum by (partition) (rate(lab_consumer_partition_processedtotal" + W + "))", "p{{partition}}")])
    b.ts("Process delay setting (ms/record)", [("lab_consumer_processms", "{{instance}}")], steps=True)
    b.ts("Fetch lag max per member (records)", [
        ("max by (instance) (kafka_consumer_consumer_fetch_manager_metrics_records_lag_max)", "{{instance}}")],
         desc="컨슈머 자신이 본 fetch position 기준 lag. 커밋 lag과 다르다")
    b.ts("Time between poll max (ms)", [
        ("kafka_consumer_consumer_metrics_time_between_poll_max", "{{instance}}")], unit="ms",
         desc="max.poll.interval.ms(기본 실험 30s)에 가까워지면 poll timeout 위험")
    b.ts("Poll idle ratio", [("kafka_consumer_consumer_metrics_poll_idle_ratio_avg", "{{instance}}")],
         unit="percentunit", desc="0에 가까우면 처리 시간이 poll 주기 대부분을 차지한다")

    b.row("컨슈머: 그룹 프로토콜")
    b.ts("Last poll seconds ago", [("kafka_consumer_consumer_metrics_last_poll_seconds_ago", "{{instance}}")],
         unit="s")
    b.ts("Last heartbeat seconds ago", [
        ("kafka_consumer_consumer_coordinator_metrics_last_heartbeat_seconds_ago", "{{instance}}")], unit="s")
    b.ts("Heartbeat response time max (ms)", [
        ("kafka_consumer_consumer_coordinator_metrics_heartbeat_response_time_max", "{{instance}}")], unit="ms")
    b.ts("Commit latency avg / max (ms)", [
        ("kafka_consumer_consumer_coordinator_metrics_commit_latency_avg", "avg {{instance}}"),
        ("kafka_consumer_consumer_coordinator_metrics_commit_latency_max", "max {{instance}}")], unit="ms")
    b.ts("Rebalance latency max (ms)", [
        ("kafka_consumer_consumer_coordinator_metrics_rebalance_latency_max", "{{instance}}")], unit="ms")
    b.ts("Rebalances per hour (client)", [
        ("kafka_consumer_consumer_coordinator_metrics_rebalance_rate_per_hour", "{{instance}}"),
        ("kafka_consumer_consumer_coordinator_metrics_failed_rebalance_rate_per_hour", "failed {{instance}}")])

    b.row("컨슈머: 이상 신호")
    b.ts("Poison fail / skipped (per 1m)", [
        ("sum by (instance) (increase(lab_consumer_poisonfailtotal[1m]))", "fail {{instance}}"),
        ("sum by (instance) (increase(lab_consumer_poisonskippedtotal[1m]))", "skipped {{instance}}")])
    b.ts("Skipped offsets (offset jump > 100)", [
        ("sum by (instance) (increase(lab_consumer_skippedoffsetstotal[1m]))", "{{instance}}")],
         desc="auto.offset.reset 또는 retention 삭제로 위치가 건너뛴 레코드 수")
    b.ts("Stalled / errors", [
        ("lab_consumer_stalled", "stalled {{instance}}"),
        ("sum by (instance) (increase(lab_consumer_errortotal[1m]))", "error {{instance}}")], steps=True)
    b.ts("HW - LSO by partition (records)", [
        ("lab_partition_logendoffset - lab_partition_laststableoffset", "p{{partition}}")],
         desc="계속 벌어지면 열린 트랜잭션이 LSO를 막고 있다. read_committed 컨슈머는 LSO까지만 읽는다")
    b.ts("Committed offset by partition", [("lab_partition_committedoffset", "p{{partition}}")])
    b.ts("Lag monitor errors (per 1m)", [
        ("increase(lab_group_describeerrorstotal[1m])", "describe"),
        ("increase(lab_group_lagerrorstotal[1m])", "lag")])

    b.row("프로듀서")
    b.ts("Record send / error / retry rate", [
        ("kafka_producer_producer_metrics_record_send_rate", "send"),
        ("kafka_producer_producer_metrics_record_error_rate", "error"),
        ("kafka_producer_producer_metrics_record_retry_rate", "retry")])
    b.ts("Errors by type (per 1m)", [("increase(lab_producer_error_count[1m])", "{{error}}")])
    b.ts("Request latency avg / max (ms)", [
        ("kafka_producer_producer_metrics_request_latency_avg", "avg"),
        ("kafka_producer_producer_metrics_request_latency_max", "max")], unit="ms")
    b.ts("Buffer available / total", [
        ("kafka_producer_producer_metrics_buffer_available_bytes", "available"),
        ("kafka_producer_producer_metrics_buffer_total_bytes", "total")], unit="bytes")
    b.ts("Bufferpool wait ratio / send() blocked", [
        ("kafka_producer_producer_metrics_bufferpool_wait_ratio", "bufferpool wait ratio"),
        ("rate(lab_producer_sendblockedmstotal" + W + ") / 1000", "send() blocked (s/s)")],
         unit="percentunit")
    b.ts("Requests in flight / batch size avg", [
        ("kafka_producer_producer_metrics_requests_in_flight", "in flight"),
        ("kafka_producer_producer_metrics_batch_size_avg", "batch size avg")])
    b.ts("Producer node latency (ms)", [
        ("kafka_producer_producer_node_metrics_request_latency_avg", "node {{node_id}}")], unit="ms",
         desc="브로커별 요청 지연. 특정 노드만 높으면 그 브로커가 느리다")
    b.ts("Target rate / skipped", [
        ("lab_producer_targetrate", "target rate"),
        ("rate(lab_producer_skippedtotal" + W + ")", "skipped/s")])
    b.ts("Transaction commit time (ns, cumulative)", [
        ("kafka_producer_producer_metrics_txn_commit_time_ns", "commit")])

    b.row("JVM (앱)")
    b.ts("Heap used", [('jvm_memory_used_bytes{area="heap",job=~"consumer|producer|lag-monitor"}', "{{instance}}")],
         unit="bytes", w=12)
    b.ts("GC time (s/s)", [
        ('sum by (instance) (rate(jvm_gc_collection_seconds_sum{job=~"consumer|producer|lag-monitor"}' + W + "))",
         "{{instance}}")], unit="percentunit", w=12)
    b.write()


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    overview()
    cluster()
    clients()
    print("dashboards written to", OUT)
