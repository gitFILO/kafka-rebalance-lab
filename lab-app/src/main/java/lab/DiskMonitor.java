package lab;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

// 브로커는 로그 디렉터리 여유 공간을 JMX로 노출하지 않는다. 데이터 볼륨을 읽기 전용으로 마운트해 대신 잰다.
final class DiskMonitor {

    void run() throws Exception {
        String[] targets = Env.str("DISK_TARGETS", "broker-1,broker-2,broker-3").split(",");
        Path root = Path.of(Env.str("DISK_ROOT", "/disks"));
        System.out.printf("disk-monitor targets=%s%n", String.join(",", targets));
        while (true) {
            for (String broker : targets) {
                LabMetrics m = LabMetrics.of("type=disk,broker=" + broker);
                try {
                    FileStore store = Files.getFileStore(root.resolve(broker));
                    m.set("TotalBytes", store.getTotalSpace());
                    m.set("UsableBytes", store.getUsableSpace());
                } catch (IOException e) {
                    m.set("UsableBytes", -1);
                }
            }
            Thread.sleep(2_000);
        }
    }
}
