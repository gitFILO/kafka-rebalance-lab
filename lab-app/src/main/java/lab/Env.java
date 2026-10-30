package lab;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class Env {

    private Env() {
    }

    static String str(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    static String required(String key) {
        String value = str(key, null);
        if (value == null) {
            throw new IllegalStateException("missing env " + key);
        }
        return value;
    }

    static int integer(String key, int defaultValue) {
        return Integer.parseInt(str(key, String.valueOf(defaultValue)));
    }

    static boolean bool(String key, boolean defaultValue) {
        return Boolean.parseBoolean(str(key, String.valueOf(defaultValue)));
    }

    static Path runDir() throws IOException {
        Path dir = Path.of(str("RUN_DIR", "/runs/adhoc"));
        Files.createDirectories(dir.resolve("control"));
        return dir;
    }

    static BufferedWriter appender(Path file, String header) throws IOException {
        boolean fresh = Files.notExists(file);
        BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        if (fresh) {
            writer.write(header);
            writer.newLine();
            writer.flush();
        }
        return writer;
    }
}
