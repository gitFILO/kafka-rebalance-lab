package lab;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

// run 디렉터리의 control/ 파일로 실행 중인 프로세스의 동작을 바꾼다. 스크립트가 파일을 쓰고 앱이 읽는다.
final class Control {

    private final Path dir;

    Control(Path dir) {
        this.dir = dir;
    }

    Optional<String> read(String name) {
        Path file = dir.resolve(name);
        try {
            if (Files.notExists(file)) {
                return Optional.empty();
            }
            String value = Files.readString(file, StandardCharsets.UTF_8).trim();
            return value.isEmpty() ? Optional.empty() : Optional.of(value);
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    // 1회성 지시. 읽으면서 지운다.
    Optional<String> take(String name) {
        Optional<String> value = read(name);
        if (value.isPresent()) {
            try {
                Files.deleteIfExists(dir.resolve(name));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return value;
    }

    // 멤버 전용 파일이 있으면 그것을, 없으면 전체 파일을 쓴다.
    Optional<String> read(String member, String name) {
        Optional<String> own = read(member + "." + name);
        return own.isPresent() ? own : read(name);
    }

    long readLong(String member, String name, long defaultValue) {
        return read(member, name).map(Long::parseLong).orElse(defaultValue);
    }
}
