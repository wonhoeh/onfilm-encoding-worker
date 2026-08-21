package kr.co.onfilm.encodingworker.infra.transcode;

import kr.co.onfilm.encodingworker.config.AppProperties;
import kr.co.onfilm.encodingworker.domain.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Component
@RequiredArgsConstructor
public class FfmpegTranscoder {
    private static final int LOG_TAIL_BYTES = 8 * 1024;
    private final FfmpegCommandFactory commandFactory;
    private final AppProperties properties;

    public EncodedOutput transcode(MediaEncodeRequestedMessage message, Path sourceFile, Path workingDir) {
        Path targetFile = resolveTargetPath(message, workingDir);
        Path logFile = workingDir.resolve("ffmpeg.log");
        Process process = null;
        try {
            Files.createDirectories(targetFile.getParent());
            process = new ProcessBuilder(commandFactory.create(message.preset(), sourceFile, targetFile))
                    .redirectErrorStream(true)
                    .redirectOutput(logFile.toFile())
                    .start();
            boolean finished = process.waitFor(
                    properties.worker().transcodeTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) {
                terminate(process);
                throw new TranscodeTimeoutException(
                        "ffmpeg exceeded timeout " + properties.worker().transcodeTimeout());
            }
            if (process.exitValue() != 0) {
                throw new TranscodeException(
                        "ffmpeg exited with code %d: %s".formatted(process.exitValue(), readTail(logFile)));
            }
            return new EncodedOutput(message.targetContentType(), collectOutputFiles(targetFile));
        } catch (IOException exception) {
            throw new TranscodeException("Failed to run ffmpeg process", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            if (process != null) terminate(process);
            throw new TranscodeException("ffmpeg process was interrupted", exception);
        }
    }

    private Path resolveTargetPath(MediaEncodeRequestedMessage message, Path workingDir) {
        Path normalizedWorkingDir = workingDir.toAbsolutePath().normalize();
        Path target = normalizedWorkingDir.resolve(message.targetKey()).normalize();
        if (!target.startsWith(normalizedWorkingDir)) {
            throw new TranscodeException("targetKey escapes working directory");
        }
        return target;
    }

    private List<Path> collectOutputFiles(Path targetFile) throws IOException {
        if (targetFile.toString().endsWith(".jpg")) return List.of(targetFile);
        try (Stream<Path> stream = Files.list(targetFile.getParent())) {
            return stream.filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toList();
        }
    }

    private void terminate(Process process) {
        process.descendants().forEach(ProcessHandle::destroy);
        process.destroy();
        try {
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.descendants().forEach(ProcessHandle::destroyForcibly);
                process.destroyForcibly();
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    private String readTail(Path logFile) {
        if (Files.notExists(logFile)) return "";
        try (RandomAccessFile file = new RandomAccessFile(logFile.toFile(), "r")) {
            long start = Math.max(0, file.length() - LOG_TAIL_BYTES);
            file.seek(start);
            byte[] bytes = new byte[(int) (file.length() - start)];
            file.readFully(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException exception) {
            return "unable to read ffmpeg log";
        }
    }
}
