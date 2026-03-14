package kr.co.onfilm.encodingworker.infra.transcode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import kr.co.onfilm.encodingworker.domain.EncodedOutput;
import kr.co.onfilm.encodingworker.domain.MediaEncodeRequestedMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class FfmpegTranscoder {

    private final FfmpegCommandFactory commandFactory;

    public EncodedOutput transcode(MediaEncodeRequestedMessage message, Path sourceFile, Path workingDir) {
        Path targetFile = resolveTargetPath(message, workingDir);

        try {
            Files.createDirectories(targetFile.getParent());
            Process process = new ProcessBuilder(commandFactory.create(message.preset(), sourceFile, targetFile))
                    .redirectErrorStream(true)
                    .start();
            String logs = new String(process.getInputStream().readAllBytes());
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new TranscodeException("ffmpeg exited with code %d: %s".formatted(exitCode, logs));
            }

            List<Path> files = collectOutputFiles(targetFile);
            return new EncodedOutput(resolveContentType(targetFile), files);
        } catch (IOException exception) {
            throw new TranscodeException("Failed to start ffmpeg process", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new TranscodeException("ffmpeg process was interrupted", exception);
        }
    }

    private Path resolveTargetPath(MediaEncodeRequestedMessage message, Path workingDir) {
        return workingDir.resolve(message.targetKey());
    }

    private List<Path> collectOutputFiles(Path targetFile) throws IOException {
        if (targetFile.toString().endsWith(".jpg")) {
            return List.of(targetFile);
        }
        try (Stream<Path> stream = Files.list(targetFile.getParent())) {
            return stream
                    .filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toList();
        }
    }

    private String resolveContentType(Path targetFile) {
        return targetFile.toString().endsWith(".jpg")
                ? "image/jpeg"
                : "application/vnd.apple.mpegurl";
    }
}
