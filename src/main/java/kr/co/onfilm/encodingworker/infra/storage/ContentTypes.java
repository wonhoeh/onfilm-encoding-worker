package kr.co.onfilm.encodingworker.infra.storage;

import java.nio.file.Path;

final class ContentTypes {

    private ContentTypes() {
    }

    static String resolve(Path file, String fallback) {
        String name = file.getFileName().toString();
        if (name.endsWith(".m3u8")) {
            return "application/vnd.apple.mpegurl";
        }
        if (name.endsWith(".ts")) {
            return "video/mp2t";
        }
        if (name.endsWith(".jpg") || name.endsWith(".jpeg")) {
            return "image/jpeg";
        }
        return fallback;
    }
}
