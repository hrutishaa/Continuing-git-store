package dev.continuing.gitstore.storage;

import dev.continuing.gitstore.model.PushCommand;
import dev.continuing.gitstore.model.PushOutcome;
import dev.continuing.gitstore.wal.WalRefStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs only when a MinIO (or other S3) endpoint is provided. GitHub Actions starts MinIO
 * and sets {@code GITSTORE_MINIO_ENDPOINT}.
 */
@EnabledIfEnvironmentVariable(named = "GITSTORE_MINIO_ENDPOINT", matches = ".+")
class MinioWalIT {
    @Test
    void conditionalPutAndCrossStoreFetch() {
        var config = new S3Config(
                System.getenv("GITSTORE_MINIO_ENDPOINT"),
                env("GITSTORE_MINIO_REGION", "us-east-1"),
                env("GITSTORE_MINIO_BUCKET", "gitstore-it"),
                env("GITSTORE_MINIO_ACCESS_KEY", "minioadmin"),
                env("GITSTORE_MINIO_SECRET_KEY", "minioadmin"));
        String key = "probe/" + System.nanoTime();
        try (var writerStorage = new S3ObjectStorage(config);
             var readerStorage = new S3ObjectStorage(config)) {
            PutResult created = writerStorage.put(key, "one".getBytes(StandardCharsets.UTF_8), "*", null);
            PutResult raced = writerStorage.put(key, "two".getBytes(StandardCharsets.UTF_8), "*", null);
            assertTrue(created.applied());
            assertFalse(raced.applied());

            String repo = "m" + Long.toString(System.nanoTime(), 36);
            var writer = new WalRefStore(writerStorage);
            var reader = new WalRefStore(readerStorage);
            PushOutcome outcome = writer.push(repo, PushCommand.of(
                    "refs/heads/main",
                    null,
                    "Ada Lovelace",
                    "from minio",
                    Map.of("README.md", "minio\n".getBytes(StandardCharsets.UTF_8))));
            var ok = assertInstanceOf(PushOutcome.Ok.class, outcome);
            assertEquals(ok.commit(), reader.fetch(repo).commit("refs/heads/main"));
            assertEquals("minio\n", new String(
                    reader.files(repo, "refs/heads/main").get("README.md"), StandardCharsets.UTF_8));
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
