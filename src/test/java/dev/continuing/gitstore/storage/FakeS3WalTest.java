package dev.continuing.gitstore.storage;

import dev.continuing.gitstore.model.PushCommand;
import dev.continuing.gitstore.model.PushOutcome;
import dev.continuing.gitstore.wal.WalRefStore;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FakeS3WalTest {
    @Test
    void sdkConditionalPutIsIfNoneMatchAndASecondStoreCanRead() throws Exception {
        try (var fake = new FakeS3("gitstore")) {
            var config = new S3Config(fake.endpoint(), "us-east-1", "gitstore", "key", "secret");
            try (var storage = new S3ObjectStorage(config);
                 var readerStorage = new S3ObjectStorage(config)) {
                PutResult created = storage.put("demo/probe", "one".getBytes(StandardCharsets.UTF_8), "*", null);
                PutResult raced = storage.put("demo/probe", "two".getBytes(StandardCharsets.UTF_8), "*", null);
                assertTrue(created.applied());
                assertFalse(raced.applied());
                assertEquals("one", new String(storage.get("demo/probe").orElseThrow().body(), StandardCharsets.UTF_8));
                assertTrue(fake.calls.stream().anyMatch(call -> call.contains("none=*")), fake.calls.toString());

                var writer = new WalRefStore(storage);
                var reader = new WalRefStore(readerStorage);
                PushOutcome outcome = writer.push("demo", PushCommand.of(
                        "refs/heads/main",
                        null,
                        "Ada Lovelace",
                        "hello",
                        Map.of("README.md", "hello\n".getBytes(StandardCharsets.UTF_8))));
                var ok = assertInstanceOf(PushOutcome.Ok.class, outcome);
                assertEquals(writer.fetch("demo"), reader.fetch("demo"));
                assertEquals("hello\n", new String(reader.files("demo", "refs/heads/main").get("README.md"),
                        StandardCharsets.UTF_8));
                assertEquals(ok.commit(), reader.fetch("demo").commit("refs/heads/main"));
            }
        }
    }
}
