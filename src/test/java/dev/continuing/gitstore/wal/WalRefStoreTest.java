package dev.continuing.gitstore.wal;

import dev.continuing.gitstore.GitStoreException;
import dev.continuing.gitstore.model.PushCommand;
import dev.continuing.gitstore.model.PushOutcome;
import dev.continuing.gitstore.storage.MemoryObjectStorage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WalRefStoreTest {
    private static final String REPO = "demo";
    private static final String REF = "refs/heads/main";
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void pushThenFetchFromASecondStore() {
        var storage = new MemoryObjectStorage();
        var writer = new WalRefStore(storage, CLOCK, 8);
        var reader = new WalRefStore(storage, CLOCK, 8);
        PushOutcome.Ok created = ok(writer, null, "hello", Map.of("README.md", "hello\n".getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, created.seq());
        assertEquals(writer.fetch(REPO), reader.fetch(REPO));
        assertEquals("hello\n", new String(reader.files(REPO, REF).get("README.md"), StandardCharsets.UTF_8));
        assertEquals(created.commit(), reader.log(REPO, REF, 10).get(0).hash());
    }

    @Test
    void secondPushKeepsEarlierFilesAndLinksTheParent() {
        var store = new WalRefStore(new MemoryObjectStorage(), CLOCK, 8);
        PushOutcome.Ok first = ok(store, null, "one", Map.of("a.txt", "A".getBytes(StandardCharsets.UTF_8)));
        PushOutcome.Ok second = ok(store, first.commit(), "two", Map.of("b.txt", "B".getBytes(StandardCharsets.UTF_8)));
        Map<String, byte[]> files = store.files(REPO, REF);
        assertEquals("A", new String(files.get("a.txt"), StandardCharsets.UTF_8));
        assertEquals("B", new String(files.get("b.txt"), StandardCharsets.UTF_8));
        assertEquals(first.commit(), store.log(REPO, REF, 10).get(0).parents().get(0));
        assertEquals(second.commit(), store.fetch(REPO).commit(REF));
        assertEquals(2, store.fetch(REPO).seq());
    }

    @Test
    void staleExpectedConflictsWithoutAdvancingTheRef() {
        var store = new WalRefStore(new MemoryObjectStorage(), CLOCK, 8);
        PushOutcome.Ok first = ok(store, null, "one", Map.of("a.txt", "A".getBytes(StandardCharsets.UTF_8)));
        PushOutcome outcome = store.push(REPO, PushCommand.of(
                REF, null, "Ada Lovelace", "again", Map.of("a.txt", "B".getBytes(StandardCharsets.UTF_8))));
        var conflict = assertInstanceOf(PushOutcome.Conflict.class, outcome);
        assertNull(conflict.expected());
        assertEquals(first.commit(), conflict.actual());
        assertEquals(1, store.fetch(REPO).seq());
        assertEquals(first.commit(), store.fetch(REPO).commit(REF));
    }

    @Test
    void deletePathRemovesAFile() {
        var store = new WalRefStore(new MemoryObjectStorage(), CLOCK, 8);
        PushOutcome.Ok first = ok(store, null, "one", Map.of(
                "a.txt", "A".getBytes(StandardCharsets.UTF_8),
                "b.txt", "B".getBytes(StandardCharsets.UTF_8)));
        var outcome = store.push(REPO, new PushCommand(
                REF, first.commit(), "Ada Lovelace", "drop b", Map.of(), java.util.List.of("b.txt"), false));
        assertInstanceOf(PushOutcome.Ok.class, outcome);
        assertEquals(1, store.files(REPO, REF).size());
        assertTrue(store.files(REPO, REF).containsKey("a.txt"));
    }

    @Test
    void forcePushBuildsOnTheCurrentTipRatherThanTheStaleExpected() {
        var store = new WalRefStore(new MemoryObjectStorage(), CLOCK, 8);
        PushOutcome.Ok first = ok(store, null, "one", Map.of("a.txt", "A".getBytes(StandardCharsets.UTF_8)));
        PushOutcome.Ok second = ok(store, first.commit(), "two", Map.of("b.txt", "B".getBytes(StandardCharsets.UTF_8)));
        var forced = store.push(REPO, new PushCommand(
                REF,
                first.commit(),
                "Ada Lovelace",
                "forced",
                Map.of("c.txt", "C".getBytes(StandardCharsets.UTF_8)),
                java.util.List.of(),
                true));
        var ok = assertInstanceOf(PushOutcome.Ok.class, forced);
        assertEquals(second.commit(), store.log(REPO, REF, 10).get(0).parents().get(0));
        assertEquals(3, store.files(REPO, REF).size());
        assertEquals(ok.commit(), store.fetch(REPO).commit(REF));
    }

    @Test
    void missingIndexAndALyingTipStillReplayTheLog() {
        var storage = new MemoryObjectStorage();
        var store = new WalRefStore(storage, CLOCK, 8);
        ok(store, null, "one", Map.of("a.txt", "A".getBytes(StandardCharsets.UTF_8)));
        PushOutcome.Ok second = ok(store, store.fetch(REPO).commit(REF), "two",
                Map.of("b.txt", "B".getBytes(StandardCharsets.UTF_8)));
        storage.delete(WalLayout.index(REPO, 1));
        storage.delete(WalLayout.index(REPO, 2));
        storage.put(WalLayout.tip(REPO), "99".getBytes(StandardCharsets.UTF_8), null, null);
        var reader = new WalRefStore(storage, CLOCK, 8);
        assertEquals(2, reader.fetch(REPO).seq());
        assertEquals(second.commit(), reader.fetch(REPO).commit(REF));
        storage.put(WalLayout.tip(REPO), "0".getBytes(StandardCharsets.UTF_8), null, null);
        assertEquals(second.commit(), reader.fetch(REPO).commit(REF));
        assertEquals(reader.fetch(REPO), reader.rebuild(REPO));
    }

    @Test
    void rejectsBadNames() {
        var store = new WalRefStore(new MemoryObjectStorage(), CLOCK, 4);
        assertThrows(GitStoreException.class, () -> store.push("../x", PushCommand.of(
                REF, null, "Ada Lovelace", "m", Map.of())));
        assertThrows(GitStoreException.class, () -> store.push(REPO, PushCommand.of(
                "main", null, "Ada Lovelace", "m", Map.of())));
        assertThrows(GitStoreException.class, () -> store.push(REPO, PushCommand.of(
                REF, null, "Ada Lovelace", "m", Map.of("../x", "no".getBytes(StandardCharsets.UTF_8)))));
    }

    private static PushOutcome.Ok ok(WalRefStore store, String expected, String message, Map<String, byte[]> files) {
        PushOutcome outcome = store.push(REPO, PushCommand.of(REF, expected, "Ada Lovelace", message, files));
        return assertInstanceOf(PushOutcome.Ok.class, outcome);
    }
}
