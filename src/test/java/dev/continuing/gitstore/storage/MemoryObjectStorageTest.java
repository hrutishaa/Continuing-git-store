package dev.continuing.gitstore.storage;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemoryObjectStorageTest {
    @Test
    void ifNoneMatchStarLetsOnlyTheFirstCreateWin() {
        var storage = new MemoryObjectStorage();
        byte[] first = "one".getBytes(StandardCharsets.UTF_8);
        byte[] second = "two".getBytes(StandardCharsets.UTF_8);
        PutResult created = storage.put("wal/1", first, "*", null);
        PutResult raced = storage.put("wal/1", second, "*", null);
        assertTrue(created.applied());
        assertFalse(raced.applied());
        assertArrayEquals(first, storage.get("wal/1").orElseThrow().body());
        assertEquals(created.etag(), storage.head("wal/1").orElseThrow());
    }

    @Test
    void ifMatchRejectsAStaleEtagAndAcceptsTheCurrentOne() {
        var storage = new MemoryObjectStorage();
        PutResult first = storage.put("tip", "1".getBytes(StandardCharsets.UTF_8), null, null);
        PutResult stale = storage.put("tip", "2".getBytes(StandardCharsets.UTF_8), null, "\"deadbeef\"");
        assertFalse(stale.applied());
        PutResult fresh = storage.put("tip", "2".getBytes(StandardCharsets.UTF_8), null, first.etag());
        assertTrue(fresh.applied());
        assertArrayEquals("2".getBytes(StandardCharsets.UTF_8), storage.get("tip").orElseThrow().body());
    }

    @Test
    void unconditionalPutOverwrites() {
        var storage = new MemoryObjectStorage();
        storage.put("tip", "1".getBytes(StandardCharsets.UTF_8), null, null);
        storage.put("tip", "3".getBytes(StandardCharsets.UTF_8), null, null);
        assertArrayEquals("3".getBytes(StandardCharsets.UTF_8), storage.get("tip").orElseThrow().body());
    }

    @Test
    void deleteRemovesTheKey() {
        var storage = new MemoryObjectStorage();
        storage.put("k", new byte[] {1}, "*", null);
        storage.delete("k");
        assertTrue(storage.get("k").isEmpty());
        assertTrue(storage.put("k", new byte[] {2}, "*", null).applied());
    }
}
