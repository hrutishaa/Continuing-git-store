package dev.continuing.gitstore.wal;

import dev.continuing.gitstore.model.PushCommand;
import dev.continuing.gitstore.model.PushOutcome;
import dev.continuing.gitstore.storage.MemoryObjectStorage;
import dev.continuing.gitstore.store.HistoryCheck;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WalConcurrencyTest {
    private static final String REF = "refs/heads/main";
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void oneCreateWinsWhenTwoStoresShareTheLog() throws Exception {
        var storage = new MemoryObjectStorage();
        var first = new WalRefStore(storage, CLOCK, 32);
        var second = new WalRefStore(storage, CLOCK, 32);
        List<WalRefStore> stores = List.of(first, second);
        int threads = 24;
        var start = new CyclicBarrier(threads);
        var atAppend = new CyclicBarrier(threads);
        WalRefStore.BeforeAppend holdFirstSlot = (repo, seq) -> {
            if (seq == 1) {
                atAppend.await(10, TimeUnit.SECONDS);
            }
        };
        first.setBeforeAppend(holdFirstSlot);
        second.setBeforeAppend(holdFirstSlot);
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Future<PushOutcome>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int n = i;
                futures.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return stores.get(n % 2).push("repo", PushCommand.of(
                            REF,
                            null,
                            "Ada Lovelace",
                            "m" + n,
                            Map.of("f.txt", ("c" + n).getBytes(StandardCharsets.UTF_8))));
                }));
            }
            int wins = 0;
            int conflicts = 0;
            for (Future<PushOutcome> future : futures) {
                if (future.get(30, TimeUnit.SECONDS) instanceof PushOutcome.Ok) {
                    wins++;
                } else {
                    conflicts++;
                }
            }
            assertEquals(1, wins);
            assertEquals(threads - 1, conflicts);
        }
        assertEquals(1, first.fetch("repo").seq());
        assertEquals(first.fetch("repo"), second.fetch("repo"));
        assertTrue(HistoryCheck.oneRef(second, "repo", REF, 1).ok());
        long slotLosses = first.stats().appendLosses() + second.stats().appendLosses();
        assertEquals(threads - 1, slotLosses);
    }

    @Test
    void retriedPushesFromBothStoresFormOneHistory() throws Exception {
        var storage = new MemoryObjectStorage();
        var first = new WalRefStore(storage, CLOCK, 64);
        var second = new WalRefStore(storage, CLOCK, 64);
        List<WalRefStore> stores = List.of(first, second);
        int threads = 8;
        int each = 8;
        var start = new CyclicBarrier(threads);
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int thread = i;
                futures.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    WalRefStore store = stores.get(thread % 2);
                    for (int n = 0; n < each; n++) {
                        boolean landed = false;
                        byte[] body = ("t" + thread + "-" + n).getBytes(StandardCharsets.UTF_8);
                        for (int attempt = 0; attempt < 100 && !landed; attempt++) {
                            String expected = store.fetch("repo").commit(REF);
                            PushOutcome outcome = store.push("repo", PushCommand.of(
                                    REF, expected, "Ada Lovelace", "t" + thread + "-" + n, Map.of("note.txt", body)));
                            landed = outcome instanceof PushOutcome.Ok;
                        }
                        if (!landed) {
                            throw new IllegalStateException("thread " + thread + " did not land push " + n);
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        }
        HistoryCheck.Report history = HistoryCheck.oneRef(second, "repo", REF, threads * each);
        assertTrue(history.ok(), history.detail());
        assertEquals(first.fetch("repo"), second.fetch("repo"));
        assertEquals(threads * each, first.fetch("repo").seq());
    }

    @Test
    void distinctRefsDoNotConflict() throws Exception {
        var storage = new MemoryObjectStorage();
        var store = new WalRefStore(storage, CLOCK, 64);
        int threads = 8;
        int each = 6;
        var start = new CyclicBarrier(threads);
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int thread = i;
                futures.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    String ref = "refs/heads/b" + thread;
                    String expected = null;
                    int conflicts = 0;
                    for (int n = 0; n < each; n++) {
                        PushOutcome outcome = store.push("repo", PushCommand.of(
                                ref,
                                expected,
                                "Ada Lovelace",
                                "n" + n,
                                Map.of("f.txt", ("t" + thread + "-" + n).getBytes(StandardCharsets.UTF_8))));
                        if (outcome instanceof PushOutcome.Ok ok) {
                            expected = ok.commit();
                        } else {
                            conflicts++;
                        }
                    }
                    return conflicts;
                }));
            }
            for (Future<Integer> future : futures) {
                assertEquals(0, future.get(30, TimeUnit.SECONDS));
            }
        }
        for (int thread = 0; thread < threads; thread++) {
            HistoryCheck.Report history = HistoryCheck.oneRef(store, "repo", "refs/heads/b" + thread, each);
            assertTrue(history.ok(), history.detail());
        }
        assertEquals(threads * each, store.fetch("repo").seq());
    }
}
