package dev.continuing.gitstore.consensus;

import dev.continuing.gitstore.GitStoreException;
import dev.continuing.gitstore.model.PushCommand;
import dev.continuing.gitstore.model.PushOutcome;
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreePhaseCommitStoreTest {
    private static final String REPO = "demo";
    private static final String REF = "refs/heads/main";
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void commitReachesEveryReplica() {
        try (var store = ThreePhaseCommitStore.quorumOfThree(CLOCK)) {
            PushOutcome.Ok ok = push(store, null, "hello", Map.of("README.md", "hello".getBytes(StandardCharsets.UTF_8)));
            assertEquals(ok.commit(), store.fetch(REPO).commit(REF));
            assertTrue(store.healthyAgree(REPO));
            for (ThreePhaseCommitStore.Replica replica : store.replicas()) {
                assertEquals(ok.commit(), replica.snapshot(REPO).commit(REF));
                assertEquals("hello", new String(
                        store.files(REPO, REF).get("README.md"), StandardCharsets.UTF_8));
            }
            assertTrue(HistoryCheck.oneRef(store, REPO, REF, 1).ok());
        }
    }

    @Test
    void staleExpectedConflictsAndTheNextPushCanStillCommit() {
        try (var store = ThreePhaseCommitStore.quorumOfThree(CLOCK)) {
            PushOutcome.Ok first = push(store, null, "one", Map.of("a.txt", "A".getBytes(StandardCharsets.UTF_8)));
            PushOutcome again = store.push(REPO, PushCommand.of(
                    REF, null, "Ada Lovelace", "nope", Map.of("a.txt", "B".getBytes(StandardCharsets.UTF_8))));
            assertInstanceOf(PushOutcome.Conflict.class, again);
            PushOutcome.Ok second = push(store, first.commit(), "two", Map.of("b.txt", "B".getBytes(StandardCharsets.UTF_8)));
            assertEquals(second.commit(), store.fetch(REPO).commit(REF));
            assertEquals(2, store.files(REPO, REF).size());
            assertTrue(store.healthyAgree(REPO));
        }
    }

    @Test
    void oneDownReplicaStillCommitsOnTheQuorumAndCanCatchUp() {
        try (var store = ThreePhaseCommitStore.quorumOfThree(CLOCK)) {
            store.replicas().get(2).setHealthy(false);
            PushOutcome.Ok ok = push(store, null, "one", Map.of("a.txt", "A".getBytes(StandardCharsets.UTF_8)));
            assertEquals(0, store.replicas().get(2).snapshot(REPO).seq());
            assertEquals(ok.commit(), store.replicas().get(0).snapshot(REPO).commit(REF));
            assertEquals(ok.commit(), store.replicas().get(1).snapshot(REPO).commit(REF));
            assertTrue(store.healthyAgree(REPO));
            store.catchUp(REPO, "r3");
            store.replicas().get(2).setHealthy(true);
            assertTrue(store.healthyAgree(REPO));
            push(store, ok.commit(), "two", Map.of("b.txt", "B".getBytes(StandardCharsets.UTF_8)));
            assertTrue(store.healthyAgree(REPO));
            assertEquals(2, store.replicas().get(2).snapshot(REPO).seq());
        }
    }

    @Test
    void twoDownReplicasRefuseThePush() {
        try (var store = ThreePhaseCommitStore.quorumOfThree(CLOCK)) {
            store.replicas().get(1).setHealthy(false);
            store.replicas().get(2).setHealthy(false);
            GitStoreException error = assertThrows(GitStoreException.class, () -> push(
                    store, null, "nope", Map.of("a.txt", "A".getBytes(StandardCharsets.UTF_8))));
            assertEquals(503, error.httpStatus());
            assertNull(store.replicas().get(0).snapshot(REPO).commit(REF));
        }
    }

    @Test
    void preCommitSurvivesACoordinatorCrash() {
        try (var store = ThreePhaseCommitStore.quorumOfThree(CLOCK)) {
            store.setPhaseHook((phase, txId) -> {
                if (phase == ThreePhaseCommitStore.Phase.PRE_COMMIT) {
                    throw new IllegalStateException("coordinator stopped");
                }
            });
            assertThrows(IllegalStateException.class, () -> push(
                    store, null, "half", Map.of("a.txt", "A".getBytes(StandardCharsets.UTF_8))));
            assertNull(store.fetch(REPO).commit(REF));
            store.setPhaseHook(null);
            store.recover();
            assertTrue(store.fetch(REPO).commit(REF) != null);
            assertTrue(store.healthyAgree(REPO));
            push(store, store.fetch(REPO).commit(REF), "next", Map.of("b.txt", "B".getBytes(StandardCharsets.UTF_8)));
            assertEquals(2, store.fetch(REPO).seq());
        }
    }

    @Test
    void voteWithoutPreCommitRollsBackOnRecovery() {
        try (var store = ThreePhaseCommitStore.quorumOfThree(CLOCK)) {
            store.setPhaseHook((phase, txId) -> {
                if (phase == ThreePhaseCommitStore.Phase.VOTE) {
                    throw new IllegalStateException("died during vote");
                }
            });
            assertThrows(IllegalStateException.class, () -> push(
                    store, null, "half", Map.of("a.txt", "A".getBytes(StandardCharsets.UTF_8))));
            store.setPhaseHook(null);
            store.recover();
            assertNull(store.fetch(REPO).commit(REF));
            PushOutcome.Ok ok = push(store, null, "after", Map.of("a.txt", "A".getBytes(StandardCharsets.UTF_8)));
            assertEquals(ok.commit(), store.fetch(REPO).commit(REF));
        }
    }

    @Test
    void slowReplicaSetsThePhaseTime() {
        try (var store = new ThreePhaseCommitStore(CLOCK, 0, 0, 40)) {
            long start = System.nanoTime();
            push(store, null, "slow", Map.of("a.txt", "A".getBytes(StandardCharsets.UTF_8)));
            double elapsedMs = (System.nanoTime() - start) / 1_000_000.0;
            assertTrue(elapsedMs >= 90, "elapsed " + elapsedMs + " ms, want the 3 * 40 ms tail");
            assertTrue(store.healthyAgree(REPO));
        }
    }

    @Test
    void concurrentCreatorsHaveASingleWinner() throws Exception {
        try (var store = ThreePhaseCommitStore.quorumOfThree(CLOCK)) {
            int threads = 12;
            var start = new CyclicBarrier(threads);
            try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
                List<Future<PushOutcome>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    int n = i;
                    futures.add(pool.submit(() -> {
                        start.await(10, TimeUnit.SECONDS);
                        return store.push(REPO, PushCommand.of(
                                REF,
                                null,
                                "Ada Lovelace",
                                "m" + n,
                                Map.of("f.txt", ("c" + n).getBytes(StandardCharsets.UTF_8))));
                    }));
                }
                int wins = 0;
                for (Future<PushOutcome> future : futures) {
                    if (future.get(30, TimeUnit.SECONDS) instanceof PushOutcome.Ok) {
                        wins++;
                    }
                }
                assertEquals(1, wins);
            }
            assertTrue(store.healthyAgree(REPO));
            assertTrue(HistoryCheck.oneRef(store, REPO, REF, 1).ok());
        }
    }

    private static PushOutcome.Ok push(
            ThreePhaseCommitStore store, String expected, String message, Map<String, byte[]> files
    ) {
        PushOutcome outcome = store.push(REPO, PushCommand.of(REF, expected, "Ada Lovelace", message, files));
        return assertInstanceOf(PushOutcome.Ok.class, outcome);
    }
}
