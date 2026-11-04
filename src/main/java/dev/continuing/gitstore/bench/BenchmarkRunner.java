package dev.continuing.gitstore.bench;

import dev.continuing.gitstore.Main;
import dev.continuing.gitstore.consensus.ThreePhaseCommitStore;
import dev.continuing.gitstore.http.NodeServer;
import dev.continuing.gitstore.json.Jsons;
import dev.continuing.gitstore.model.PushCommand;
import dev.continuing.gitstore.model.PushOutcome;
import dev.continuing.gitstore.storage.CountingObjectStorage;
import dev.continuing.gitstore.storage.LatencyObjectStorage;
import dev.continuing.gitstore.storage.MemoryObjectStorage;
import dev.continuing.gitstore.storage.S3Config;
import dev.continuing.gitstore.storage.S3ObjectStorage;
import dev.continuing.gitstore.store.HistoryCheck;
import dev.continuing.gitstore.store.RepositoryStore;
import dev.continuing.gitstore.wal.WalRefStore;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Prints a single measured run. Rates are {@code successes / elapsed}, timed with
 * {@link System#nanoTime()}. Nothing here is copied from a blog post or filled in by hand.
 */
public final class BenchmarkRunner {
    static final String REF = "refs/heads/main";

    private BenchmarkRunner() {}

    public record Plan(
            int warmup,
            int sequential,
            int parallelThreads,
            int parallelEach,
            int singleFlight,
            int retryThreads,
            int retryEach,
            int latencyPushes,
            long putDelayMs,
            long getDelayMs,
            int httpPushes,
            int consensusFast,
            int consensusDelayPushes,
            long consensusDelayMs,
            int consensusTailPushes,
            long consensusTailFastMs,
            long consensusTailSlowMs,
            int consensusFlight
    ) {
        public static Plan full() {
            return new Plan(
                    200,
                    1500,
                    8, 150,
                    32,
                    8, 25,
                    40, 3, 1,
                    200,
                    300,
                    40, 5,
                    25, 5, 25,
                    16);
        }

        public static Plan mini() {
            return new Plan(
                    10,
                    20,
                    4, 5,
                    8,
                    4, 4,
                    5, 1, 0,
                    10,
                    15,
                    5, 2,
                    4, 1, 5,
                    8);
        }
    }

    public record MinioTarget(
            String endpoint,
            String region,
            String bucket,
            String accessKey,
            String secretKey,
            int sequential,
            int retryThreads,
            int retryEach
    ) {}

    public static int run(String[] args) {
        Map<String, String> flags = Main.flags(args);
        MinioTarget minio = null;
        if (flags.containsKey("minio")) {
            minio = new MinioTarget(
                    flags.getOrDefault("endpoint", env("GITSTORE_MINIO_ENDPOINT", "http://127.0.0.1:9000")),
                    flags.getOrDefault("region", env("GITSTORE_MINIO_REGION", "us-east-1")),
                    flags.getOrDefault("bucket", env("GITSTORE_MINIO_BUCKET", "gitstore-bench")),
                    flags.getOrDefault("access-key", env("GITSTORE_MINIO_ACCESS_KEY", "minioadmin")),
                    flags.getOrDefault("secret-key", env("GITSTORE_MINIO_SECRET_KEY", "minioadmin")),
                    30,
                    4,
                    5);
        }
        return run(Plan.full(), System.out, minio);
    }

    public static int run(Plan plan, Appendable out, MinioTarget minio) {
        List<Scenario> scenarios = new ArrayList<>();
        try {
            scenarios.add(warmup(plan.warmup()));
            scenarios.add(walSequential("wal_memory_sequential", plan.sequential(), 0, 0));
            scenarios.add(walParallelRefs(plan.parallelThreads(), plan.parallelEach()));
            scenarios.add(singleFlight("wal_memory_same_ref_single_flight", false, plan.singleFlight(), 0));
            scenarios.add(walRetry(plan.retryThreads(), plan.retryEach(), 0, 0));
            scenarios.add(walSequential(
                    "wal_memory_put" + plan.putDelayMs() + "ms_get" + plan.getDelayMs() + "ms_sequential",
                    plan.latencyPushes,
                    plan.putDelayMs(),
                    plan.getDelayMs()));
            scenarios.add(httpNodes(plan.httpPushes()));
            scenarios.add(consensusSequential(
                    "consensus_3pc_delay0_sequential",
                    plan.consensusFast,
                    new long[] {0, 0, 0}));
            scenarios.add(consensusSequential(
                    "consensus_3pc_delay" + plan.consensusDelayMs() + "ms_sequential",
                    plan.consensusDelayPushes,
                    new long[] {plan.consensusDelayMs(), plan.consensusDelayMs(), plan.consensusDelayMs()}));
            scenarios.add(consensusSequential(
                    "consensus_3pc_tail_" + plan.consensusTailFastMs() + "_"
                            + plan.consensusTailFastMs() + "_" + plan.consensusTailSlowMs() + "ms_sequential",
                    plan.consensusTailPushes,
                    new long[] {plan.consensusTailFastMs(), plan.consensusTailFastMs(), plan.consensusTailSlowMs()}));
            scenarios.add(singleFlight(
                    "consensus_3pc_same_ref_single_flight", true, plan.consensusFlight, 0));
            if (minio != null) {
                scenarios.add(minioRun(minio));
            }
        } catch (RuntimeException e) {
            scenarios.add(Scenario.failed("runner", e));
        }
        try {
            writeReport(out, scenarios);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        boolean failed = scenarios.stream().anyMatch(Scenario::failed);
        return failed ? 1 : 0;
    }

    private static Scenario warmup(int pushes) {
        var storage = new MemoryObjectStorage();
        var store = new WalRefStore(storage);
        long start = System.nanoTime();
        int ok = pushChain(store, "warm", pushes);
        double elapsed = millisSince(start);
        var fields = base("warmup_wal_memory_not_a_result", 1, pushes, ok, 0, 0, elapsed);
        fields.put("note", "discarded from comparisons; it only exercises the JIT");
        HistoryCheck.Report history = HistoryCheck.oneRef(store, "warm", REF, ok);
        fields.put("history_ok", Boolean.toString(history.ok()));
        fields.put("chain_length", Integer.toString(history.chainLength()));
        return new Scenario(fields, history.ok() && ok == pushes ? List.of() : List.of(history.detail()));
    }

    private static Scenario walSequential(String name, int pushes, long putDelayMs, long getDelayMs) {
        var counting = new CountingObjectStorage(new LatencyObjectStorage(new MemoryObjectStorage(), putDelayMs, getDelayMs));
        var store = new WalRefStore(counting);
        long start = System.nanoTime();
        int ok = pushChain(store, "seq", pushes);
        double elapsed = millisSince(start);
        var fields = base(name, 1, pushes, ok, 0, 0, elapsed);
        storageFields(fields, counting);
        fields.put("wal_append_losses", Long.toString(store.stats().appendLosses()));
        fields.put("injected_put_delay_ms", Long.toString(putDelayMs));
        fields.put("injected_get_delay_ms", Long.toString(getDelayMs));
        HistoryCheck.Report history = HistoryCheck.oneRef(store, "seq", REF, ok);
        fields.put("history_ok", Boolean.toString(history.ok()));
        fields.put("chain_length", Integer.toString(history.chainLength()));
        fields.put("head", String.valueOf(history.head()));
        List<String> failures = new ArrayList<>();
        if (ok != pushes) {
            failures.add("expected " + pushes + " successes");
        }
        if (!history.ok()) {
            failures.add(history.detail());
        }
        if (store.stats().appendLosses() != 0) {
            failures.add("single-threaded append lost a slot");
        }
        if (putDelayMs > 0) {
            double floor = counting.puts() * putDelayMs * 0.70;
            fields.put("delay_floor_ms", format(floor));
            if (elapsed < floor) {
                failures.add("elapsed " + elapsed + " ms is below the injected put delay floor " + floor);
            }
        }
        return new Scenario(fields, failures);
    }

    private static Scenario walParallelRefs(int threads, int each) {
        var counting = new CountingObjectStorage(new MemoryObjectStorage());
        var store = new WalRefStore(counting);
        Tally tally = parallel(threads, thread -> {
            String ref = "refs/heads/b" + thread;
            String expected = null;
            int localOk = 0;
            int localConflict = 0;
            int localError = 0;
            for (int n = 0; n < each; n++) {
                byte[] body = ("ref " + thread + " n " + n).getBytes(StandardCharsets.UTF_8);
                var command = PushCommand.of(ref, expected, "bench", "t" + thread + "-" + n, Map.of("note.txt", body));
                try {
                    switch (store.push("parallel", command)) {
                        case PushOutcome.Ok ok -> {
                            localOk++;
                            expected = ok.commit();
                        }
                        case PushOutcome.Conflict ignored -> localConflict++;
                    }
                } catch (RuntimeException e) {
                    localError++;
                }
            }
            return new int[] {each, localOk, localConflict, localError};
        });
        var fields = base("wal_memory_parallel_refs", threads, tally.attempts, tally.successes, tally.conflicts, tally.errors, tally.elapsedMs);
        storageFields(fields, counting);
        fields.put("wal_append_losses", Long.toString(store.stats().appendLosses()));
        List<String> failures = new ArrayList<>();
        if (tally.errors != 0 || tally.conflicts != 0 || tally.successes != threads * each) {
            failures.add("distinct refs should all commit without client conflicts");
        }
        boolean histories = true;
        for (int thread = 0; thread < threads; thread++) {
            HistoryCheck.Report history = HistoryCheck.oneRef(store, "parallel", "refs/heads/b" + thread, each);
            histories &= history.ok();
            if (!history.ok()) {
                failures.add("refs/heads/b" + thread + " " + history.detail());
            }
        }
        fields.put("history_ok", Boolean.toString(histories));
        var second = new WalRefStore(counting);
        boolean sameView = second.fetch("parallel").equals(store.fetch("parallel"));
        fields.put("second_store_sees_same_snapshot", Boolean.toString(sameView));
        if (!sameView) {
            failures.add("a second stateless store did not see the same snapshot");
        }
        return new Scenario(fields, failures);
    }

    private static Scenario walRetry(int threads, int each, long putDelayMs, long getDelayMs) {
        var counting = new CountingObjectStorage(new LatencyObjectStorage(new MemoryObjectStorage(), putDelayMs, getDelayMs));
        return retry("wal_memory_same_ref_retry", counting, threads, each);
    }

    private static Scenario retry(String name, CountingObjectStorage counting, int threads, int each) {
        var store = new WalRefStore(counting);
        Tally tally = parallel(threads, thread -> {
            int localOk = 0;
            int localConflict = 0;
            int localError = 0;
            int attempts = 0;
            for (int n = 0; n < each; n++) {
                byte[] body = ("t" + thread + "-" + n).getBytes(StandardCharsets.UTF_8);
                String message = "t" + thread + "-" + n;
                boolean landed = false;
                for (int attempt = 0; attempt < 200 && !landed; attempt++) {
                    attempts++;
                    String expected = store.fetch("retry").commit(REF);
                    var command = PushCommand.of(REF, expected, "bench", message, Map.of("note.txt", body));
                    try {
                        switch (store.push("retry", command)) {
                            case PushOutcome.Ok ignored -> {
                                localOk++;
                                landed = true;
                            }
                            case PushOutcome.Conflict ignored -> localConflict++;
                        }
                    } catch (RuntimeException e) {
                        break;
                    }
                }
                if (!landed) {
                    localError++;
                }
            }
            return new int[] {attempts, localOk, localConflict, localError};
        });
        var fields = base(name, threads, tally.attempts, tally.successes, tally.conflicts, tally.errors, tally.elapsedMs);
        storageFields(fields, counting);
        fields.put("wal_append_losses", Long.toString(store.stats().appendLosses()));
        fields.put("client_conflicts_from_store", Long.toString(store.stats().clientConflicts()));
        HistoryCheck.Report history = HistoryCheck.oneRef(store, "retry", REF, threads * each);
        fields.put("history_ok", Boolean.toString(history.ok()));
        fields.put("chain_length", Integer.toString(history.chainLength()));
        fields.put("head", String.valueOf(history.head()));
        List<String> failures = new ArrayList<>();
        if (tally.successes != threads * each || tally.errors != 0 || !history.ok()) {
            failures.add(history.detail() == null ? "retry did not land every push" : history.detail());
        }
        var reader = new WalRefStore(counting);
        if (!reader.fetch("retry").equals(store.fetch("retry"))) {
            failures.add("second store diverged");
            fields.put("second_store_sees_same_snapshot", "false");
        } else {
            fields.put("second_store_sees_same_snapshot", "true");
        }
        return new Scenario(fields, failures);
    }

    private static Scenario singleFlight(String name, boolean consensus, int threads, long delayMs) {
        RepositoryStore store;
        ThreePhaseCommitStore cluster = null;
        CountingObjectStorage counting = null;
        if (consensus) {
            cluster = new ThreePhaseCommitStore(java.time.Clock.systemUTC(), delayMs, delayMs, delayMs);
            store = cluster;
        } else {
            counting = new CountingObjectStorage(new MemoryObjectStorage());
            store = new WalRefStore(counting);
        }
        try {
            Tally tally = parallel(threads, thread -> {
                byte[] body = ("flight " + thread).getBytes(StandardCharsets.UTF_8);
                var command = PushCommand.of(REF, null, "bench", "create-" + thread, Map.of("note.txt", body));
                try {
                    return switch (store.push("flight", command)) {
                        case PushOutcome.Ok ignored -> new int[] {1, 1, 0, 0};
                        case PushOutcome.Conflict ignored -> new int[] {1, 0, 1, 0};
                    };
                } catch (RuntimeException e) {
                    return new int[] {1, 0, 0, 1};
                }
            });
            var fields = base(name, threads, tally.attempts, tally.successes, tally.conflicts, tally.errors, tally.elapsedMs);
            if (counting != null) {
                storageFields(fields, counting);
                fields.put("wal_append_losses", Long.toString(((WalRefStore) store).stats().appendLosses()));
            }
            if (cluster != null) {
                consensusFields(fields, cluster);
            }
            HistoryCheck.Report history = HistoryCheck.oneRef(store, "flight", REF, 1);
            fields.put("history_ok", Boolean.toString(history.ok()));
            fields.put("chain_length", Integer.toString(history.chainLength()));
            fields.put("head", String.valueOf(history.head()));
            boolean singleWinner = tally.successes == 1 && tally.conflicts == threads - 1 && tally.errors == 0;
            fields.put("invariant_single_winner", Boolean.toString(singleWinner));
            List<String> failures = new ArrayList<>();
            if (!singleWinner) {
                failures.add("expected exactly one winner and " + (threads - 1) + " conflicts");
            }
            if (!history.ok()) {
                failures.add(history.detail());
            }
            if (cluster != null && !cluster.healthyAgree("flight")) {
                failures.add("replicas diverged");
            }
            return new Scenario(fields, failures);
        } finally {
            if (cluster != null) {
                cluster.close();
            }
        }
    }

    private static Scenario consensusSequential(String name, int pushes, long[] delays) {
        var cluster = new ThreePhaseCommitStore(java.time.Clock.systemUTC(), delays);
        try {
            long start = System.nanoTime();
            int ok = pushChain(cluster, "cseq", pushes);
            double elapsed = millisSince(start);
            var fields = base(name, 1, pushes, ok, 0, 0, elapsed);
            consensusFields(fields, cluster);
            long slowest = 0;
            for (long delay : delays) {
                slowest = Math.max(slowest, delay);
            }
            fields.put("replica_delays_ms", delays[0] + "," + delays[1] + "," + delays[2]);
            HistoryCheck.Report history = HistoryCheck.oneRef(cluster, "cseq", REF, ok);
            fields.put("history_ok", Boolean.toString(history.ok()));
            fields.put("chain_length", Integer.toString(history.chainLength()));
            fields.put("replicas_agree", Boolean.toString(cluster.healthyAgree("cseq")));
            List<String> failures = new ArrayList<>();
            if (ok != pushes || !history.ok() || !cluster.healthyAgree("cseq")) {
                failures.add(history.detail());
            }
            if (slowest > 0) {
                double floor = pushes * 3.0 * slowest * 0.70;
                fields.put("delay_floor_ms", format(floor));
                fields.put("delay_floor_meaning", "0.70 * pushes * 3 phases * slowest replica sleep");
                if (elapsed < floor) {
                    failures.add("elapsed below injected 3PC delay floor");
                }
            }
            return new Scenario(fields, failures);
        } finally {
            cluster.close();
        }
    }

    private static Scenario httpNodes(int pushes) {
        var storage = new MemoryObjectStorage();
        List<NodeServer> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            nodes.add(NodeServer.start(0, "node-" + (i + 1), storage, "memory"));
        }
        var client = HttpClient.newHttpClient();
        try {
            int ok = 0;
            int conflict = 0;
            int error = 0;
            String expected = null;
            long start = System.nanoTime();
            for (int i = 0; i < pushes; i++) {
                NodeServer node = nodes.get(i % nodes.size());
                String json = pushJson(expected, "http-" + i, "n=" + i);
                HttpResponse<String> response = client.send(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + node.port() + "/v1/repos/httpdemo/push"))
                                .header("content-type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(json))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 201) {
                    ok++;
                    expected = Jsons.MAPPER.readTree(response.body()).get("commit").asText();
                } else if (response.statusCode() == 409) {
                    conflict++;
                } else {
                    error++;
                }
            }
            double elapsed = millisSince(start);
            var fields = base("wal_http_three_nodes_round_robin", 1, pushes, ok, conflict, error, elapsed);
            List<String> heads = new ArrayList<>();
            for (NodeServer node : nodes) {
                HttpResponse<String> response = client.send(
                        HttpRequest.newBuilder(URI.create(
                                        "http://127.0.0.1:" + node.port() + "/v1/repos/httpdemo/files?ref=" + REF))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                heads.add(response.statusCode() + ":" + Jsons.MAPPER.readTree(response.body()).path("commit").asText());
            }
            boolean agree = heads.stream().distinct().count() == 1 && heads.get(0).startsWith("200:");
            fields.put("nodes_agree", Boolean.toString(agree));
            fields.put("node_heads", String.join(" ", heads));
            var reader = new WalRefStore(storage);
            HistoryCheck.Report history = HistoryCheck.oneRef(reader, "httpdemo", REF, ok);
            fields.put("history_ok", Boolean.toString(history.ok()));
            fields.put("chain_length", Integer.toString(history.chainLength()));
            List<String> failures = new ArrayList<>();
            if (ok != pushes || conflict != 0 || error != 0 || !agree || !history.ok()) {
                failures.add(history.ok() ? "http nodes disagreed or a push failed" : history.detail());
            }
            return new Scenario(fields, failures);
        } catch (Exception e) {
            return Scenario.failed("wal_http_three_nodes_round_robin", e);
        } finally {
            nodes.forEach(NodeServer::close);
        }
    }

    private static Scenario minioRun(MinioTarget target) {
        S3ObjectStorage s3;
        try {
            s3 = new S3ObjectStorage(new S3Config(
                    target.endpoint(), target.region(), target.bucket(), target.accessKey(), target.secretKey()));
        } catch (RuntimeException e) {
            return Scenario.failed("minio", e);
        }
        try {
            var counting = new CountingObjectStorage(s3);
            String repo = "m" + Long.toString(System.nanoTime(), 36);
            var writer = new WalRefStore(counting);
            var reader = new WalRefStore(s3);
            long start = System.nanoTime();
            int ok = pushChain(writer, repo, target.sequential());
            double elapsed = millisSince(start);
            var fields = base("minio_sequential", 1, target.sequential(), ok, 0, 0, elapsed);
            storageFields(fields, counting);
            fields.put("endpoint", target.endpoint());
            fields.put("bucket", target.bucket());
            HistoryCheck.Report history = HistoryCheck.oneRef(reader, repo, REF, ok);
            fields.put("history_ok", Boolean.toString(history.ok()));
            fields.put("chain_length", Integer.toString(history.chainLength()));
            fields.put("reader_is_a_second_store", "true");
            fields.put("wal_append_losses", Long.toString(writer.stats().appendLosses()));
            List<String> failures = new ArrayList<>();
            if (ok != target.sequential() || !history.ok() || writer.stats().appendLosses() != 0) {
                failures.add(history.detail());
            }
            Scenario sequential = new Scenario(fields, failures);
            counting.reset();
            String contended = repo + "c";
            var contendedStore = new WalRefStore(counting);
            Tally tally = parallel(target.retryThreads(), thread -> {
                int localOk = 0;
                int localConflict = 0;
                int localError = 0;
                int attempts = 0;
                for (int n = 0; n < target.retryEach(); n++) {
                    boolean landed = false;
                    byte[] body = ("minio t" + thread + " n" + n).getBytes(StandardCharsets.UTF_8);
                    for (int attempt = 0; attempt < 100 && !landed; attempt++) {
                        attempts++;
                        String expected = contendedStore.fetch(contended).commit(REF);
                        var command = PushCommand.of(
                                REF, expected, "bench", "t" + thread + "-" + n, Map.of("note.txt", body));
                        try {
                            switch (contendedStore.push(contended, command)) {
                                case PushOutcome.Ok ignored -> {
                                    localOk++;
                                    landed = true;
                                }
                                case PushOutcome.Conflict ignored -> localConflict++;
                            }
                        } catch (RuntimeException e) {
                            break;
                        }
                    }
                    if (!landed) {
                        localError++;
                    }
                }
                return new int[] {attempts, localOk, localConflict, localError};
            });
            var contendedFields = base(
                    "minio_same_ref_retry",
                    target.retryThreads(),
                    tally.attempts,
                    tally.successes,
                    tally.conflicts,
                    tally.errors,
                    tally.elapsedMs);
            storageFields(contendedFields, counting);
            HistoryCheck.Report contendedHistory = HistoryCheck.oneRef(
                    reader, contended, REF, target.retryThreads() * target.retryEach());
            contendedFields.put("history_ok", Boolean.toString(contendedHistory.ok()));
            contendedFields.put("chain_length", Integer.toString(contendedHistory.chainLength()));
            contendedFields.put("endpoint", target.endpoint());
            List<String> contendedFailures = new ArrayList<>();
            if (tally.successes != target.retryThreads() * target.retryEach()
                    || tally.errors != 0
                    || !contendedHistory.ok()) {
                contendedFailures.add(contendedHistory.detail());
            }
            return sequential.plus(new Scenario(contendedFields, contendedFailures));
        } catch (RuntimeException e) {
            return Scenario.failed("minio", e);
        } finally {
            s3.close();
        }
    }

    private static int pushChain(RepositoryStore store, String repo, int pushes) {
        String expected = null;
        int ok = 0;
        for (int i = 0; i < pushes; i++) {
            byte[] body = ("n=" + i).getBytes(StandardCharsets.UTF_8);
            var command = PushCommand.of(REF, expected, "bench", "n=" + i, Map.of("note.txt", body));
            if (store.push(repo, command) instanceof PushOutcome.Ok committed) {
                ok++;
                expected = committed.commit();
            }
        }
        return ok;
    }

    private static Tally parallel(int threads, java.util.function.IntFunction<int[]> work) {
        var barrier = new CyclicBarrier(threads);
        var done = new CountDownLatch(threads);
        var attempts = new LongAdder();
        var successes = new LongAdder();
        var conflicts = new LongAdder();
        var errors = new LongAdder();
        var errorsText = new ConcurrentLinkedQueue<String>();
        var earliest = new AtomicLong(Long.MAX_VALUE);
        var latest = new AtomicLong(0);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                int thread = t;
                pool.submit(() -> {
                    try {
                        barrier.await(60, TimeUnit.SECONDS);
                        earliest.accumulateAndGet(System.nanoTime(), Math::min);
                        int[] counts = work.apply(thread);
                        attempts.add(counts[0]);
                        successes.add(counts[1]);
                        conflicts.add(counts[2]);
                        errors.add(counts[3]);
                    } catch (Exception e) {
                        errors.increment();
                        if (errorsText.size() < 3) {
                            errorsText.add(e.getClass().getSimpleName() + ": " + e.getMessage());
                        }
                    } finally {
                        latest.accumulateAndGet(System.nanoTime(), Math::max);
                        done.countDown();
                    }
                });
            }
            if (!done.await(5, TimeUnit.MINUTES)) {
                errors.increment();
                errorsText.add("workers did not finish");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            errors.increment();
        } finally {
            pool.shutdownNow();
        }
        long span = latest.get() - earliest.get();
        if (span < 0) {
            span = 0;
        }
        return new Tally(
                attempts.intValue(),
                successes.intValue(),
                conflicts.intValue(),
                errors.intValue(),
                span / 1_000_000.0,
                String.join("; ", errorsText));
    }

    private static String pushJson(String expected, String message, String body) {
        var node = Jsons.MAPPER.createObjectNode();
        node.put("ref", REF);
        if (expected == null) {
            node.putNull("expected");
        } else {
            node.put("expected", expected);
        }
        node.put("author", "bench");
        node.put("message", message);
        node.putObject("files").put("note.txt", body);
        return node.toString();
    }

    private static Map<String, String> base(
            String name, int threads, int attempts, int successes, int conflicts, int errors, double elapsedMs
    ) {
        var fields = new LinkedHashMap<String, String>();
        fields.put("scenario", name);
        fields.put("threads", Integer.toString(threads));
        fields.put("attempts", Integer.toString(attempts));
        fields.put("successes", Integer.toString(successes));
        fields.put("conflicts", Integer.toString(conflicts));
        fields.put("errors", Integer.toString(errors));
        fields.put("elapsed_ms", format(elapsedMs));
        if (elapsedMs > 0) {
            fields.put("pushes_per_sec", format(successes * 1000.0 / elapsedMs));
            fields.put("attempts_per_sec", format(attempts * 1000.0 / elapsedMs));
        } else {
            fields.put("pushes_per_sec", "n/a");
            fields.put("attempts_per_sec", "n/a");
        }
        return fields;
    }

    private static void storageFields(Map<String, String> fields, CountingObjectStorage counting) {
        fields.put("storage_puts", Long.toString(counting.puts()));
        fields.put("storage_gets", Long.toString(counting.gets()));
        fields.put("storage_heads", Long.toString(counting.heads()));
        fields.put("conditional_put_rejections", Long.toString(counting.rejectedPuts()));
    }

    private static void consensusFields(Map<String, String> fields, ThreePhaseCommitStore cluster) {
        ThreePhaseCommitStore.Stats stats = cluster.stats();
        fields.put("consensus_commits", Long.toString(stats.commits()));
        fields.put("consensus_client_conflicts", Long.toString(stats.clientConflicts()));
        fields.put("consensus_retries", Long.toString(stats.retries()));
        long phases = Math.max(1, stats.commits() + stats.retries());
        fields.put("mean_vote_ms", format(stats.voteNanos() / 1_000_000.0 / phases));
        fields.put("mean_precommit_ms", format(stats.preCommitNanos() / 1_000_000.0 / phases));
        fields.put("mean_docommit_ms", format(stats.doCommitNanos() / 1_000_000.0 / phases));
        fields.put("replicas_agree", Boolean.toString(cluster.healthyAgree(
                fields.get("scenario").contains("flight") ? "flight" : "cseq")));
    }

    private static void writeReport(Appendable out, List<Scenario> scenarios) throws IOException {
        out.append("CONTINUING GIT STORE BENCHMARK\n");
        out.append("source=BenchmarkRunner in this repository, one process, one run\n");
        out.append("clock=System.nanoTime\n");
        out.append("rate=successes * 1000 / elapsed_ms\n");
        out.append("these numbers were measured here; they are not taken from the Cursor blog\n");
        out.append("timestamp=").append(Instant.now().toString()).append('\n');
        out.append("java.version=").append(System.getProperty("java.version")).append('\n');
        out.append("java.vendor=").append(System.getProperty("java.vendor")).append('\n');
        out.append("os.name=").append(System.getProperty("os.name")).append('\n');
        out.append("os.arch=").append(System.getProperty("os.arch")).append('\n');
        out.append("os.version=").append(System.getProperty("os.version")).append('\n');
        out.append("availableProcessors=").append(Integer.toString(Runtime.getRuntime().availableProcessors())).append('\n');
        appendProc(out);
        out.append('\n');
        List<Scenario> flat = new ArrayList<>();
        for (Scenario scenario : scenarios) {
            flat.addAll(scenario.flatten());
        }
        for (Scenario scenario : flat) {
            out.append('[').append(scenario.fields.get("scenario")).append("]\n");
            for (var entry : scenario.fields.entrySet()) {
                if (!"scenario".equals(entry.getKey())) {
                    out.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
                }
            }
            if (!scenario.failures.isEmpty()) {
                out.append("FAILED=").append(String.join(" | ", scenario.failures)).append('\n');
            }
            out.append('\n');
        }
        Double wal = rate(flat, "wal_memory_sequential");
        Double fast = rate(flat, "consensus_3pc_delay0_sequential");
        Double delayed = flat.stream()
                .filter(s -> s.fields.get("scenario").startsWith("consensus_3pc_delay")
                        && !s.fields.get("scenario").contains("delay0"))
                .map(s -> parseRate(s.fields.get("pushes_per_sec")))
                .filter(v -> v != null)
                .findFirst()
                .orElse(null);
        out.append("[derived_from_the_rates_above]\n");
        if (wal != null && fast != null && fast > 0) {
            out.append("wal_sequential_over_consensus_delay0=")
                    .append(format(wal / fast)).append('\n');
        }
        if (wal != null && delayed != null && delayed > 0) {
            out.append("wal_sequential_over_consensus_with_injected_delay=")
                    .append(format(wal / delayed)).append('\n');
        }
        out.append("derivation=ratio of the pushes_per_sec values printed above\n");
    }

    private static void appendProc(Appendable out) throws IOException {
        try {
            for (String line : Files.readAllLines(Path.of("/proc/cpuinfo"))) {
                if (line.startsWith("model name")) {
                    out.append("cpu.model=").append(line.substring(line.indexOf(':') + 1).trim()).append('\n');
                    break;
                }
            }
        } catch (IOException ignored) {
            // not Linux, or the file is unavailable
        }
        try {
            for (String line : Files.readAllLines(Path.of("/proc/meminfo"))) {
                if (line.startsWith("MemTotal:")) {
                    out.append("mem.total=").append(line.substring(line.indexOf(':') + 1).trim()).append('\n');
                    break;
                }
            }
        } catch (IOException ignored) {
            // not Linux, or the file is unavailable
        }
    }

    private static Double rate(List<Scenario> scenarios, String name) {
        return scenarios.stream()
                .filter(scenario -> name.equals(scenario.fields.get("scenario")))
                .map(scenario -> parseRate(scenario.fields.get("pushes_per_sec")))
                .findFirst()
                .orElse(null);
    }

    private static Double parseRate(String text) {
        if (text == null || "n/a".equals(text)) {
            return null;
        }
        return Double.valueOf(text);
    }

    private static double millisSince(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000.0;
    }

    private static String format(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private record Tally(int attempts, int successes, int conflicts, int errors, double elapsedMs, String errorText) {}

    private static final class Scenario {
        final Map<String, String> fields;
        final List<String> failures;
        final Scenario next;

        Scenario(Map<String, String> fields, List<String> failures) {
            this(fields, failures, null);
        }

        Scenario(Map<String, String> fields, List<String> failures, Scenario next) {
            this.fields = fields;
            this.failures = List.copyOf(failures);
            this.next = next;
        }

        static Scenario failed(String name, Exception e) {
            var fields = new LinkedHashMap<String, String>();
            fields.put("scenario", name);
            fields.put("errors", "1");
            fields.put("history_ok", "false");
            return new Scenario(fields, List.of(e.getClass().getSimpleName() + ": " + e.getMessage()));
        }

        Scenario plus(Scenario more) {
            return new Scenario(fields, failures, more);
        }

        boolean failed() {
            return !failures.isEmpty() || (next != null && next.failed());
        }

        List<Scenario> flatten() {
            List<Scenario> out = new ArrayList<>();
            Scenario cursor = this;
            while (cursor != null) {
                out.add(new Scenario(cursor.fields, cursor.failures));
                cursor = cursor.next;
            }
            return out;
        }
    }
}
