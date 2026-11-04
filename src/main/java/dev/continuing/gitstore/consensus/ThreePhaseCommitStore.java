package dev.continuing.gitstore.consensus;

import dev.continuing.gitstore.GitStoreException;
import dev.continuing.gitstore.Names;
import dev.continuing.gitstore.model.GitObjects;
import dev.continuing.gitstore.model.PushCommand;
import dev.continuing.gitstore.model.PushOutcome;
import dev.continuing.gitstore.model.RepoSnapshot;
import dev.continuing.gitstore.store.CommitBuilder;
import dev.continuing.gitstore.store.RepositoryStore;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * A teaching model of the GitHub Spokes shape: three replicas, and a push is accepted
 * only after a three-phase commit (vote, pre-commit, do-commit) reaches every live replica.
 * The live set must still be a majority of the configured replicas.
 *
 * <p>Each phase fans out in parallel and waits for the slowest replica, which is the
 * tail-latency cost this design pays as replicas are added or one of them stalls.
 * Message delay is an injected sleep, not a real network. There is no separate object
 * store: each replica keeps its own copy, and that copy is the source of truth.
 * That is the operational contrast with the WAL store, where S3 is the source of truth
 * and a node can disappear without a repair protocol.
 */
public final class ThreePhaseCommitStore implements RepositoryStore, AutoCloseable {
    public enum Phase { VOTE, PRE_COMMIT, DO_COMMIT }

    public interface PhaseHook {
        void after(Phase phase, String txId);
    }

    private final List<Replica> replicas;
    private final Clock clock;
    private final int maxAttempts;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ConcurrentHashMap<String, Object> refLocks = new ConcurrentHashMap<>();
    private volatile PhaseHook hook = (phase, txId) -> {};

    private final LongAdder commits = new LongAdder();
    private final LongAdder clientConflicts = new LongAdder();
    private final LongAdder retries = new LongAdder();
    private final LongAdder voteNanos = new LongAdder();
    private final LongAdder preCommitNanos = new LongAdder();
    private final LongAdder doCommitNanos = new LongAdder();

    public ThreePhaseCommitStore(Clock clock, long... replicaDelayMillis) {
        if (replicaDelayMillis.length < 1) {
            throw new IllegalArgumentException("at least one replica");
        }
        this.clock = Objects.requireNonNull(clock);
        this.maxAttempts = 32;
        List<Replica> created = new ArrayList<>();
        for (int i = 0; i < replicaDelayMillis.length; i++) {
            created.add(new Replica("r" + (i + 1), replicaDelayMillis[i]));
        }
        this.replicas = List.copyOf(created);
    }

    public static ThreePhaseCommitStore quorumOfThree(Clock clock) {
        return new ThreePhaseCommitStore(clock, 0, 0, 0);
    }

    public void setPhaseHook(PhaseHook hook) {
        this.hook = hook == null ? (phase, txId) -> {} : hook;
    }

    public List<Replica> replicas() {
        return replicas;
    }

    public record Stats(
            long commits,
            long clientConflicts,
            long retries,
            long voteNanos,
            long preCommitNanos,
            long doCommitNanos
    ) {}

    public Stats stats() {
        return new Stats(
                commits.sum(),
                clientConflicts.sum(),
                retries.sum(),
                voteNanos.sum(),
                preCommitNanos.sum(),
                doCommitNanos.sum());
    }

    public void resetStats() {
        commits.reset();
        clientConflicts.reset();
        retries.reset();
        voteNanos.reset();
        preCommitNanos.reset();
        doCommitNanos.reset();
    }

    @Override
    public PushOutcome push(String repo, PushCommand command) {
        Names.requireRepo(repo);
        command.validate();
        Object lock = refLocks.computeIfAbsent(repo + "\0" + command.ref(), key -> new Object());
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            if (attempt > 0) {
                retries.increment();
            }
            synchronized (lock) {
                List<Replica> live = liveReplicas();
                if (live.size() < majority()) {
                    throw GitStoreException.unavailable(
                            "only " + live.size() + " of " + replicas.size() + " replicas are up");
                }
                Replica primary = live.get(0);
                String actual = primary.ref(repo, command.ref());
                if (!command.force() && !Objects.equals(actual, command.expected())) {
                    clientConflicts.increment();
                    return new PushOutcome.Conflict(command.ref(), command.expected(), actual);
                }
                CommitBuilder.Built built = CommitBuilder.build(
                        hash -> primary.readObject(repo, hash),
                        actual,
                        command,
                        clock.millis());
                var objects = new LinkedHashMap<String, byte[]>();
                for (GitObjects.Raw raw : built.objects()) {
                    objects.put(raw.hash(), raw.stored());
                }
                String txId = UUID.randomUUID().toString();
                String commitHash = built.commit().hash();
                List<Vote> votes = time(voteNanos, () -> fanout(live, replica ->
                        replica.vote(txId, repo, command.ref(), actual, commitHash, objects)));
                hook.after(Phase.VOTE, txId);
                long yes = votes.stream().filter(vote -> vote.decision == Decision.YES).count();
                boolean conflict = votes.stream().anyMatch(vote -> vote.decision == Decision.CONFLICT);
                if (yes != live.size()) {
                    fanout(live, replica -> {
                        replica.abort(txId);
                        return Boolean.TRUE;
                    });
                    if (conflict && !command.force()) {
                        String observed = votes.stream()
                                .filter(vote -> vote.decision == Decision.CONFLICT)
                                .map(Vote::actual)
                                .findFirst()
                                .orElse(actual);
                        clientConflicts.increment();
                        return new PushOutcome.Conflict(command.ref(), command.expected(), observed);
                    }
                    continue;
                }
                time(preCommitNanos, () -> fanout(live, replica -> {
                    replica.preCommit(txId);
                    return Boolean.TRUE;
                }));
                hook.after(Phase.PRE_COMMIT, txId);
                time(doCommitNanos, () -> fanout(live, replica -> {
                    replica.doCommit(txId);
                    return Boolean.TRUE;
                }));
                hook.after(Phase.DO_COMMIT, txId);
                commits.increment();
                long seq = primary.snapshot(repo).seq();
                return new PushOutcome.Ok(seq, commitHash, command.ref());
            }
        }
        throw GitStoreException.busy("3PC did not finish in " + maxAttempts + " attempts");
    }

    @Override
    public RepoSnapshot fetch(String repo) {
        Names.requireRepo(repo);
        List<Replica> live = liveReplicas();
        if (live.isEmpty()) {
            throw GitStoreException.unavailable("no replicas are up");
        }
        return live.get(0).snapshot(repo);
    }

    @Override
    public Optional<byte[]> readObject(String repo, String hash) {
        Names.requireRepo(repo);
        Names.requireHash(hash);
        List<Replica> live = liveReplicas();
        if (live.isEmpty()) {
            throw GitStoreException.unavailable("no replicas are up");
        }
        return live.get(0).readObject(repo, hash);
    }

    /** Finish any pre-committed transaction whose coordinator stopped before do-commit. */
    public void recover() {
        for (Replica replica : replicas) {
            replica.recover();
        }
    }

    /** Copy one repository from a live replica onto {@code targetId}, which may be down. */
    public void catchUp(String repo, String targetId) {
        Names.requireRepo(repo);
        Replica target = find(targetId);
        Replica source = liveReplicas().stream()
                .filter(replica -> !replica.id.equals(targetId))
                .findFirst()
                .orElseThrow(() -> GitStoreException.unavailable("no live source for catch-up"));
        target.replace(repo, source.copy(repo));
    }

    public boolean healthyAgree(String repo) {
        List<String> digests = liveReplicas().stream().map(replica -> replica.digest(repo)).distinct().toList();
        return digests.size() <= 1;
    }

    private Replica find(String id) {
        return replicas.stream()
                .filter(replica -> replica.id.equals(id))
                .findFirst()
                .orElseThrow(() -> GitStoreException.badRequest("unknown replica " + id));
    }

    private int majority() {
        return replicas.size() / 2 + 1;
    }

    private List<Replica> liveReplicas() {
        return replicas.stream()
                .filter(Replica::healthy)
                .sorted(Comparator.comparing(replica -> replica.id))
                .toList();
    }

    private <T> List<T> fanout(List<Replica> live, Function<Replica, T> call) {
        List<Future<T>> futures = new ArrayList<>();
        for (Replica replica : live) {
            futures.add(executor.submit(() -> call.apply(replica)));
        }
        List<T> out = new ArrayList<>(futures.size());
        for (Future<T> future : futures) {
            try {
                out.add(future.get());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException runtime) {
                    throw runtime;
                }
                if (cause instanceof Error error) {
                    throw error;
                }
                throw new IllegalStateException(cause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw GitStoreException.unavailable("interrupted during 3PC");
            }
        }
        return out;
    }

    private <T> T time(LongAdder nanos, java.util.function.Supplier<T> call) {
        long start = System.nanoTime();
        try {
            return call.get();
        } finally {
            nanos.add(System.nanoTime() - start);
        }
    }

    @Override
    public void close() {
        executor.close();
    }

    private enum Decision { YES, CONFLICT, LOCKED }

    private record Vote(Decision decision, String actual) {}

    public static final class Replica {
        private final String id;
        private volatile boolean healthy = true;
        private volatile long delayMillis;
        private final Object monitor = new Object();
        private final Map<String, RepoData> repos = new HashMap<>();
        private final Map<String, Staged> stagedByTx = new HashMap<>();
        private final Map<String, String> lockOwner = new HashMap<>();

        Replica(String id, long delayMillis) {
            this.id = id;
            this.delayMillis = delayMillis;
        }

        public String id() {
            return id;
        }

        public boolean healthy() {
            return healthy;
        }

        public void setHealthy(boolean healthy) {
            this.healthy = healthy;
        }

        public void setDelayMillis(long delayMillis) {
            this.delayMillis = delayMillis;
        }

        public long delayMillis() {
            return delayMillis;
        }

        String ref(String repo, String ref) {
            synchronized (monitor) {
                RepoData data = repos.get(repo);
                return data == null ? null : data.refs.get(ref);
            }
        }

        Optional<byte[]> readObject(String repo, String hash) {
            synchronized (monitor) {
                RepoData data = repos.get(repo);
                if (data == null) {
                    return Optional.empty();
                }
                byte[] body = data.objects.get(hash);
                return body == null ? Optional.empty() : Optional.of(Arrays.copyOf(body, body.length));
            }
        }

        public RepoSnapshot snapshot(String repo) {
            synchronized (monitor) {
                RepoData data = repos.get(repo);
                if (data == null) {
                    return new RepoSnapshot(repo, 0, Map.of());
                }
                return new RepoSnapshot(repo, data.seq, new TreeMap<>(data.refs));
            }
        }

        String digest(String repo) {
            synchronized (monitor) {
                RepoData data = repos.get(repo);
                var sb = new StringBuilder();
                if (data == null) {
                    return "0|";
                }
                sb.append(data.seq).append('|');
                data.refs.forEach((name, hash) -> sb.append(name).append('=').append(hash).append(';'));
                sb.append('|');
                data.objects.keySet().stream().sorted().forEach(hash ->
                        sb.append(hash).append('=').append(GitObjects.hash(data.objects.get(hash))).append(';'));
                return sb.toString();
            }
        }

        RepoData copy(String repo) {
            synchronized (monitor) {
                RepoData data = repos.get(repo);
                if (data == null) {
                    return new RepoData();
                }
                return data.copy();
            }
        }

        void replace(String repo, RepoData data) {
            synchronized (monitor) {
                repos.put(repo, data.copy());
            }
        }

        Vote vote(
                String txId,
                String repo,
                String ref,
                String observed,
                String commitHash,
                Map<String, byte[]> objects
        ) {
            delay();
            synchronized (monitor) {
                String lockKey = repo + "\0" + ref;
                String owner = lockOwner.get(lockKey);
                RepoData data = repos.computeIfAbsent(repo, key -> new RepoData());
                String current = data.refs.get(ref);
                if (owner != null && !owner.equals(txId)) {
                    return new Vote(Decision.LOCKED, current);
                }
                if (!Objects.equals(current, observed)) {
                    return new Vote(Decision.CONFLICT, current);
                }
                lockOwner.put(lockKey, txId);
                stagedByTx.put(txId, new Staged(txId, repo, ref, commitHash, objects));
                return new Vote(Decision.YES, current);
            }
        }

        void preCommit(String txId) {
            delay();
            synchronized (monitor) {
                Staged staged = stagedByTx.get(txId);
                if (staged != null) {
                    staged.precommitted = true;
                }
            }
        }

        void doCommit(String txId) {
            delay();
            synchronized (monitor) {
                applyAndUnlock(txId);
            }
        }

        void abort(String txId) {
            delay();
            synchronized (monitor) {
                Staged staged = stagedByTx.remove(txId);
                if (staged != null) {
                    lockOwner.remove(staged.repo + "\0" + staged.ref, txId);
                }
            }
        }

        void recover() {
            synchronized (monitor) {
                List<String> ids = new ArrayList<>(stagedByTx.keySet());
                for (String txId : ids) {
                    Staged staged = stagedByTx.get(txId);
                    if (staged != null && staged.precommitted) {
                        applyAndUnlock(txId);
                    } else {
                        Staged removed = stagedByTx.remove(txId);
                        if (removed != null) {
                            lockOwner.remove(removed.repo + "\0" + removed.ref, txId);
                        }
                    }
                }
            }
        }

        private void applyAndUnlock(String txId) {
            Staged staged = stagedByTx.remove(txId);
            if (staged == null) {
                return;
            }
            RepoData data = repos.computeIfAbsent(staged.repo, key -> new RepoData());
            data.objects.putAll(staged.objects);
            data.refs.put(staged.ref, staged.commitHash);
            data.seq++;
            lockOwner.remove(staged.repo + "\0" + staged.ref, txId);
        }

        private void delay() {
            long millis = delayMillis;
            if (millis <= 0) {
                return;
            }
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw GitStoreException.unavailable("replica delay interrupted");
            }
        }
    }

    private static final class Staged {
        final String txId;
        final String repo;
        final String ref;
        final String commitHash;
        final Map<String, byte[]> objects;
        boolean precommitted;

        Staged(String txId, String repo, String ref, String commitHash, Map<String, byte[]> objects) {
            this.txId = txId;
            this.repo = repo;
            this.ref = ref;
            this.commitHash = commitHash;
            this.objects = objects;
        }
    }

    private static final class RepoData {
        long seq;
        final Map<String, String> refs = new TreeMap<>();
        final Map<String, byte[]> objects = new HashMap<>();

        RepoData copy() {
            RepoData copy = new RepoData();
            copy.seq = seq;
            copy.refs.putAll(refs);
            objects.forEach((hash, body) -> copy.objects.put(hash, Arrays.copyOf(body, body.length)));
            return copy;
        }
    }
}
