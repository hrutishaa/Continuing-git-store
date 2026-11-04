package dev.continuing.gitstore.wal;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.continuing.gitstore.GitStoreException;
import dev.continuing.gitstore.Names;
import dev.continuing.gitstore.json.Jsons;
import dev.continuing.gitstore.model.GitObjects;
import dev.continuing.gitstore.model.PushCommand;
import dev.continuing.gitstore.model.PushOutcome;
import dev.continuing.gitstore.model.RepoSnapshot;
import dev.continuing.gitstore.storage.ObjectStorage;
import dev.continuing.gitstore.storage.PutResult;
import dev.continuing.gitstore.storage.StoredBlob;
import dev.continuing.gitstore.store.CommitBuilder;
import dev.continuing.gitstore.store.RepositoryStore;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Git refs advanced by appending a write-ahead log to object storage.
 *
 * <p>The linearization point is a single conditional create: {@code PUT wal/{seq}} with
 * {@code If-None-Match: *}. Exactly one caller can create sequence N. That caller computed
 * N from the current tip and based the ref update on that tip, so the winner's entry is a
 * valid successor. A loser gets a precondition failure, re-reads, and either conflicts
 * (the ref moved) or tries the next slot (some other ref won the race).
 *
 * <p>Objects are written before the WAL entry. The index and tip pointer are caches.
 * A reader that finds a missing index replays the log. A stale tip is probed forward,
 * and a tip that points past the log is walked backward. Any node can do this; none of
 * them keep the repository.
 */
public final class WalRefStore implements RepositoryStore {
    private final ObjectStorage storage;
    private final Clock clock;
    private final int maxAttempts;
    private final AtomicLong appendWins = new AtomicLong();
    private final AtomicLong appendLosses = new AtomicLong();
    private final AtomicLong clientConflicts = new AtomicLong();
    private volatile BeforeAppend beforeAppend = (repo, seq) -> {};

    /** Test seam: runs after the snapshot is read and before the conditional WAL put. */
    @FunctionalInterface
    public interface BeforeAppend {
        void onBeforeAppend(String repo, long seq) throws Exception;
    }

    public void setBeforeAppend(BeforeAppend beforeAppend) {
        this.beforeAppend = beforeAppend == null ? (repo, seq) -> {} : beforeAppend;
    }

    public WalRefStore(ObjectStorage storage) {
        this(storage, Clock.systemUTC(), 128);
    }

    public WalRefStore(ObjectStorage storage, Clock clock, int maxAttempts) {
        this.storage = Objects.requireNonNull(storage);
        this.clock = Objects.requireNonNull(clock);
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts");
        }
        this.maxAttempts = maxAttempts;
    }

    public record Stats(long appendWins, long appendLosses, long clientConflicts) {}

    public Stats stats() {
        return new Stats(appendWins.get(), appendLosses.get(), clientConflicts.get());
    }

    public void resetStats() {
        appendWins.set(0);
        appendLosses.set(0);
        clientConflicts.set(0);
    }

    @Override
    public PushOutcome push(String repo, PushCommand command) {
        Names.requireRepo(repo);
        command.validate();
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            Snapshot snap = readSnapshot(repo);
            String actual = snap.refs.get(command.ref());
            if (!command.force() && !Objects.equals(actual, command.expected())) {
                clientConflicts.incrementAndGet();
                return new PushOutcome.Conflict(command.ref(), command.expected(), actual);
            }
            long now = clock.millis();
            CommitBuilder.Built built = CommitBuilder.build(
                    hash -> readObject(repo, hash), actual, command, now);
            for (GitObjects.Raw object : built.objects()) {
                storage.put(WalLayout.object(repo, object.hash()), object.stored(), "*", null);
            }
            long next = snap.seq + 1;
            List<String> hashes = built.objects().stream().map(GitObjects.Raw::hash).toList();
            var entry = new WalRecords.WalEntry(
                    next,
                    command.ref(),
                    actual,
                    built.commit().hash(),
                    hashes,
                    command.author(),
                    command.message(),
                    now);
            try {
                beforeAppend.onBeforeAppend(repo, next);
            } catch (Exception e) {
                throw GitStoreException.unavailable("append hook failed: " + e.getMessage());
            }
            PutResult written = storage.put(WalLayout.wal(repo, next), encode(entry), "*", null);
            if (!written.applied()) {
                appendLosses.incrementAndGet();
                continue;
            }
            appendWins.incrementAndGet();
            Snapshot published = apply(snap, entry);
            try {
                storage.put(WalLayout.index(repo, next), encode(new WalRecords.IndexEntry(next, published.refs)), "*", null);
                storage.put(WalLayout.tip(repo), Long.toString(next).getBytes(StandardCharsets.UTF_8), null, null);
            } catch (RuntimeException e) {
                System.getLogger("gitstore").log(
                        System.Logger.Level.WARNING,
                        "WAL entry " + next + " is durable; index or tip update failed",
                        e);
            }
            return new PushOutcome.Ok(next, built.commit().hash(), command.ref());
        }
        throw GitStoreException.busy("lost the WAL append race " + maxAttempts + " times");
    }

    @Override
    public RepoSnapshot fetch(String repo) {
        Names.requireRepo(repo);
        return view(repo, readSnapshot(repo));
    }

    @Override
    public Optional<byte[]> readObject(String repo, String hash) {
        Names.requireRepo(repo);
        Names.requireHash(hash);
        Optional<StoredBlob> stored = storage.get(WalLayout.object(repo, hash));
        if (stored.isEmpty()) {
            return Optional.empty();
        }
        String actual = GitObjects.hash(stored.get().body());
        if (!actual.equals(hash)) {
            throw GitStoreException.corrupt("object " + hash + " hashes to " + actual);
        }
        return Optional.of(stored.get().body());
    }

    /**
     * Replays the log from sequence 1, rewriting index snapshots and the tip.
     * Used when the cache objects were removed; the WAL entries are left untouched.
     */
    public RepoSnapshot rebuild(String repo) {
        Names.requireRepo(repo);
        long seq = 0;
        while (storage.head(WalLayout.wal(repo, seq + 1)).isPresent()) {
            seq++;
        }
        Snapshot snap = Snapshot.EMPTY;
        for (long i = 1; i <= seq; i++) {
            snap = apply(snap, readWal(repo, i));
            storage.put(WalLayout.index(repo, i), encode(new WalRecords.IndexEntry(snap.seq, snap.refs)), "*", null);
        }
        storage.put(WalLayout.tip(repo), Long.toString(seq).getBytes(StandardCharsets.UTF_8), null, null);
        return view(repo, snap);
    }

    private Snapshot readSnapshot(String repo) {
        long seq = findSeq(repo);
        if (seq == 0) {
            return Snapshot.EMPTY;
        }
        Optional<StoredBlob> exact = storage.get(WalLayout.index(repo, seq));
        if (exact.isPresent()) {
            WalRecords.IndexEntry index = decode(exact.get().body(), WalRecords.IndexEntry.class);
            if (index.seq() != seq) {
                throw GitStoreException.corrupt("index " + seq + " claims seq " + index.seq());
            }
            return new Snapshot(seq, copyRefs(index.refs()));
        }
        Snapshot snap = Snapshot.EMPTY;
        for (long s = seq - 1; s >= 1; s--) {
            Optional<StoredBlob> earlier = storage.get(WalLayout.index(repo, s));
            if (earlier.isPresent()) {
                WalRecords.IndexEntry index = decode(earlier.get().body(), WalRecords.IndexEntry.class);
                snap = new Snapshot(index.seq(), copyRefs(index.refs()));
                break;
            }
        }
        for (long i = snap.seq + 1; i <= seq; i++) {
            snap = apply(snap, readWal(repo, i));
        }
        return snap;
    }

    private long findSeq(String repo) {
        long seq = readTip(repo);
        while (seq > 0 && storage.head(WalLayout.wal(repo, seq)).isEmpty()) {
            seq--;
        }
        while (storage.head(WalLayout.wal(repo, seq + 1)).isPresent()) {
            seq++;
        }
        return seq;
    }

    private long readTip(String repo) {
        Optional<StoredBlob> tip = storage.get(WalLayout.tip(repo));
        if (tip.isEmpty()) {
            return 0;
        }
        String text = new String(tip.get().body(), StandardCharsets.UTF_8).trim();
        if (text.isEmpty()) {
            return 0;
        }
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private WalRecords.WalEntry readWal(String repo, long seq) {
        StoredBlob blob = storage.get(WalLayout.wal(repo, seq))
                .orElseThrow(() -> GitStoreException.corrupt("missing wal " + seq));
        WalRecords.WalEntry entry = decode(blob.body(), WalRecords.WalEntry.class);
        if (entry.seq() != seq) {
            throw GitStoreException.corrupt("wal key " + seq + " contains seq " + entry.seq());
        }
        return entry;
    }

    private static Snapshot apply(Snapshot snap, WalRecords.WalEntry entry) {
        if (entry.seq() != snap.seq + 1) {
            throw GitStoreException.corrupt("wal seq " + entry.seq() + " does not follow " + snap.seq);
        }
        String current = snap.refs.get(entry.ref());
        if (!Objects.equals(current, entry.expected())) {
            throw GitStoreException.corrupt(
                    "wal " + entry.seq() + " expected ref " + entry.expected() + " but found " + current);
        }
        var refs = new TreeMap<>(snap.refs);
        refs.put(entry.ref(), entry.commit());
        return new Snapshot(entry.seq(), refs);
    }

    private static RepoSnapshot view(String repo, Snapshot snap) {
        return new RepoSnapshot(repo, snap.seq, snap.refs);
    }

    private static TreeMap<String, String> copyRefs(Map<String, String> refs) {
        return refs == null ? new TreeMap<>() : new TreeMap<>(refs);
    }

    private static byte[] encode(Object value) {
        try {
            return Jsons.MAPPER.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw GitStoreException.corrupt("could not encode wal record");
        }
    }

    private static <T> T decode(byte[] body, Class<T> type) {
        try {
            return Jsons.MAPPER.readValue(body, type);
        } catch (Exception e) {
            throw GitStoreException.corrupt("could not decode " + type.getSimpleName());
        }
    }

    private record Snapshot(long seq, TreeMap<String, String> refs) {
        static final Snapshot EMPTY = new Snapshot(0, new TreeMap<>());
    }
}
