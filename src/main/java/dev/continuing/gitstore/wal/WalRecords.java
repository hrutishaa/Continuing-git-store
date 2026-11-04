package dev.continuing.gitstore.wal;

import java.util.List;
import java.util.Map;

public final class WalRecords {
    private WalRecords() {}

    /** One accepted push. {@code expected} is the ref value this entry was based on. */
    public record WalEntry(
            long seq,
            String ref,
            String expected,
            String commit,
            List<String> objects,
            String author,
            String message,
            long timeMillis
    ) {}

    /** Materialized ref map after entry {@code seq}. Derived from the log; the log wins if it is missing. */
    public record IndexEntry(long seq, Map<String, String> refs) {}
}
