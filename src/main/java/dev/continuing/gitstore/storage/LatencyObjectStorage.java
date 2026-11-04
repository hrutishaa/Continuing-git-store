package dev.continuing.gitstore.storage;

import java.util.Optional;

/**
 * Injects sleep before each call so a benchmark can show what a round trip to object storage costs.
 * The sleep is the injected delay itself; it is not a guess at Amazon S3's latency.
 */
public final class LatencyObjectStorage implements ObjectStorage {
    private final ObjectStorage delegate;
    private final long putDelayMillis;
    private final long getDelayMillis;

    public LatencyObjectStorage(ObjectStorage delegate, long putDelayMillis, long getDelayMillis) {
        this.delegate = delegate;
        this.putDelayMillis = putDelayMillis;
        this.getDelayMillis = getDelayMillis;
    }

    @Override
    public PutResult put(String key, byte[] body, String ifNoneMatch, String ifMatch) {
        pause(putDelayMillis);
        return delegate.put(key, body, ifNoneMatch, ifMatch);
    }

    @Override
    public Optional<StoredBlob> get(String key) {
        pause(getDelayMillis);
        return delegate.get(key);
    }

    @Override
    public Optional<String> head(String key) {
        pause(getDelayMillis);
        return delegate.head(key);
    }

    @Override
    public void delete(String key) {
        pause(putDelayMillis);
        delegate.delete(key);
    }

    private static void pause(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("latency sleep interrupted", e);
        }
    }
}
