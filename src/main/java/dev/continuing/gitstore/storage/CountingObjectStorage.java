package dev.continuing.gitstore.storage;

import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

/** Counts backend calls so a benchmark can show how many conditional puts a push costs. */
public final class CountingObjectStorage implements ObjectStorage {
    private final ObjectStorage delegate;
    private final LongAdder puts = new LongAdder();
    private final LongAdder gets = new LongAdder();
    private final LongAdder heads = new LongAdder();
    private final LongAdder deletes = new LongAdder();
    private final LongAdder rejectedPuts = new LongAdder();

    public CountingObjectStorage(ObjectStorage delegate) {
        this.delegate = delegate;
    }

    @Override
    public PutResult put(String key, byte[] body, String ifNoneMatch, String ifMatch) {
        puts.increment();
        PutResult result = delegate.put(key, body, ifNoneMatch, ifMatch);
        if (!result.applied()) {
            rejectedPuts.increment();
        }
        return result;
    }

    @Override
    public Optional<StoredBlob> get(String key) {
        gets.increment();
        return delegate.get(key);
    }

    @Override
    public Optional<String> head(String key) {
        heads.increment();
        return delegate.head(key);
    }

    @Override
    public void delete(String key) {
        deletes.increment();
        delegate.delete(key);
    }

    public long puts() {
        return puts.sum();
    }

    public long gets() {
        return gets.sum();
    }

    public long heads() {
        return heads.sum();
    }

    public long deletes() {
        return deletes.sum();
    }

    public long rejectedPuts() {
        return rejectedPuts.sum();
    }

    public void reset() {
        puts.reset();
        gets.reset();
        heads.reset();
        deletes.reset();
        rejectedPuts.reset();
    }
}
