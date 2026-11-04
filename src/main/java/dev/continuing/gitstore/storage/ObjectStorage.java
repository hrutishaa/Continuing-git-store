package dev.continuing.gitstore.storage;

import java.util.Optional;

/**
 * The small slice of S3 this project depends on.
 * Conditional puts are the whole consistency story: {@code If-None-Match: *} creates a key
 * only when it is absent, so two callers cannot both win the same WAL slot.
 */
public interface ObjectStorage {
    /**
     * @param ifNoneMatch when non-null, reject the write if the current etag matches.
     *                    {@code *} rejects the write when the key already exists.
     * @param ifMatch     when non-null, reject the write unless the current etag matches.
     *                    Ignored when {@code ifNoneMatch} is also set.
     */
    PutResult put(String key, byte[] body, String ifNoneMatch, String ifMatch);

    Optional<StoredBlob> get(String key);

    Optional<String> head(String key);

    void delete(String key);
}
