package dev.continuing.gitstore.storage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process stand-in for S3. {@code putIfAbsent} is the atomic conditional create,
 * which is the same rule S3 implements for {@code If-None-Match: *}.
 */
public final class MemoryObjectStorage implements ObjectStorage {
    private final ConcurrentHashMap<String, StoredBlob> objects = new ConcurrentHashMap<>();

    @Override
    public PutResult put(String key, byte[] body, String ifNoneMatch, String ifMatch) {
        byte[] copy = Arrays.copyOf(body, body.length);
        StoredBlob next = new StoredBlob(copy, etag(copy));
        if (ifNoneMatch == null && ifMatch == null) {
            objects.put(key, next);
            return PutResult.applied(next.etag());
        }
        if ("*".equals(ifNoneMatch)) {
            StoredBlob prev = objects.putIfAbsent(key, next);
            if (prev != null) {
                return PutResult.rejected(prev.etag());
            }
            return PutResult.applied(next.etag());
        }
        while (true) {
            StoredBlob current = objects.get(key);
            if (ifNoneMatch != null) {
                if (current != null && etagEquals(current.etag(), ifNoneMatch)) {
                    return PutResult.rejected(current.etag());
                }
                if (current == null) {
                    if (objects.putIfAbsent(key, next) == null) {
                        return PutResult.applied(next.etag());
                    }
                    continue;
                }
                if (objects.replace(key, current, next)) {
                    return PutResult.applied(next.etag());
                }
                continue;
            }
            if (current == null || !etagEquals(current.etag(), ifMatch)) {
                return PutResult.rejected(current == null ? null : current.etag());
            }
            if (objects.replace(key, current, next)) {
                return PutResult.applied(next.etag());
            }
        }
    }

    @Override
    public Optional<StoredBlob> get(String key) {
        StoredBlob current = objects.get(key);
        if (current == null) {
            return Optional.empty();
        }
        return Optional.of(new StoredBlob(Arrays.copyOf(current.body(), current.body().length), current.etag()));
    }

    @Override
    public Optional<String> head(String key) {
        StoredBlob current = objects.get(key);
        return current == null ? Optional.empty() : Optional.of(current.etag());
    }

    @Override
    public void delete(String key) {
        objects.remove(key);
    }

    public int size() {
        return objects.size();
    }

    static boolean etagEquals(String left, String right) {
        return normalize(left).equals(normalize(right));
    }

    static String normalize(String etag) {
        if (etag == null) {
            return "";
        }
        String trimmed = etag.trim();
        if (trimmed.startsWith("W/")) {
            trimmed = trimmed.substring(2).trim();
        }
        if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static String etag(byte[] body) {
        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(body);
            return "\"" + HexFormat.of().formatHex(digest) + "\"";
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
