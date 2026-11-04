package dev.continuing.gitstore.model;

import dev.continuing.gitstore.GitStoreException;
import dev.continuing.gitstore.Names;

import java.util.List;
import java.util.Map;

/**
 * A push is a compare-and-swap on one ref plus a tree upsert.
 * {@code expected} is the commit the client last observed ({@code null} to create the ref).
 * When {@code force} is set, the store still linearizes the update, but it retries against
 * whatever the ref currently points at instead of failing the client's stale {@code expected}.
 */
public record PushCommand(
        String ref,
        String expected,
        String author,
        String message,
        Map<String, byte[]> upserts,
        List<String> deletePaths,
        boolean force
) {
    public PushCommand {
        upserts = upserts == null ? Map.of() : Map.copyOf(upserts);
        deletePaths = deletePaths == null ? List.of() : List.copyOf(deletePaths);
        if (message == null) {
            message = "";
        }
    }

    public static PushCommand of(
            String ref,
            String expected,
            String author,
            String message,
            Map<String, byte[]> files
    ) {
        return new PushCommand(ref, expected, author, message, files, List.of(), false);
    }

    public void validate() {
        Names.requireRef(ref);
        Names.requireExpected(expected);
        Names.requireAuthor(author);
        if (message.length() > 16_384) {
            throw GitStoreException.badRequest("message too long");
        }
        if (upserts.size() + deletePaths.size() > 200) {
            throw GitStoreException.badRequest("too many path updates");
        }
        for (var entry : upserts.entrySet()) {
            Names.requirePath(entry.getKey());
            if (entry.getValue() == null) {
                throw GitStoreException.badRequest("null file content");
            }
            if (entry.getValue().length > 1_048_576) {
                throw GitStoreException.badRequest("file too large: " + entry.getKey());
            }
        }
        for (String path : deletePaths) {
            Names.requirePath(path);
        }
    }
}
