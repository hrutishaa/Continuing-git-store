package dev.continuing.gitstore;

/** Validation for names that become object-storage keys or Git refs. */
public final class Names {
    private Names() {}

    public static void requireRepo(String repo) {
        if (repo == null || !repo.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")) {
            throw GitStoreException.badRequest(
                    "repo name must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}");
        }
    }

    public static void requireRef(String ref) {
        if (ref == null
                || ref.contains("..")
                || ref.contains("//")
                || !ref.matches("refs/(heads|tags)/[A-Za-z0-9][A-Za-z0-9._/-]{0,200}")) {
            throw GitStoreException.badRequest("ref must look like refs/heads/main");
        }
    }

    public static void requireHash(String hash) {
        if (hash == null || !hash.matches("[0-9a-f]{64}")) {
            throw GitStoreException.badRequest("object hash must be 64 lowercase hex characters");
        }
    }

    public static void requirePath(String path) {
        if (path == null || path.isBlank() || path.length() > 512
                || path.startsWith("/") || path.contains("\\") || path.contains("\0")) {
            throw GitStoreException.badRequest("illegal path");
        }
        for (String segment : path.split("/")) {
            if (segment.isEmpty() || ".".equals(segment) || "..".equals(segment)) {
                throw GitStoreException.badRequest("illegal path");
            }
        }
    }

    public static void requireAuthor(String author) {
        if (author == null || author.isBlank() || author.length() > 120
                || author.indexOf('\n') >= 0 || author.indexOf('\r') >= 0) {
            throw GitStoreException.badRequest("author is required and must be a single line");
        }
    }

    /** {@code null} means "the ref must not exist yet". Any other value must be a commit hash. */
    public static void requireExpected(String expected) {
        if (expected != null && !expected.matches("[0-9a-f]{64}")) {
            throw GitStoreException.badRequest("expected must be null or a commit hash");
        }
    }
}
