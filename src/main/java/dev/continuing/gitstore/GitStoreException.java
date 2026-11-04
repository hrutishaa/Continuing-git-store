package dev.continuing.gitstore;

/**
 * A failure the HTTP layer can turn into a status code.
 * Conflict between concurrent pushes is not an exception: it is a {@code PushOutcome.Conflict}.
 */
public final class GitStoreException extends RuntimeException {
    private final int httpStatus;

    public GitStoreException(int httpStatus, String message) {
        super(message);
        this.httpStatus = httpStatus;
    }

    public GitStoreException(int httpStatus, String message, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
    }

    public int httpStatus() {
        return httpStatus;
    }

    public static GitStoreException badRequest(String message) {
        return new GitStoreException(400, message);
    }

    public static GitStoreException notFound(String message) {
        return new GitStoreException(404, message);
    }

    public static GitStoreException busy(String message) {
        return new GitStoreException(503, message);
    }

    public static GitStoreException corrupt(String message) {
        return new GitStoreException(500, message);
    }

    public static GitStoreException unavailable(String message) {
        return new GitStoreException(503, message);
    }
}
