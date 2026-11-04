package dev.continuing.gitstore.storage;

public record PutResult(boolean applied, String etag) {
    public static PutResult applied(String etag) {
        return new PutResult(true, etag);
    }

    public static PutResult rejected(String etag) {
        return new PutResult(false, etag);
    }
}
