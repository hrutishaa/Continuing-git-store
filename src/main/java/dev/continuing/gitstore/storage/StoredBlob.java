package dev.continuing.gitstore.storage;

/** Bytes stored under one key, plus the etag the backend would return. */
public record StoredBlob(byte[] body, String etag) {}
