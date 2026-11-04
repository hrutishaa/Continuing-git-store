package dev.continuing.gitstore.model;

/** Result of one push attempt. A conflict leaves the ref unchanged. */
public sealed interface PushOutcome permits PushOutcome.Ok, PushOutcome.Conflict {
    record Ok(long seq, String commit, String ref) implements PushOutcome {}

    record Conflict(String ref, String expected, String actual) implements PushOutcome {}
}
