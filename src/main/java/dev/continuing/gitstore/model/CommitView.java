package dev.continuing.gitstore.model;

import java.util.List;

public record CommitView(
        String hash,
        List<String> parents,
        String author,
        long timeMillis,
        String message,
        String tree
) {
    public CommitView {
        parents = parents == null ? List.of() : List.copyOf(parents);
    }
}
