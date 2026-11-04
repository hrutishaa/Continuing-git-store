package dev.continuing.gitstore.model;

import java.util.Map;

/** The published view of a repository: a monotonic sequence and the ref map it produced. */
public record RepoSnapshot(String repo, long seq, Map<String, String> refs) {
    public RepoSnapshot {
        refs = refs == null ? Map.of() : Map.copyOf(refs);
    }

    public String commit(String ref) {
        return refs.get(ref);
    }
}
