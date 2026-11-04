package dev.continuing.gitstore.store;

import dev.continuing.gitstore.model.CommitView;

import java.util.List;

/** Checks that a ref is one parent-linked chain and that every object on it can be read. */
public final class HistoryCheck {
    private HistoryCheck() {}

    public record Report(boolean ok, int chainLength, String head, String detail) {}

    public static Report oneRef(RepositoryStore store, String repo, String ref, int expectedCommits) {
        try {
            List<CommitView> log = store.log(repo, ref, Integer.MAX_VALUE);
            String head = log.isEmpty() ? null : log.get(0).hash();
            if (log.size() != expectedCommits) {
                return new Report(false, log.size(), head,
                        "chain length " + log.size() + " != " + expectedCommits);
            }
            for (int i = 0; i < log.size(); i++) {
                CommitView commit = log.get(i);
                if (i == log.size() - 1) {
                    if (!commit.parents().isEmpty()) {
                        return new Report(false, log.size(), head, "root commit has a parent");
                    }
                } else if (commit.parents().size() != 1
                        || !commit.parents().get(0).equals(log.get(i + 1).hash())) {
                    return new Report(false, log.size(), head, "parent link broken at " + commit.hash());
                }
                ObjectGraph.files(hash -> store.readObject(repo, hash), commit.hash());
            }
            return new Report(true, log.size(), head, "ok");
        } catch (RuntimeException e) {
            return new Report(false, -1, null, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }
}
