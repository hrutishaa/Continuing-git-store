package dev.continuing.gitstore.store;

import dev.continuing.gitstore.GitStoreException;
import dev.continuing.gitstore.model.GitObjects;
import dev.continuing.gitstore.model.PushCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

/** Builds the blob, tree, and commit objects for one push on top of an optional parent commit. */
public final class CommitBuilder {
    private CommitBuilder() {}

    public record Built(GitObjects.Raw commit, List<GitObjects.Raw> objects) {}

    public static Built build(
            Function<String, Optional<byte[]>> read,
            String parentCommit,
            PushCommand command,
            long nowMillis
    ) {
        var paths = new TreeMap<String, String>();
        if (parentCommit != null) {
            GitObjects.Decoded parent = GitObjects.decode(required(read, parentCommit, "parent commit"));
            GitObjects.Decoded tree = GitObjects.decode(required(read, parent.commit().tree(), "parent tree"));
            for (GitObjects.TreeEntry entry : tree.entries()) {
                paths.put(entry.path(), entry.blobHash());
            }
        }
        List<GitObjects.Raw> created = new ArrayList<>();
        for (var upsert : new TreeMap<>(command.upserts()).entrySet()) {
            GitObjects.Raw blob = GitObjects.blob(upsert.getValue());
            created.add(blob);
            paths.put(upsert.getKey(), blob.hash());
        }
        for (String deleted : command.deletePaths()) {
            paths.remove(deleted);
        }
        List<GitObjects.TreeEntry> entries = paths.entrySet().stream()
                .map(entry -> new GitObjects.TreeEntry("100644", entry.getKey(), entry.getValue()))
                .toList();
        GitObjects.Raw tree = GitObjects.tree(entries);
        created.add(tree);
        var info = new GitObjects.CommitInfo(
                tree.hash(),
                parentCommit == null ? List.of() : List.of(parentCommit),
                command.author(),
                nowMillis,
                command.message());
        GitObjects.Raw commit = GitObjects.commit(info);
        created.add(commit);
        return new Built(commit, List.copyOf(created));
    }

    private static byte[] required(Function<String, Optional<byte[]>> read, String hash, String label) {
        return read.apply(hash).orElseThrow(() -> GitStoreException.corrupt("missing " + label + " " + hash));
    }
}
