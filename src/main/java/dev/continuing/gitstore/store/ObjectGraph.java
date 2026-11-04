package dev.continuing.gitstore.store;

import dev.continuing.gitstore.GitStoreException;
import dev.continuing.gitstore.model.CommitView;
import dev.continuing.gitstore.model.GitObjects;
import dev.continuing.gitstore.model.ObjectKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

/** Walks commit, tree, and blob objects once they have been loaded as raw bytes. */
public final class ObjectGraph {
    private ObjectGraph() {}

    public static List<CommitView> log(Function<String, Optional<byte[]>> read, String head, int limit) {
        if (head == null || limit <= 0) {
            return List.of();
        }
        List<CommitView> out = new ArrayList<>();
        String hash = head;
        int guard = 0;
        while (hash != null && out.size() < limit && guard++ < 1_000_000) {
            GitObjects.Decoded decoded = decodeChecked(read, hash);
            if (decoded.kind() != ObjectKind.COMMIT) {
                throw GitStoreException.corrupt(hash + " is not a commit");
            }
            GitObjects.CommitInfo commit = decoded.commit();
            out.add(new CommitView(
                    hash,
                    commit.parents(),
                    commit.author(),
                    commit.timeMillis(),
                    commit.message(),
                    commit.tree()));
            hash = commit.parents().isEmpty() ? null : commit.parents().get(0);
        }
        return List.copyOf(out);
    }

    public static TreeMap<String, byte[]> files(Function<String, Optional<byte[]>> read, String commitHash) {
        GitObjects.Decoded commit = decodeChecked(read, commitHash);
        if (commit.kind() != ObjectKind.COMMIT) {
            throw GitStoreException.corrupt(commitHash + " is not a commit");
        }
        GitObjects.Decoded tree = decodeChecked(read, commit.commit().tree());
        var files = new TreeMap<String, byte[]>();
        for (GitObjects.TreeEntry entry : tree.entries()) {
            GitObjects.Decoded blob = decodeChecked(read, entry.blobHash());
            if (blob.kind() != ObjectKind.BLOB) {
                throw GitStoreException.corrupt(entry.path() + " is not a blob");
            }
            files.put(entry.path(), Arrays.copyOf(blob.payload(), blob.payload().length));
        }
        return files;
    }

    private static GitObjects.Decoded decodeChecked(Function<String, Optional<byte[]>> read, String hash) {
        byte[] raw = read.apply(hash).orElseThrow(() -> GitStoreException.corrupt("missing object " + hash));
        GitObjects.Decoded decoded = GitObjects.decode(raw);
        if (!decoded.hash().equals(hash)) {
            throw GitStoreException.corrupt("object key " + hash + " does not match content hash " + decoded.hash());
        }
        return decoded;
    }
}
