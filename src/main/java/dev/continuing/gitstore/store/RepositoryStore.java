package dev.continuing.gitstore.store;

import dev.continuing.gitstore.GitStoreException;
import dev.continuing.gitstore.model.CommitView;
import dev.continuing.gitstore.model.PushCommand;
import dev.continuing.gitstore.model.PushOutcome;
import dev.continuing.gitstore.model.RepoSnapshot;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The operations both the WAL store and the 3-replica simulator implement.
 * A stateless node is just this interface in front of shared storage.
 */
public interface RepositoryStore {
    PushOutcome push(String repo, PushCommand command);

    RepoSnapshot fetch(String repo);

    Optional<byte[]> readObject(String repo, String hash);

    default List<CommitView> log(String repo, String ref, int limit) {
        String head = fetch(repo).commit(ref);
        if (head == null) {
            return List.of();
        }
        return ObjectGraph.log(hash -> readObject(repo, hash), head, limit);
    }

    default Map<String, byte[]> files(String repo, String ref) {
        String head = fetch(repo).commit(ref);
        if (head == null) {
            throw GitStoreException.notFound("unknown ref " + ref);
        }
        return ObjectGraph.files(hash -> readObject(repo, hash), head);
    }
}
