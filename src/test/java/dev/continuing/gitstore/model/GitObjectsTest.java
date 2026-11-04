package dev.continuing.gitstore.model;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GitObjectsTest {
    @Test
    void blobHeaderMatchesGitLooseObjectShape() {
        byte[] payload = "hello\n".getBytes(StandardCharsets.UTF_8);
        GitObjects.Raw raw = GitObjects.blob(payload);
        byte[] header = "blob 6\0".getBytes(StandardCharsets.UTF_8);
        byte[] expected = new byte[header.length + payload.length];
        System.arraycopy(header, 0, expected, 0, header.length);
        System.arraycopy(payload, 0, expected, header.length, payload.length);
        assertArrayEquals(expected, raw.stored());
        assertEquals(GitObjects.hash(expected), raw.hash());
        assertEquals(64, raw.hash().length());
    }

    @Test
    void sameBytesSameHashAndRoundTrip() {
        GitObjects.Raw first = GitObjects.blob("same".getBytes(StandardCharsets.UTF_8));
        GitObjects.Raw second = GitObjects.blob("same".getBytes(StandardCharsets.UTF_8));
        assertEquals(first.hash(), second.hash());
        GitObjects.Decoded decoded = GitObjects.decode(first.stored());
        assertEquals(ObjectKind.BLOB, decoded.kind());
        assertEquals(first.hash(), decoded.hash());
        assertArrayEquals("same".getBytes(StandardCharsets.UTF_8), decoded.payload());
    }

    @Test
    void treeHashIgnoresInsertionOrder() {
        var forward = List.of(
                new GitObjects.TreeEntry("100644", "a.txt", "a".repeat(64)),
                new GitObjects.TreeEntry("100644", "b.txt", "b".repeat(64)));
        var backward = List.of(forward.get(1), forward.get(0));
        assertEquals(GitObjects.tree(forward).hash(), GitObjects.tree(backward).hash());
        GitObjects.Decoded decoded = GitObjects.decode(GitObjects.tree(backward).stored());
        assertEquals("a.txt", decoded.entries().get(0).path());
        assertEquals("b.txt", decoded.entries().get(1).path());
    }

    @Test
    void commitRoundTripKeepsParentsAuthorAndMessage() {
        GitObjects.Raw tree = GitObjects.tree(List.of());
        var info = new GitObjects.CommitInfo(
                tree.hash(),
                List.of("c".repeat(64)),
                "Ada Lovelace",
                1_700_000_000_000L,
                "line one\nline two");
        GitObjects.Raw commit = GitObjects.commit(info);
        GitObjects.Decoded decoded = GitObjects.decode(commit.stored());
        assertEquals(ObjectKind.COMMIT, decoded.kind());
        assertEquals(info, decoded.commit());
        assertTrue(new String(decoded.payload(), StandardCharsets.UTF_8).contains("Ada Lovelace"));
    }

    @Test
    void differentContentDifferentHash() {
        assertNotEquals(
                GitObjects.blob("a".getBytes(StandardCharsets.UTF_8)).hash(),
                GitObjects.blob("b".getBytes(StandardCharsets.UTF_8)).hash());
    }
}
