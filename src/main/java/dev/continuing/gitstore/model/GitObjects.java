package dev.continuing.gitstore.model;

import dev.continuing.gitstore.GitStoreException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

/**
 * Loose objects in the Git style: {@code <type> <size>\0<payload>}, addressed by SHA-256.
 * SHA-256 is used instead of Git's SHA-1 so the exercise is not tied to a broken hash.
 */
public final class GitObjects {
    private GitObjects() {}

    public record TreeEntry(String mode, String path, String blobHash) {}

    public record CommitInfo(
            String tree,
            List<String> parents,
            String author,
            long timeMillis,
            String message
    ) {
        public CommitInfo {
            parents = parents == null ? List.of() : List.copyOf(parents);
        }
    }

    public record Raw(ObjectKind kind, byte[] stored, String hash) {}

    public record Decoded(
            ObjectKind kind,
            String hash,
            byte[] payload,
            CommitInfo commit,
            List<TreeEntry> entries
    ) {}

    public static Raw blob(byte[] content) {
        byte[] body = content == null ? new byte[0] : Arrays.copyOf(content, content.length);
        return encode(ObjectKind.BLOB, body);
    }

    public static Raw tree(List<TreeEntry> entries) {
        List<TreeEntry> sorted = entries.stream()
                .sorted(Comparator.comparing(TreeEntry::path))
                .toList();
        var out = new java.io.ByteArrayOutputStream();
        for (TreeEntry entry : sorted) {
            out.writeBytes(entry.mode().getBytes(StandardCharsets.UTF_8));
            out.write(' ');
            out.writeBytes(entry.path().getBytes(StandardCharsets.UTF_8));
            out.write(0);
            out.writeBytes(entry.blobHash().getBytes(StandardCharsets.UTF_8));
            out.write('\n');
        }
        return encode(ObjectKind.TREE, out.toByteArray());
    }

    public static Raw commit(CommitInfo info) {
        var sb = new StringBuilder();
        sb.append("tree ").append(info.tree()).append('\n');
        for (String parent : info.parents()) {
            sb.append("parent ").append(parent).append('\n');
        }
        sb.append("author ").append(info.author()).append(' ').append(info.timeMillis()).append('\n');
        sb.append('\n');
        sb.append(info.message());
        return encode(ObjectKind.COMMIT, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    public static String hash(byte[] stored) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(stored);
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Decoded decode(byte[] stored) {
        int nul = indexOf(stored, (byte) 0, 0);
        if (nul < 0) {
            throw GitStoreException.corrupt("object missing header NUL");
        }
        String header = new String(stored, 0, nul, StandardCharsets.UTF_8);
        int space = header.indexOf(' ');
        if (space <= 0) {
            throw GitStoreException.corrupt("object header is not '<type> <size>'");
        }
        ObjectKind kind;
        int size;
        try {
            kind = ObjectKind.fromWire(header.substring(0, space));
            size = Integer.parseInt(header.substring(space + 1));
        } catch (RuntimeException e) {
            throw GitStoreException.corrupt("object header is not '<type> <size>'");
        }
        byte[] payload = Arrays.copyOfRange(stored, nul + 1, stored.length);
        if (payload.length != size) {
            throw GitStoreException.corrupt(
                    "object size header " + size + " but payload is " + payload.length);
        }
        String hash = hash(stored);
        return switch (kind) {
            case BLOB -> new Decoded(kind, hash, payload, null, List.of());
            case TREE -> new Decoded(kind, hash, payload, null, parseTree(payload));
            case COMMIT -> new Decoded(kind, hash, payload, parseCommit(payload), List.of());
        };
    }

    private static Raw encode(ObjectKind kind, byte[] payload) {
        byte[] header = (kind.wire() + " " + payload.length + "\0").getBytes(StandardCharsets.UTF_8);
        byte[] stored = new byte[header.length + payload.length];
        System.arraycopy(header, 0, stored, 0, header.length);
        System.arraycopy(payload, 0, stored, header.length, payload.length);
        return new Raw(kind, stored, hash(stored));
    }

    private static List<TreeEntry> parseTree(byte[] payload) {
        List<TreeEntry> entries = new ArrayList<>();
        int i = 0;
        while (i < payload.length) {
            int space = indexOf(payload, (byte) ' ', i);
            int nul = space < 0 ? -1 : indexOf(payload, (byte) 0, space + 1);
            int nl = nul < 0 ? -1 : indexOf(payload, (byte) '\n', nul + 1);
            if (space < 0 || nul < 0 || nl < 0) {
                throw GitStoreException.corrupt("truncated tree entry");
            }
            String mode = new String(payload, i, space - i, StandardCharsets.UTF_8);
            String path = new String(payload, space + 1, nul - (space + 1), StandardCharsets.UTF_8);
            String blobHash = new String(payload, nul + 1, nl - (nul + 1), StandardCharsets.UTF_8);
            entries.add(new TreeEntry(mode, path, blobHash));
            i = nl + 1;
        }
        return List.copyOf(entries);
    }

    private static CommitInfo parseCommit(byte[] payload) {
        String text = new String(payload, StandardCharsets.UTF_8);
        int blank = text.indexOf("\n\n");
        if (blank < 0) {
            throw GitStoreException.corrupt("commit is missing the blank line before the message");
        }
        String header = text.substring(0, blank);
        String message = text.substring(blank + 2);
        String tree = null;
        List<String> parents = new ArrayList<>();
        String author = null;
        long timeMillis = 0;
        if (!header.isEmpty()) {
            for (String line : header.split("\n", -1)) {
                if (line.startsWith("tree ")) {
                    tree = line.substring("tree ".length());
                } else if (line.startsWith("parent ")) {
                    parents.add(line.substring("parent ".length()));
                } else if (line.startsWith("author ")) {
                    int lastSpace = line.lastIndexOf(' ');
                    if (lastSpace <= "author ".length()) {
                        throw GitStoreException.corrupt("commit author line has no timestamp");
                    }
                    author = line.substring("author ".length(), lastSpace);
                    try {
                        timeMillis = Long.parseLong(line.substring(lastSpace + 1));
                    } catch (NumberFormatException e) {
                        throw GitStoreException.corrupt("commit author timestamp is not an integer");
                    }
                } else {
                    throw GitStoreException.corrupt("unexpected commit header line");
                }
            }
        }
        if (tree == null || author == null) {
            throw GitStoreException.corrupt("commit is missing tree or author");
        }
        return new CommitInfo(tree, parents, author, timeMillis, message);
    }

    private static int indexOf(byte[] bytes, byte target, int from) {
        for (int i = Math.max(0, from); i < bytes.length; i++) {
            if (bytes[i] == target) {
                return i;
            }
        }
        return -1;
    }
}
