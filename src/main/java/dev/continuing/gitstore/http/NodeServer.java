package dev.continuing.gitstore.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.continuing.gitstore.GitStoreException;
import dev.continuing.gitstore.Names;
import dev.continuing.gitstore.json.Jsons;
import dev.continuing.gitstore.model.CommitView;
import dev.continuing.gitstore.model.GitObjects;
import dev.continuing.gitstore.model.ObjectKind;
import dev.continuing.gitstore.model.PushCommand;
import dev.continuing.gitstore.model.PushOutcome;
import dev.continuing.gitstore.model.RepoSnapshot;
import dev.continuing.gitstore.storage.ObjectStorage;
import dev.continuing.gitstore.wal.WalRefStore;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * One stateless Git node. It keeps no repository data: every push and fetch goes to the
 * shared {@link ObjectStorage} through a fresh view of the write-ahead log.
 */
public final class NodeServer implements AutoCloseable {
    private final String nodeId;
    private final String storageLabel;
    private final WalRefStore store;
    private final HttpServer server;
    private final java.util.concurrent.ExecutorService executor;

    private NodeServer(String nodeId, String storageLabel, WalRefStore store, HttpServer server,
                       java.util.concurrent.ExecutorService executor) {
        this.nodeId = nodeId;
        this.storageLabel = storageLabel;
        this.store = store;
        this.server = server;
        this.executor = executor;
    }

    public static NodeServer start(int port, String nodeId, ObjectStorage storage, String storageLabel) {
        WalRefStore store = new WalRefStore(storage);
        try {
            HttpServer http = HttpServer.create(new InetSocketAddress(port), 0);
            var executor = Executors.newVirtualThreadPerTaskExecutor();
            http.setExecutor(executor);
            NodeServer node = new NodeServer(nodeId, storageLabel, store, http, executor);
            http.createContext("/", node::handle);
            http.start();
            return node;
        } catch (IOException e) {
            throw new IllegalStateException("could not bind node " + nodeId, e);
        }
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public String nodeId() {
        return nodeId;
    }

    @Override
    public void close() {
        server.stop(0);
        executor.close();
    }

    private void handle(HttpExchange exchange) {
        try {
            route(exchange);
        } catch (GitStoreException e) {
            writeError(exchange, e.httpStatus(), e.getMessage());
        } catch (RuntimeException e) {
            System.getLogger("gitstore").log(System.Logger.Level.ERROR, "request failed", e);
            writeError(exchange, 500, "internal error");
        } finally {
            exchange.close();
        }
    }

    private void route(HttpExchange exchange) {
        URI uri = exchange.getRequestURI();
        String method = exchange.getRequestMethod();
        String path = uri.getPath();
        if ("/health".equals(path) && "GET".equals(method)) {
            ObjectNode body = Jsons.MAPPER.createObjectNode();
            body.put("ok", true);
            body.put("nodeId", nodeId);
            body.put("storage", storageLabel);
            write(exchange, 200, body);
            return;
        }
        if (!path.startsWith("/v1/repos/")) {
            writeError(exchange, 404, "not found");
            return;
        }
        String rest = path.substring("/v1/repos/".length());
        int slash = rest.indexOf('/');
        String repo = urlDecode(slash < 0 ? rest : rest.substring(0, slash));
        String tail = slash < 0 ? "" : rest.substring(slash + 1);
        Names.requireRepo(repo);
        Map<String, String> query = query(uri);
        if (tail.isEmpty() && "GET".equals(method)) {
            writeSnapshot(exchange, store.fetch(repo));
            return;
        }
        if ("push".equals(tail) && "POST".equals(method)) {
            push(exchange, repo);
            return;
        }
        if ("log".equals(tail) && "GET".equals(method)) {
            writeLog(exchange, repo, required(query, "ref"), limit(query));
            return;
        }
        if ("files".equals(tail) && "GET".equals(method)) {
            writeFiles(exchange, repo, required(query, "ref"));
            return;
        }
        if (tail.startsWith("objects/") && "GET".equals(method)) {
            writeObject(exchange, repo, tail.substring("objects/".length()));
            return;
        }
        writeError(exchange, 404, "not found");
    }

    private void push(HttpExchange exchange, String repo) {
        JsonNode node = readJson(exchange);
        if (node == null || !node.isObject()) {
            throw GitStoreException.badRequest("push body must be a JSON object");
        }
        String ref = text(node, "ref");
        String author = text(node, "author");
        String message = node.hasNonNull("message") ? node.get("message").asText() : "";
        String expected = node.has("expected") && !node.get("expected").isNull()
                ? node.get("expected").asText()
                : null;
        boolean force = node.path("force").asBoolean(false);
        Map<String, byte[]> files = new LinkedHashMap<>();
        JsonNode fileNode = node.get("files");
        if (fileNode != null && !fileNode.isNull()) {
            if (!fileNode.isObject()) {
                throw GitStoreException.badRequest("files must be an object of path to text");
            }
            fileNode.fields().forEachRemaining(entry -> {
                if (!entry.getValue().isTextual()) {
                    throw GitStoreException.badRequest("file content must be a string: " + entry.getKey());
                }
                files.put(entry.getKey(), entry.getValue().asText().getBytes(StandardCharsets.UTF_8));
            });
        }
        List<String> deletes = new java.util.ArrayList<>();
        JsonNode deleteNode = node.get("deletePaths");
        if (deleteNode != null && !deleteNode.isNull()) {
            if (!deleteNode.isArray()) {
                throw GitStoreException.badRequest("deletePaths must be an array");
            }
            deleteNode.forEach(item -> deletes.add(item.asText()));
        }
        PushOutcome outcome = store.push(
                repo, new PushCommand(ref, expected, author, message, files, deletes, force));
        switch (outcome) {
            case PushOutcome.Ok ok -> {
                ObjectNode body = Jsons.MAPPER.createObjectNode();
                body.put("status", "ok");
                body.put("nodeId", nodeId);
                body.put("repo", repo);
                body.put("ref", ok.ref());
                body.put("seq", ok.seq());
                body.put("commit", ok.commit());
                write(exchange, 201, body);
            }
            case PushOutcome.Conflict conflict -> {
                ObjectNode body = Jsons.MAPPER.createObjectNode();
                body.put("status", "conflict");
                body.put("nodeId", nodeId);
                body.put("repo", repo);
                body.put("ref", conflict.ref());
                if (conflict.expected() == null) {
                    body.putNull("expected");
                } else {
                    body.put("expected", conflict.expected());
                }
                if (conflict.actual() == null) {
                    body.putNull("actual");
                } else {
                    body.put("actual", conflict.actual());
                }
                write(exchange, 409, body);
            }
        }
    }

    private void writeSnapshot(HttpExchange exchange, RepoSnapshot snapshot) {
        ObjectNode body = Jsons.MAPPER.createObjectNode();
        body.put("nodeId", nodeId);
        body.put("repo", snapshot.repo());
        body.put("seq", snapshot.seq());
        ObjectNode refs = body.putObject("refs");
        snapshot.refs().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> refs.put(entry.getKey(), entry.getValue()));
        write(exchange, 200, body);
    }

    private void writeLog(HttpExchange exchange, String repo, String ref, int limit) {
        List<CommitView> log = store.log(repo, ref, limit);
        ObjectNode body = Jsons.MAPPER.createObjectNode();
        body.put("nodeId", nodeId);
        body.put("repo", repo);
        body.put("ref", ref);
        var commits = body.putArray("commits");
        for (CommitView commit : log) {
            ObjectNode item = commits.addObject();
            item.put("hash", commit.hash());
            item.put("author", commit.author());
            item.put("timeMillis", commit.timeMillis());
            item.put("message", commit.message());
            item.put("tree", commit.tree());
            var parents = item.putArray("parents");
            commit.parents().forEach(parents::add);
        }
        write(exchange, 200, body);
    }

    private void writeFiles(HttpExchange exchange, String repo, String ref) {
        RepoSnapshot snapshot = store.fetch(repo);
        String commit = snapshot.commit(ref);
        if (commit == null) {
            throw GitStoreException.notFound("unknown ref " + ref);
        }
        ObjectNode body = Jsons.MAPPER.createObjectNode();
        body.put("nodeId", nodeId);
        body.put("repo", repo);
        body.put("ref", ref);
        body.put("commit", commit);
        body.put("seq", snapshot.seq());
        ObjectNode files = body.putObject("files");
        store.files(repo, ref).forEach((path, bytes) -> files.put(path, textOrBase64(bytes)));
        write(exchange, 200, body);
    }

    private void writeObject(HttpExchange exchange, String repo, String hash) {
        Names.requireHash(hash);
        byte[] raw = store.readObject(repo, hash)
                .orElseThrow(() -> GitStoreException.notFound("unknown object " + hash));
        GitObjects.Decoded decoded = GitObjects.decode(raw);
        ObjectNode body = Jsons.MAPPER.createObjectNode();
        body.put("nodeId", nodeId);
        body.put("repo", repo);
        body.put("hash", decoded.hash());
        body.put("kind", decoded.kind().wire());
        body.put("size", decoded.payload().length);
        if (decoded.kind() == ObjectKind.BLOB) {
            body.put("text", textOrBase64(decoded.payload()));
        } else if (decoded.kind() == ObjectKind.COMMIT && decoded.commit() != null) {
            body.put("tree", decoded.commit().tree());
            body.put("author", decoded.commit().author());
            body.put("timeMillis", decoded.commit().timeMillis());
            body.put("message", decoded.commit().message());
            var parents = body.putArray("parents");
            decoded.commit().parents().forEach(parents::add);
        }
        write(exchange, 200, body);
    }

    private static String textOrBase64(byte[] bytes) {
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes));
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (CharacterCodingException e) {
            return "base64:" + Base64.getEncoder().encodeToString(bytes);
        }
    }

    private static JsonNode readJson(HttpExchange exchange) {
        try (InputStream in = exchange.getRequestBody()) {
            byte[] body = in.readNBytes(2_000_001);
            if (body.length > 2_000_000) {
                throw GitStoreException.badRequest("request body is larger than 2 MiB");
            }
            if (body.length == 0) {
                throw GitStoreException.badRequest("request body is empty");
            }
            return Jsons.MAPPER.readTree(body);
        } catch (GitStoreException e) {
            throw e;
        } catch (Exception e) {
            throw GitStoreException.badRequest("request body is not JSON");
        }
    }

    private static int limit(Map<String, String> query) {
        String raw = query.get("limit");
        if (raw == null || raw.isBlank()) {
            return 50;
        }
        try {
            int limit = Integer.parseInt(raw);
            if (limit < 1 || limit > 5_000) {
                throw GitStoreException.badRequest("limit must be between 1 and 5000");
            }
            return limit;
        } catch (NumberFormatException e) {
            throw GitStoreException.badRequest("limit must be an integer");
        }
    }

    private static String required(Map<String, String> query, String name) {
        String value = query.get(name);
        if (value == null || value.isBlank()) {
            throw GitStoreException.badRequest("missing query parameter " + name);
        }
        return value;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual() || value.asText().isBlank()) {
            throw GitStoreException.badRequest("missing string field " + field);
        }
        return value.asText();
    }

    private static Map<String, String> query(URI uri) {
        Map<String, String> out = new LinkedHashMap<>();
        String raw = uri.getRawQuery();
        if (raw == null || raw.isEmpty()) {
            return out;
        }
        for (String part : raw.split("&")) {
            if (part.isEmpty()) {
                continue;
            }
            int eq = part.indexOf('=');
            String key = urlDecode(eq < 0 ? part : part.substring(0, eq));
            String value = urlDecode(eq < 0 ? "" : part.substring(eq + 1));
            out.put(key, value);
        }
        return out;
    }

    private static String urlDecode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private void writeError(HttpExchange exchange, int status, String message) {
        ObjectNode body = Jsons.MAPPER.createObjectNode();
        body.put("status", "error");
        body.put("nodeId", nodeId);
        body.put("message", message);
        write(exchange, status, body);
    }

    private void write(HttpExchange exchange, int status, ObjectNode body) {
        byte[] bytes;
        try {
            bytes = Jsons.MAPPER.writeValueAsBytes(body);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        try {
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.getResponseHeaders().set("X-Gitstore-Node", nodeId);
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
