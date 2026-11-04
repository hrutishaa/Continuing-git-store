package dev.continuing.gitstore.http;

import com.fasterxml.jackson.databind.JsonNode;
import dev.continuing.gitstore.json.Jsons;
import dev.continuing.gitstore.storage.MemoryObjectStorage;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeServerTest {
    private final HttpClient client = HttpClient.newHttpClient();

    @Test
    void pushOnOneNodeIsVisibleOnTheOthers() throws Exception {
        var storage = new MemoryObjectStorage();
        try (NodeServer first = NodeServer.start(0, "node-a", storage, "memory");
             NodeServer second = NodeServer.start(0, "node-b", storage, "memory");
             NodeServer third = NodeServer.start(0, "node-c", storage, "memory")) {
            HttpResponse<String> health = send(first, "GET", "/health", null);
            assertEquals(200, health.statusCode());
            assertEquals("node-a", Jsons.MAPPER.readTree(health.body()).get("nodeId").asText());

            HttpResponse<String> pushed = send(first, "POST", "/v1/repos/demo/push", """
                    {"ref":"refs/heads/main","author":"Ada Lovelace","message":"hello","files":{"README.md":"hello\\n"}}
                    """);
            assertEquals(201, pushed.statusCode(), pushed.body());
            assertEquals("node-a", pushed.headers().firstValue("X-Gitstore-Node").orElseThrow());
            String commit = Jsons.MAPPER.readTree(pushed.body()).get("commit").asText();

            HttpResponse<String> files = send(
                    second, "GET", "/v1/repos/demo/files?ref=refs/heads/main", null);
            JsonNode body = Jsons.MAPPER.readTree(files.body());
            assertEquals(200, files.statusCode(), files.body());
            assertEquals("node-b", body.get("nodeId").asText());
            assertEquals(commit, body.get("commit").asText());
            assertEquals("hello\n", body.get("files").get("README.md").asText());

            HttpResponse<String> conflict = send(third, "POST", "/v1/repos/demo/push", """
                    {"ref":"refs/heads/main","author":"Ada Lovelace","message":"stale","files":{"README.md":"no"}}
                    """);
            assertEquals(409, conflict.statusCode(), conflict.body());
            assertEquals(commit, Jsons.MAPPER.readTree(conflict.body()).get("actual").asText());

            HttpResponse<String> log = send(
                    third, "GET", "/v1/repos/demo/log?ref=refs/heads/main&limit=5", null);
            assertEquals(commit, Jsons.MAPPER.readTree(log.body()).get("commits").get(0).get("hash").asText());
        }
    }

    @Test
    void concurrentCreatesThroughDifferentNodesHaveOneWinner() throws Exception {
        var storage = new MemoryObjectStorage();
        List<NodeServer> nodes = List.of(
                NodeServer.start(0, "node-a", storage, "memory"),
                NodeServer.start(0, "node-b", storage, "memory"),
                NodeServer.start(0, "node-c", storage, "memory"));
        try {
            int threads = 12;
            var start = new CyclicBarrier(threads);
            try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
                List<Future<Integer>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    int n = i;
                    futures.add(pool.submit(() -> {
                        start.await(10, TimeUnit.SECONDS);
                        NodeServer node = nodes.get(n % nodes.size());
                        HttpResponse<String> response = send(node, "POST", "/v1/repos/race/push", """
                                {"ref":"refs/heads/main","author":"Ada Lovelace","message":"m%d","files":{"f.txt":"c%d"}}
                                """.formatted(n, n));
                        return response.statusCode();
                    }));
                }
                int created = 0;
                int conflicts = 0;
                for (Future<Integer> future : futures) {
                    int status = future.get(30, TimeUnit.SECONDS);
                    if (status == 201) {
                        created++;
                    } else if (status == 409) {
                        conflicts++;
                    }
                }
                assertEquals(1, created);
                assertEquals(threads - 1, conflicts);
            }
            List<String> heads = new ArrayList<>();
            for (NodeServer node : nodes) {
                HttpResponse<String> files = send(
                        node, "GET", "/v1/repos/race/files?ref=refs/heads/main", null);
                assertEquals(200, files.statusCode(), files.body());
                heads.add(Jsons.MAPPER.readTree(files.body()).get("commit").asText());
            }
            assertEquals(1, heads.stream().distinct().count());
            assertTrue(heads.get(0).matches("[0-9a-f]{64}"));
        } finally {
            nodes.forEach(NodeServer::close);
        }
    }

    private HttpResponse<String> send(NodeServer node, String method, String path, String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + node.port() + path));
        if ("POST".equals(method)) {
            request.header("content-type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body));
        } else {
            request.GET();
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }
}
