package dev.continuing.gitstore.storage;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

/** Just enough of the S3 API to exercise the AWS SDK's conditional puts. Auth is ignored. */
final class FakeS3 implements AutoCloseable {
    final List<String> calls = new CopyOnWriteArrayList<>();
    private final String bucket;
    private final ConcurrentHashMap<String, byte[]> objects = new ConcurrentHashMap<>();
    private final HttpServer server;

    FakeS3(String bucket) throws IOException {
        this.bucket = bucket;
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        this.server.createContext("/", this::handle);
        this.server.start();
    }

    String endpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getRawPath();
        String query = exchange.getRequestURI().getRawQuery();
        calls.add(method + " " + path + (query == null ? "" : "?" + query)
                + " none=" + exchange.getRequestHeaders().getFirst("If-None-Match")
                + " match=" + exchange.getRequestHeaders().getFirst("If-Match"));
        String prefix = "/" + bucket;
        try {
            if (path.equals(prefix) || path.equals(prefix + "/")) {
                bucket(exchange, method);
                return;
            }
            if (!path.startsWith(prefix + "/")) {
                send(exchange, 404, "NoSuchBucket", "unknown bucket");
                return;
            }
            String key = java.net.URLDecoder.decode(path.substring(prefix.length() + 1), StandardCharsets.UTF_8);
            switch (method) {
                case "PUT" -> put(exchange, key, body);
                case "GET" -> get(exchange, key);
                case "HEAD" -> head(exchange, key);
                case "DELETE" -> {
                    objects.remove(key);
                    send(exchange, 204, null, null);
                }
                default -> send(exchange, 405, "MethodNotAllowed", method);
            }
        } catch (RuntimeException e) {
            send(exchange, 500, "InternalError", e.getMessage());
        } finally {
            exchange.close();
        }
    }

    private void bucket(HttpExchange exchange, String method) throws IOException {
        if ("PUT".equals(method) || "HEAD".equals(method)) {
            send(exchange, 200, null, null);
            return;
        }
        if ("GET".equals(method)) {
            byte[] xml = "<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></LocationConstraint>"
                    .getBytes(StandardCharsets.UTF_8);
            sendBytes(exchange, 200, "application/xml", xml, null);
            return;
        }
        send(exchange, 200, null, null);
    }

    private void put(HttpExchange exchange, String key, byte[] body) throws IOException {
        String ifNone = exchange.getRequestHeaders().getFirst("If-None-Match");
        String ifMatch = exchange.getRequestHeaders().getFirst("If-Match");
        String etag = etag(body);
        if ("*".equals(ifNone) && objects.containsKey(key)) {
            send(exchange, 412, "PreconditionFailed", "key exists");
            return;
        }
        if (ifMatch != null && !etagEquals(ifMatch, objects.containsKey(key) ? etag(objects.get(key)) : null)) {
            send(exchange, 412, "PreconditionFailed", "etag mismatch");
            return;
        }
        objects.put(key, body);
        sendBytes(exchange, 200, "application/octet-stream", new byte[0], etag);
    }

    private void get(HttpExchange exchange, String key) throws IOException {
        byte[] body = objects.get(key);
        if (body == null) {
            send(exchange, 404, "NoSuchKey", key);
            return;
        }
        sendBytes(exchange, 200, "application/octet-stream", body, etag(body));
    }

    private void head(HttpExchange exchange, String key) throws IOException {
        byte[] body = objects.get(key);
        if (body == null) {
            send(exchange, 404, "NoSuchKey", key);
            return;
        }
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        exchange.getResponseHeaders().set("ETag", etag(body));
        exchange.sendResponseHeaders(200, -1);
    }

    private static boolean etagEquals(String header, String current) {
        if (current == null) {
            return false;
        }
        return MemoryObjectStorage.normalize(header).equals(MemoryObjectStorage.normalize(current));
    }

    private static String etag(byte[] body) {
        try {
            return "\"" + HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(body)) + "\"";
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void send(HttpExchange exchange, int status, String code, String message) throws IOException {
        if (code == null) {
            sendBytes(exchange, status, "application/xml", new byte[0], null);
            return;
        }
        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <Error><Code>%s</Code><Message>%s</Message><RequestId>fake</RequestId></Error>
                """.formatted(code, message == null ? "" : message);
        sendBytes(exchange, status, "application/xml", xml.getBytes(StandardCharsets.UTF_8), null);
    }

    private static void sendBytes(HttpExchange exchange, int status, String type, byte[] body, String etag)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", type);
        if (etag != null) {
            exchange.getResponseHeaders().set("ETag", etag);
        }
        boolean head = "HEAD".equalsIgnoreCase(exchange.getRequestMethod());
        exchange.sendResponseHeaders(status, head ? -1 : body.length);
        if (!head) {
            exchange.getResponseBody().write(body);
        }
    }
}
