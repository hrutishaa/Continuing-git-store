package dev.continuing.gitstore;

import dev.continuing.gitstore.bench.BenchmarkRunner;
import dev.continuing.gitstore.http.NodeServer;
import dev.continuing.gitstore.storage.MemoryObjectStorage;
import dev.continuing.gitstore.storage.ObjectStorage;
import dev.continuing.gitstore.storage.S3Config;
import dev.continuing.gitstore.storage.S3ObjectStorage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Main {
    private Main() {}

    public static void main(String[] args) {
        if (args.length == 0 || "help".equals(args[0]) || "--help".equals(args[0]) || "-h".equals(args[0])) {
            usage();
            System.exit(args.length == 0 ? 2 : 0);
            return;
        }
        try {
            switch (args[0]) {
                case "node" -> node(flags(args));
                case "demo" -> demo(flags(args));
                case "benchmark" -> System.exit(BenchmarkRunner.run(args));
                default -> {
                    usage();
                    System.exit(2);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("interrupted");
            System.exit(1);
        } catch (RuntimeException e) {
            System.err.println(e.getMessage() == null ? e.toString() : e.getMessage());
            System.exit(1);
        }
    }

    private static void node(Map<String, String> flags) throws InterruptedException {
        int port = Integer.parseInt(flags.getOrDefault("port", env("PORT", "8080")));
        String nodeId = flags.getOrDefault("node-id", env("NODE_ID", "node-" + port));
        String storageName = flags.getOrDefault("storage", env("STORAGE", "memory"));
        ObjectStorage storage = openStorage(storageName, flags);
        NodeServer server = NodeServer.start(port, nodeId, storage, storageName);
        System.out.println("node " + nodeId + " listening on " + server.port() + " storage=" + storageName);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            if (storage instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception ignored) {
                    // shutting down
                }
            }
        }));
        Thread.currentThread().join();
    }

    private static void demo(Map<String, String> flags) throws InterruptedException {
        int base = Integer.parseInt(flags.getOrDefault("base-port", "8080"));
        var storage = new MemoryObjectStorage();
        List<NodeServer> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            nodes.add(NodeServer.start(base + i, "node-" + (i + 1), storage, "memory"));
        }
        System.out.println("Three stateless nodes sharing one in-memory object store.");
        System.out.println("Any node can take a push. The others serve that push on the next fetch.");
        System.out.println();
        System.out.println("curl -s -X POST localhost:" + base + "/v1/repos/demo/push \\");
        System.out.println("  -H 'content-type: application/json' \\");
        System.out.println("  -d '{\"ref\":\"refs/heads/main\",\"author\":\"ada\",\"message\":\"hello\",\"files\":{\"README.md\":\"hello\\n\"}}'");
        System.out.println();
        System.out.println("curl -s 'localhost:" + (base + 1) + "/v1/repos/demo/files?ref=refs/heads/main'");
        System.out.println("curl -s 'localhost:" + (base + 2) + "/v1/repos/demo'");
        System.out.println();
        System.out.println("Ctrl+C to stop.");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> nodes.forEach(NodeServer::close)));
        Thread.currentThread().join();
    }

    private static ObjectStorage openStorage(String storageName, Map<String, String> flags) {
        if ("memory".equals(storageName)) {
            return new MemoryObjectStorage();
        }
        if (!"s3".equals(storageName)) {
            throw new IllegalArgumentException("storage must be memory or s3");
        }
        return new S3ObjectStorage(new S3Config(
                flags.getOrDefault("endpoint", env("S3_ENDPOINT", "http://127.0.0.1:9000")),
                flags.getOrDefault("region", env("S3_REGION", "us-east-1")),
                flags.getOrDefault("bucket", env("S3_BUCKET", "gitstore")),
                flags.getOrDefault("access-key", env("S3_ACCESS_KEY", "minioadmin")),
                flags.getOrDefault("secret-key", env("S3_SECRET_KEY", "minioadmin"))));
    }

    public static Map<String, String> flags(String[] args) {
        Map<String, String> flags = new LinkedHashMap<>();
        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("unexpected argument " + arg);
            }
            String key = arg.substring(2);
            if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
                flags.put(key, "true");
            } else {
                flags.put(key, args[++i]);
            }
        }
        return flags;
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void usage() {
        System.out.println("""
                continuing-gitstore

                node       --port 8080 --node-id node-a --storage memory|s3
                           --endpoint http://127.0.0.1:9000 --bucket gitstore
                           --access-key minioadmin --secret-key minioadmin
                demo       --base-port 8080
                benchmark  [--minio] [--endpoint URL] [--bucket name]
                           [--access-key key] [--secret-key secret]
                """);
    }
}
