package dev.continuing.gitstore.storage;

public record S3Config(
        String endpoint,
        String region,
        String bucket,
        String accessKey,
        String secretKey
) {
    public S3Config {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("S3 endpoint is required");
        }
        if (region == null || region.isBlank()) {
            region = "us-east-1";
        }
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("S3 bucket is required");
        }
        if (accessKey == null || secretKey == null) {
            throw new IllegalArgumentException("S3 credentials are required");
        }
    }
}
