package dev.continuing.gitstore.storage;

import dev.continuing.gitstore.GitStoreException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.util.Optional;

/**
 * S3-compatible backend. Path-style access is on so MinIO works without a DNS wildcard.
 * Checksums are sent only when the operation requires them; older MinIO builds reject the
 * default flexible-checksum headers.
 */
public final class S3ObjectStorage implements ObjectStorage, AutoCloseable {
    private final S3Client client;
    private final String bucket;

    public S3ObjectStorage(S3Config config) {
        this.client = S3Client.builder()
                .endpointOverride(URI.create(config.endpoint()))
                .region(Region.of(config.region()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(config.accessKey(), config.secretKey())))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .chunkedEncodingEnabled(false)
                        .build())
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .build();
        this.bucket = config.bucket();
        createBucketWithRetry();
    }

    private void createBucketWithRetry() {
        GitStoreException last = null;
        for (int attempt = 1; attempt <= 30; attempt++) {
            try {
                client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
                return;
            } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException already) {
                return;
            } catch (S3Exception e) {
                if (e.statusCode() == 409) {
                    return;
                }
                last = new GitStoreException(503, "create bucket " + bucket + ": " + e.getMessage(), e);
            } catch (RuntimeException e) {
                last = new GitStoreException(503, "create bucket " + bucket + ": " + e.getMessage(), e);
            }
            try {
                Thread.sleep(1_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw GitStoreException.unavailable("interrupted while creating bucket " + bucket);
            }
        }
        throw last == null ? GitStoreException.unavailable("create bucket " + bucket) : last;
    }

    @Override
    public PutResult put(String key, byte[] body, String ifNoneMatch, String ifMatch) {
        try {
            var request = PutObjectRequest.builder().bucket(bucket).key(key);
            if (ifNoneMatch != null) {
                request.ifNoneMatch(ifNoneMatch);
            } else if (ifMatch != null) {
                request.ifMatch(ifMatch);
            }
            var response = client.putObject(request.build(), RequestBody.fromBytes(body));
            return PutResult.applied(response.eTag());
        } catch (S3Exception e) {
            if (e.statusCode() == 412) {
                return PutResult.rejected(null);
            }
            throw new GitStoreException(502, "put " + key + " failed: HTTP " + e.statusCode(), e);
        }
    }

    @Override
    public Optional<StoredBlob> get(String key) {
        try (var stream = client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())) {
            byte[] body = stream.readAllBytes();
            return Optional.of(new StoredBlob(body, stream.response().eTag()));
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return Optional.empty();
            }
            throw new GitStoreException(502, "get " + key + " failed: HTTP " + e.statusCode(), e);
        } catch (java.io.IOException e) {
            throw new GitStoreException(502, "get " + key + " failed", e);
        }
    }

    @Override
    public Optional<String> head(String key) {
        try {
            var response = client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
            return Optional.ofNullable(response.eTag());
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return Optional.empty();
            }
            throw new GitStoreException(502, "head " + key + " failed: HTTP " + e.statusCode(), e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (S3Exception e) {
            throw new GitStoreException(502, "delete " + key + " failed: HTTP " + e.statusCode(), e);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
