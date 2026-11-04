# Community MinIO at the last AGPL tag we could build.
# Prebuilt minio/minio tags are no longer on Docker Hub, and the 2026 AIStor
# binaries refuse S3 calls unless a license is installed.
FROM golang:1.24.8
WORKDIR /src
RUN git clone --depth 1 --branch RELEASE.2025-10-15T17-29-55Z https://github.com/minio/minio.git . \
    && go build -o /usr/bin/minio .
EXPOSE 9000 9001
VOLUME ["/data"]
ENTRYPOINT ["/usr/bin/minio"]
CMD ["server", "/data", "--console-address", ":9001"]
