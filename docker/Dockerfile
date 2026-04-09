FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go.mod and download deps first (cache layer)
COPY go.mod go.sum ./
RUN go mod download

# Copy source (including pre-generated .pb.go files)
COPY . .

# Build all binaries
RUN CGO_ENABLED=0 go build -o /bin/namenode ./cmd/namenode && \
    CGO_ENABLED=0 go build -o /bin/datanode ./cmd/datanode && \
    CGO_ENABLED=0 go build -o /bin/resourcemanager ./cmd/resourcemanager && \
    CGO_ENABLED=0 go build -o /bin/nodemanager ./cmd/nodemanager && \
    CGO_ENABLED=0 go build -o /bin/hdfs ./cmd/hdfs && \
    CGO_ENABLED=0 go build -o /bin/mapreduce ./cmd/mapreduce && \
    CGO_ENABLED=0 go build -o /bin/mapworker ./cmd/mapworker

# --- Runtime image ---
FROM alpine:3.20

RUN apk add --no-cache bash curl

COPY --from=builder /bin/namenode /usr/local/bin/
COPY --from=builder /bin/datanode /usr/local/bin/
COPY --from=builder /bin/resourcemanager /usr/local/bin/
COPY --from=builder /bin/nodemanager /usr/local/bin/
COPY --from=builder /bin/hdfs /usr/local/bin/
COPY --from=builder /bin/mapreduce /usr/local/bin/
COPY --from=builder /bin/mapworker /usr/local/bin/

# Create non-root user
RUN adduser -D -u 1001 hadoop

# Create data directories
RUN mkdir -p /data/namenode /data/datanode /data/temp && \
    chown -R hadoop:hadoop /data

USER hadoop

EXPOSE 9000 9001 9010 9011 9020
