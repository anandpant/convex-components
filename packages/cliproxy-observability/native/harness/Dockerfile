FROM golang:1.26-bookworm@sha256:a688600ca24f8a4d3ca77f95b0dd40704a9fc787c826660eb7ba0b641b8b175d AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build go test -race ./... && go build -buildmode=c-shared -o /build/cliproxy-capture.so ./plugin && go build -o /build/cliproxy-capture-exporter ./exporter

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl nginx python3 procps sqlite3 && rm -rf /var/lib/apt/lists/*
ARG TARGETARCH
RUN case "$TARGETARCH" in \
 amd64) arch=amd64; digest=d8ea443f95977fda52d6b86c4f07dbd3fb448ca05574212951ec3c42b903f407 ;; \
 arm64) arch=aarch64; digest=49e905fc746cf1db2904ff0de52d14c52bde395e173b7a3d61f11224b62febc0 ;; \
 *) exit 1 ;; esac && \
 curl -fsSL "https://github.com/router-for-me/CLIProxyAPI/releases/download/v7.3.5/CLIProxyAPI_7.3.5_linux_${arch}.tar.gz" -o /tmp/stock.tar.gz && \
 echo "$digest  /tmp/stock.tar.gz" | sha256sum -c - && mkdir /stock && tar xzf /tmp/stock.tar.gz -C /stock && rm /tmp/stock.tar.gz
COPY --from=build /build/ /capture/
COPY harness/ /harness/
WORKDIR /harness
ENTRYPOINT ["python3", "/harness/proof.py"]
