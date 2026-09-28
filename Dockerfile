# Build stage: pinned toolchain matching the go.mod toolchain directive and CI.
FROM golang:1.27.1-alpine AS build

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
ARG DIRTY=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal

# One statically linked executable with the embedded build identity.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags "-s -w \
      -X github.com/maxp/hookrelay/internal/cli.version=${VERSION} \
      -X github.com/maxp/hookrelay/internal/cli.commit=${COMMIT} \
      -X github.com/maxp/hookrelay/internal/cli.buildTime=${BUILD_TIME} \
      -X github.com/maxp/hookrelay/internal/cli.dirty=${DIRTY}" \
    -o /hookrelay ./cmd/hookrelay

# Runtime: distroless static, nonroot, no shell or package manager.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /hookrelay /hookrelay

ENTRYPOINT ["/hookrelay"]
CMD ["serve"]
