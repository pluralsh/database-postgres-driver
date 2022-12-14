FROM golang:1.18.2-alpine3.15 as builder

ARG TARGETARCH

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY /cmd/main.go main.go
COPY /cmd/cmd.go cmd.go
COPY /pkg pkg/


# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} GO111MODULE=on go build -a -o database-postgres-driver main.go cmd.go

FROM gcr.io/distroless/static:nonroot
WORKDIR /

COPY --from=builder /workspace/database-postgres-driver .
USER 65532:65532
ENTRYPOINT ["/database-postgres-driver"]