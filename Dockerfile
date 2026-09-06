FROM golang:1.27.1-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rpc-proxy ./cmd/rpc-proxy

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/rpc-proxy /rpc-proxy
COPY deploy/config.yaml /etc/rpc-proxy/config.yaml
USER nonroot:nonroot
ENTRYPOINT ["/rpc-proxy"]
CMD ["-config", "/etc/rpc-proxy/config.yaml"]
