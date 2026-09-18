FROM --platform=$BUILDPLATFORM golang:1.27 AS build
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
COPY go.* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o /out/rancher-namespace-filter ./cmd/rancher-namespace-filter

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/rancher-namespace-filter /rancher-namespace-filter
EXPOSE 8080
ENTRYPOINT ["/rancher-namespace-filter"]
