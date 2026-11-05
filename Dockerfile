FROM --platform=$BUILDPLATFORM golang:1.27@sha256:a386c44c1c90025ad5bc66feea3fef23052bef3bace3609f7e3e10e1f9314f45 AS build
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
COPY go.* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o /out/drover ./cmd/drover

FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329
COPY --from=build /out/drover /drover
EXPOSE 8080
ENTRYPOINT ["/drover"]
