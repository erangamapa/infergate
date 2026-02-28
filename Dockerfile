FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/infergate ./cmd/infergate

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/infergate /infergate
COPY config /config
EXPOSE 7443
ENTRYPOINT ["/infergate", "-config", "/config/gateway.yaml"]
