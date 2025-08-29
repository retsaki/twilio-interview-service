FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG SERVER_PKG=./server
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /bin/server ${SERVER_PKG}

FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /bin/server /usr/local/bin/server
ENV REDIS_ADDR=redis:6379 \
    THRESHOLD=10 \
    WINDOW_HOURS=1 \
    COUNT_TTL_HOURS=48
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/server"]
