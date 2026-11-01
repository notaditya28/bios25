# Hydra Stream broker image (local testing only — the brief allows Docker for that, nothing else).
# One image holds every command; docker-compose.yml picks which one runs.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.20
WORKDIR /app
COPY --from=build /out/ /usr/local/bin/
COPY configs ./configs
ENTRYPOINT ["hydra-broker"]
