FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /pipeline ./cmd/example-system

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=build /pipeline /pipeline

EXPOSE 8088
ENTRYPOINT ["/pipeline"]
