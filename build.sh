#!/bin/sh

goimports -w=true $(find . -type f -name '*.go' -not -path './vendor/*' )
go mod tidy

rm -f release/*/*
mkdir -p release/amd64 release/arm64
CGO_ENABLED=0 GOARCH=amd64 go build -ldflags="-s -w" -o release/amd64/anonymizer -v ./cmd/anonymizer
CGO_ENABLED=0 GOARCH=arm64 go build -ldflags="-s -w" -o release/arm64/anonymizer -v ./cmd/anonymizer
