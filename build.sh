#!/bin/sh

goimports -w=true $(find . -type f -name '*.go' -not -path './vendor/*' )
go mod tidy

mkdir -p release
rm -f release/*

CGO_ENABLED=0 GOARCH=amd64 go build -ldflags="-s -w" -o release/anonymizer.amd64 -v ./cmd/anonymizer
CGO_ENABLED=0 GOARCH=arm64 go build -ldflags="-s -w" -o release/anonymizer.arm64 -v ./cmd/anonymizer
