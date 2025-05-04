#!/bin/sh

goimports -w=true $(find . -type f -name '*.go' -not -path './vendor/*' )
go mod tidy

mkdir -p release
rm -f release/*

CGO_ENABLED=0 GOARCH=amd64 go build -ldflags="-s -w" -o release/new_names.amd64 -v ./cmd/new_names
CGO_ENABLED=0 GOARCH=arm64 go build -ldflags="-s -w" -o release/new_names.arm64 -v ./cmd/new_names
