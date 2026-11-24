# SPDX-License-Identifier: GPL-3.0-only
.PHONY: all check clean
all:
	mkdir -p bin
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -buildvcs=false -o bin/stqr-windows.exe ./cmd/stqr-windows
check:
	mkdir -p bin
	go test -race ./...
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test -c -o bin/windows-callbacks.test.exe .
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test -c -o bin/windows-setup.test.exe ./cmd/stqr-windows
clean:
	rm -rf bin
