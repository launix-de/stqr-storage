# SPDX-License-Identifier: GPL-3.0-only
.PHONY: all check resources clean
all:
	mkdir -p bin
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -buildvcs=false -ldflags="-H=windowsgui" -o bin/stqr-windows.exe ./cmd/stqr-windows
check:
	mkdir -p bin
	go test -race ./...
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test -c -o bin/windows-callbacks.test.exe .
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test -c -o bin/windows-setup.test.exe ./cmd/stqr-windows
clean:
	rm -rf bin

# Regenerate the checked-in manifest resource after editing app.manifest.
resources:
	mkdir -p bin
	cd cmd/stqr-windows && llvm-rc /fo ../../bin/app.res app.rc
	llvm-cvtres /machine:x64 /out:cmd/stqr-windows/resource_windows_amd64.syso bin/app.res
