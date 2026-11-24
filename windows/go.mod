module github.com/launix-de/stqr-storage/windows

go 1.24.0

require (
	github.com/launix-de/stqr-storage v0.0.0
	github.com/winfsp/cgofuse v1.6.1-0.20260531120352-2fa812d1bdc7
	golang.org/x/sys v0.32.0
	golang.org/x/term v0.31.0
)

require (
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/hanwen/go-fuse/v2 v2.12.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/launix-de/stqr-storage => ..
