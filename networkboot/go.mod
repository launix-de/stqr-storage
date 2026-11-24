module github.com/launix-de/stqr-storage/networkboot

go 1.24.0

require (
	github.com/go-git/go-billy/v5 v5.6.0
	github.com/launix-de/stqr-storage v0.0.0
	github.com/willscott/go-nfs v0.0.4
	github.com/willscott/go-nfs-client v0.0.0-20240104095149-b44639837b00
)

require (
	github.com/cyphar/filepath-securejoin v0.2.5 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/hanwen/go-fuse/v2 v2.12.1 // indirect
	github.com/rasky/go-xdr v0.0.0-20170124162913-1a41d1a06c93 // indirect
	golang.org/x/sys v0.32.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/launix-de/stqr-storage => ..
