#!/bin/sh
# SPDX-License-Identifier: GPL-3.0-only
set -eu
umask 022
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
version=${1:-}
if [ -z "$version" ]; then
    echo "Usage: $0 VERSION [OUTPUT_DIRECTORY] (GO, DEB_ARCH, STQR_RELEASE optional)" >&2
    exit 2
fi
command -v dpkg-deb >/dev/null
command -v dpkg-architecture >/dev/null
# Validate before using a version in paths, control data or linker arguments.
dpkg --validate-version "$version" >/dev/null
case "$version" in *[!a-zA-Z0-9.+:~_-]*) echo 'Unsupported version characters' >&2; exit 2;; esac
arch=${DEB_ARCH:-$(dpkg-architecture -qDEB_HOST_ARCH)}
case "$arch" in amd64) goarch=amd64;; arm64) goarch=arm64;; *) echo "Unsupported architecture: $arch" >&2; exit 2;; esac
out=${2:-"$root/dist"}
mkdir -p "$out"
out=$(CDPATH= cd -- "$out" && pwd)
stage=$(mktemp -d)
chmod 0755 "$stage"
trap 'rm -rf "$stage"' EXIT HUP INT TERM
revision=$(git -C "$root" rev-parse HEAD)
release=${STQR_RELEASE:-$(git -C "$root" show -s --format=%ct HEAD)}
case "$release" in ''|*[!0-9]*) echo 'STQR_RELEASE must be an increasing integer' >&2; exit 2;; esac
mkdir -p "$stage/DEBIAN" "$stage/usr/bin" "$stage/usr/lib/systemd/system" "$stage/usr/share/doc/stqr"
cd "$root"
CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" "${GO:-go}" build -buildvcs=false -trimpath -ldflags "-s -w -X github.com/launix-de/stqr-storage/server.SoftwareVersion=$version -X github.com/launix-de/stqr-storage/server.SoftwareRevision=$revision -X github.com/launix-de/stqr-storage/server.SoftwareRelease=$release" -o "$stage/usr/bin/stqr" .
cp packaging/debian/stqr.service "$stage/usr/lib/systemd/system/"
cp LICENSE.md "$stage/usr/share/doc/stqr/copyright"
cp docs/debian.md "$stage/usr/share/doc/stqr/"
for script in preinst postinst prerm postrm; do
    cp "packaging/debian/$script" "$stage/DEBIAN/$script"
    chmod 755 "$stage/DEBIAN/$script"
done
cat > "$stage/DEBIAN/control" <<CONTROL
Package: stqr
Version: $version
Section: admin
Priority: optional
Architecture: $arch
Maintainer: Launix <info@launix.de>
Depends: adduser, init-system-helpers (>= 1.54~)
Recommends: systemd
Installed-Size: $(du -sk "$stage/usr" | awk '{print $1}')
Homepage: https://github.com/launix-de/stqr-storage
Description: STQR object storage node and administrative dashboard
 Native HTTP/WebSocket storage daemon, managed by stqr.service.
CONTROL
# SOURCE_DATE_EPOCH also makes repeated builds from the same source reproducible.
SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$release} dpkg-deb --root-owner-group --build "$stage" "$out/stqr_${version#*:}_${arch}.deb"
