#!/bin/sh
# SPDX-License-Identifier: GPL-3.0-only
set -eu
umask 022
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
version=${1:-}
if [ -z "$version" ]; then
    echo "Usage: $0 VERSION [OUTPUT_DIRECTORY] (GO, DEB_ARCH optional)" >&2
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
mkdir -p "$stage/DEBIAN" "$stage/usr/bin" "$stage/usr/lib/systemd/system" "$stage/usr/share/doc/stqr"
cd "$root"
CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" "${GO:-go}" build -buildvcs=false -trimpath -ldflags "-s -w -X github.com/launix-de/stqr-storage/server.SoftwareVersion=$version -X github.com/launix-de/stqr-storage/server.SoftwareRevision=$revision" -o "$stage/usr/bin/stqr" .
cp packaging/debian/stqr.service "$stage/usr/lib/systemd/system/"
cp packaging/debian/copyright "$stage/usr/share/doc/stqr/copyright"
mkdir -p "$stage/usr/share/man/man1" "$stage/usr/share/lintian/overrides"
gzip -n -9 -c packaging/debian/stqr.1 > "$stage/usr/share/man/man1/stqr.1.gz"
cp packaging/debian/lintian-overrides "$stage/usr/share/lintian/overrides/stqr"
# Retain the actual linked dependencies' notices, including the Go runtime.
mkdir -p "$stage/usr/share/doc/stqr/third-party/go"
install -m 0644 "$("${GO:-go}" env GOROOT)/LICENSE" "$stage/usr/share/doc/stqr/third-party/go/"
GOOS=linux GOARCH="$goarch" "${GO:-go}" list -buildvcs=false -deps -f '{{with .Module}}{{.Path}}|{{.Dir}}{{end}}' . | sort -u > "$stage/modules"
while IFS='|' read -r module module_dir; do
    if [ -z "$module" ] || [ "$module" = github.com/launix-de/stqr-storage ]; then continue; fi
    for notice in "$module_dir"/LICENSE "$module_dir"/LICENSE.* "$module_dir"/COPYING "$module_dir"/COPYING.* "$module_dir"/NOTICE "$module_dir"/NOTICE.*; do
        if [ -f "$notice" ]; then
            mkdir -p "$stage/usr/share/doc/stqr/third-party/$module"
            install -m 0644 "$notice" "$stage/usr/share/doc/stqr/third-party/$module/"
        fi
    done
done < "$stage/modules"
rm "$stage/modules"
changelog_date=$(git -C "$root" show -s --format=%aD HEAD)
printf 'stqr (%s) unstable; urgency=medium\n\n  * Build revision %s.\n\n -- Launix <info@launix.de>  %s\n' "$version" "$revision" "$changelog_date" | gzip -n -9 > "$stage/usr/share/doc/stqr/changelog.gz"
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
Description: Distributed object storage node and administrative dashboard
 Native HTTP/WebSocket storage daemon, managed by stqr.service.
CONTROL
# SOURCE_DATE_EPOCH also makes repeated builds from the same source reproducible.
SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$release} dpkg-deb --root-owner-group --build "$stage" "$out/stqr_${version#*:}_${arch}.deb"
