// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"crypto/sha1"
	"crypto/sha256"
	"hash"
	"hash/crc32"
	"hash/crc64"
)

// NVME uses reflected polynomial 0x9a6c9329ac4bc9b5, all-ones init/xorout.
// hash/crc64 already applies the required initial/final complement.
func S3Checksum(algorithm string) (hash.Hash, error) {
	switch algorithm {
	case "CRC32":
		return crc32.NewIEEE(), nil
	case "CRC32C":
		return crc32.New(crc32.MakeTable(crc32.Castagnoli)), nil
	case "CRC64NVME":
		return crc64.New(crc64.MakeTable(0x9a6c9329ac4bc9b5)), nil
	case "SHA1":
		return sha1.New(), nil
	case "SHA256":
		return sha256.New(), nil
	}
	return nil, ErrRange
}
