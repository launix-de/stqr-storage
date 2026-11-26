// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func TestEffectiveReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name             string
		requested        bool
		permissions      client.BucketPermissions
		readonly, denied bool
	}{
		{"default-writable", false, client.BucketPermissionRead | client.BucketPermissionWrite, false, false},
		{"read-only-grant", false, client.BucketPermissionRead, true, false},
		{"append-is-not-write", false, client.BucketPermissionRead | client.BucketPermissionAppend, true, false},
		{"explicit-read-only", true, client.BucketPermissionRead | client.BucketPermissionWrite, true, false},
		{"no-read-grant", false, client.BucketPermissionWrite, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := effectiveReadOnly(tc.requested, tc.permissions)
			if (err != nil) != tc.denied || got != tc.readonly {
				t.Fatal(got, err)
			}
		})
	}
}
