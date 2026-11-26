// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"fmt"

	"github.com/launix-de/stqr-storage/client"
)

func effectiveReadOnly(requested bool, permissions client.BucketPermissions) (bool, error) {
	if permissions&client.BucketPermissionRead == 0 {
		return false, fmt.Errorf("Keine Leseberechtigung für dieses Bucket")
	}
	return requested || permissions&client.BucketPermissionWrite == 0, nil
}
