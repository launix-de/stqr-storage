// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestUIRequestRejectsMalformedAndOversizedInput(t *testing.T) {
	for _, input := range []string{``, `{}`, `{"version":2,"action":"status"}`, `{"version":1,"action":"status","unknown":true}`, `{"version":1,"action":"status"}{}`, strings.Repeat("x", uiMaxRequest+1)} {
		if _, err := readUIRequest(strings.NewReader(input)); err == nil {
			t.Fatal("invalid UI request accepted")
		}
	}
	request, err := readUIRequest(strings.NewReader(`{"version":1,"action":"mount","config":{"uri":"http://storage:7419","username":"alice","password":"not-for-argv","bucket":"upper","lower_bucket":"base","whitelist":["home/"],"drive":"S:","readonly":true}}`))
	if err != nil || request.Config.LowerBucket != "base" || !request.Config.ReadOnly || request.Config.Password != "not-for-argv" {
		t.Fatal(request, err)
	}
}

func TestMountCancellationPreservesFailedFinalSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mountResult(ctx, context.Canceled); err != nil {
		t.Fatal(err)
	}
	failure := errors.Join(context.Canceled, errFinalSync, context.DeadlineExceeded)
	if err := mountResult(ctx, failure); !errors.Is(err, errFinalSync) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("failed barrier hidden by cancellation", err)
	}
}
