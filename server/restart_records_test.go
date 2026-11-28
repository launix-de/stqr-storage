// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func restartRecordFixture(t *testing.T) (string, restartIntent, restartManifest) {
	t.Helper()
	dir := t.TempDir()
	handoff, cluster, node, id := volume.ID{1}, volume.ID{2}, volume.ID{3}, volume.ID{4}
	intent := restartIntent{Schema: 1, Handoff: handoff, Volumes: []volume.ID{id}}
	m := restartManifest{Schema: 1, Handoff: handoff, Cluster: cluster, Node: node, Boot: client.ID{5}, Volumes: []volume.RestartReceipt{{Handoff: handoff, Cluster: cluster, Node: node, Volume: id, BaselineGeneration: 10, Generation: 20, HeaderDigest: [32]byte{6}}}}
	return dir, intent, m
}
func saveRestartFixture(t *testing.T, dir string, intent restartIntent, m restartManifest) {
	t.Helper()
	if err := writeRestartRecord(filepath.Join(restartDirectory(dir, volume.ID{2}, volume.ID{3}), "required"), intent); err != nil {
		t.Fatal(err)
	}
	if err := writeRestartRecord(filepath.Join(restartDirectory(dir, volume.ID{2}, volume.ID{3}), "current"), m); err != nil {
		t.Fatal(err)
	}
}
func TestRestartRetentionRecordsDynamicBoots(t *testing.T) {
	dir, intent, m := restartRecordFixture(t)
	for i := 1; i <= 1025; i++ {
		var boot client.ID
		boot[0], boot[1] = byte(i), byte(i>>8)
		m.Prefixes = append(m.Prefixes, restartPrefix{Boot: boot, Bucket: volume.ID{9}, Sequence: 7, Volumes: []restartVolumeCut{{Volume: volume.ID{4}, Generation: 15}}})
	}
	saveRestartFixture(t, dir, intent, m)
	got, err := loadRestartState(dir, m.Cluster, m.Node)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.prefixes) != 1025 {
		t.Fatal("dynamic old-boot evidence rejected", len(got.prefixes))
	}
	source := got.source(volume.ID{4})
	if source == nil || source.Receipt != m.Volumes[0] || filepath.Dir(source.Path) != filepath.Join(restartDirectory(dir, m.Cluster, m.Node), m.Handoff.String()) {
		t.Fatal("image not bound to manifest handoff")
	}
}
func TestRestartRetentionRecordsRejectUnprovenState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*restartIntent, *restartManifest)
	}{
		{"wrong-node", func(_ *restartIntent, m *restartManifest) { m.Node[0]++ }},
		{"incomplete-handoff", func(i *restartIntent, _ *restartManifest) { i.Handoff[0]++ }},
		{"missing-participant", func(_ *restartIntent, m *restartManifest) { m.Volumes = nil }},
		{"duplicate-participant", func(i *restartIntent, m *restartManifest) {
			i.Volumes = append(i.Volumes, i.Volumes[0])
			m.Volumes = append(m.Volumes, m.Volumes[0])
		}},
		{"duplicate-prefix", func(_ *restartIntent, m *restartManifest) {
			p := restartPrefix{Boot: client.ID{1}, Sequence: 2, Confirmed: 2}
			m.Prefixes = []restartPrefix{p, p}
		}},
		{"no-volume-proof", func(_ *restartIntent, m *restartManifest) {
			m.Prefixes = []restartPrefix{{Boot: client.ID{1}, Sequence: 2}}
		}},
		{"future-generation", func(_ *restartIntent, m *restartManifest) {
			m.Prefixes = []restartPrefix{{Boot: client.ID{1}, Sequence: 2, Volumes: []restartVolumeCut{{Volume: volume.ID{4}, Generation: 21}}}}
		}},
		{"false-confirmation", func(_ *restartIntent, m *restartManifest) {
			m.Prefixes = []restartPrefix{{Boot: client.ID{1}, Sequence: 2, Confirmed: 3}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, i, m := restartRecordFixture(t)
			cluster, node := m.Cluster, m.Node
			tc.mutate(&i, &m)
			saveRestartFixture(t, dir, i, m)
			if _, err := loadRestartState(dir, cluster, node); !errors.Is(err, volume.ErrRecovery) {
				t.Fatal("invalid handoff accepted", err)
			}
		})
	}
}
func TestRestartRetentionRecordsMissingTornAndUnavailable(t *testing.T) {
	dir, i, m := restartRecordFixture(t)
	if _, err := loadRestartState(dir, m.Cluster, m.Node); err != nil {
		t.Fatal("first startup rejected", err)
	}
	if err := writeRestartRecord(filepath.Join(restartDirectory(dir, m.Cluster, m.Node), "required"), i); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRestartState(dir, m.Cluster, m.Node); !errors.Is(err, volume.ErrRecovery) {
		t.Fatal("missing required manifest accepted", err)
	}
	m.Prefixes = []restartPrefix{{Boot: client.ID{1}, Sequence: 2, Unavailable: true, Volumes: []restartVolumeCut{{Volume: volume.ID{99}, Generation: 100}}}}
	saveRestartFixture(t, dir, i, m)
	if _, err := loadRestartState(dir, m.Cluster, m.Node); err != nil {
		t.Fatal("unavailable proof must remain nonconfirmable metadata", err)
	}
	path := filepath.Join(restartDirectory(dir, m.Cluster, m.Node), "current")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(9); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err = loadRestartState(dir, m.Cluster, m.Node); !errors.Is(err, volume.ErrRecovery) {
		t.Fatal("torn required manifest accepted", err)
	}
}

func TestRestartRetentionReclaimsOnlyObsoletePrivateImages(t *testing.T) {
	dir := t.TempDir()
	current, old, id := volume.ID{1}, volume.ID{2}, volume.ID{3}
	for _, handoff := range []volume.ID{current, old} {
		path := filepath.Join(dir, handoff.String())
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, id.String()+".image"), []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	foreign := filepath.Join(dir, volume.ID{4}.String())
	if err := os.Mkdir(foreign, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, "keep"), []byte("unrelated"), 0600); err != nil {
		t.Fatal(err)
	}
	r := &restartState{directory: dir, manifest: &restartManifest{Handoff: current}}
	r.reclaimObsoleteImages()
	if _, err := os.Stat(filepath.Join(dir, old.String())); !os.IsNotExist(err) {
		t.Fatal("obsolete private images not reclaimed", err)
	}
	for _, path := range []string{filepath.Join(dir, current.String(), id.String()+".image"), filepath.Join(foreign, "keep")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("cleanup removed retained or unrelated data", err)
		}
	}
}

type restartDecodeProbe struct{ calls int }

func (p *restartDecodeProbe) UnmarshalJSON([]byte) error { p.calls++; return nil }
func TestRestartRetentionRecordChecksSealBeforeAllocatingJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record")
	raw := append([]byte(`{"large":"value"}`), make([]byte, 32)...)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var probe restartDecodeProbe
	if err := readRestartRecord(path, &probe); !errors.Is(err, volume.ErrCorrupt) || probe.calls != 0 {
		t.Fatal("unsealed JSON reached allocating decoder", probe.calls, err)
	}
}
