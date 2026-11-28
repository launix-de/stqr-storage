// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/launix-de/stqr-storage/client"
)

// Software contract for rolling upgrades:
//
//   - Build identity is informational. ProtocolVersion is assigned by hand;
//     software hashes, timestamps and package releases never order protocols.
//   - stqr.v1 upgrade, HELLO, NODE_HELLO and this bounded HTTP discovery header
//     remain a stable bootstrap. Missing headers mean the original v1 profile.
//     Claims become telemetry only AFTER authentication AND NODE_HELLO binding.
//   - Wire ranges describe envelope compatibility. No overlap makes operational
//     traffic unavailable, exactly like SHORT downtime: NEVER drain, exclude,
//     reduce required replicas, acknowledge unperformed writes or move ownership.
//   - Each opcode independently declares emitted semantics and accepted revisions.
//     Both directions must understand its request AND reply. Check before decoding
//     the body or changing state, including reverse requests on outgoing sockets.
//     An incompatible operation returns 426; compatible reads, PING and telemetry
//     continue. New optional opcodes require explicit advertisement; legacy peers
//     imply revision 1 ONLY for opcodes that already existed at this checkpoint.
//   - Introducing e.g. bucket COW MUST declare affected opcodes here in the SAME
//     change. Keep safe GET semantics at revision 1 where possible, raise mutation,
//     migration/control/replica semantics wherever old code could misinterpret COW.
//     Never infer that an old reader supports a new on-disk object from GET alone.
//   - Wire compatibility does NOT imply storage-format compatibility. Every local
//     decoder must reject unknown required format/features BEFORE interpreting or
//     modifying records (including startup/recovery). Do not publish a format or
//     commit a cluster-wide format activation until ALL required owners support it.
//     A rejected upgrade must leave old bytes intact. Conversion needs a separate,
//     explicit format gate and crash-safe migration; negotiation cannot supply it.
//   - Being older is not a blanket read-only mode. Serve operations whose declared
//     semantics AND actual bucket/volume format are understood. Unsafe operations
//     fail explicitly, including fire-and-forget (no effects, no invented ACK).
//   - This gate must ship BEFORE the first incompatible change: software predating
//     it cannot be made safe retroactively. Fence such binaries before activation.
//
// baselineOpcode freezes pre-COW semantics. currentOperationSemantics explicitly
// advertises the unversioned/COW changes; future formats must extend both their
// real decoder/activation gates and these per-message declarations.
const compatibilityHeader = "X-STQR-Compatibility"
const nodeVersionsMessage uint16 = 0x203a

// Set by the Debian build script through -ldflags. Developer builds remain usable.
var SoftwareVersion = "dev"
var SoftwareRevision = "unknown"

// ProtocolVersion is assigned by hand when peer protocol semantics change.
// Never derive it from build time, commit count, package version or Git hash.
// v3 includes the current unversioned control and bucket-lease semantics.
const ProtocolVersion uint64 = 3

type operationSemantics struct {
	Emit uint16 `json:"emit"`
	Min  uint16 `json:"min"`
	Max  uint16 `json:"max"`
}
type protocolProfile struct {
	ProtocolVersion uint64                        `json:"protocol_version,omitempty"`
	Version         string                        `json:"version"`
	Revision        string                        `json:"revision"`
	Release         uint64                        `json:"release"`
	WireMin         uint16                        `json:"wire_min"`
	WireMax         uint16                        `json:"wire_max"`
	Operations      map[string]operationSemantics `json:"operations,omitempty"`
}
type nodeVersionStatus struct {
	Profile      protocolProfile `json:"profile"`
	Outdated     bool            `json:"outdated"`
	Incompatible bool            `json:"incompatible"`
	Restricted   bool            `json:"restricted"`
	Reason       string          `json:"reason,omitempty"`
}

// Mutable observations are process-local diagnostics, never committed cluster state.
type versionObservations struct {
	sync.Mutex
	peers map[string]protocolProfile
}

func legacyProfile() protocolProfile {
	return protocolProfile{Version: "legacy/unknown", WireMin: 1, WireMax: 1}
}
func (s *Server) protocolVersion() protocolProfile {
	if p := s.compatibility.Load(); p != nil {
		return *p
	}
	// Release is retained only as a wire alias for older readers. It is no
	// longer a software release or a timestamp and never drives our comparison.
	return protocolProfile{ProtocolVersion: ProtocolVersion, Version: SoftwareVersion, Revision: SoftwareRevision, Release: ProtocolVersion, WireMin: 1, WireMax: 1, Operations: currentOperationSemantics()}
}
func profileHeader(p protocolProfile) string { b, _ := json.Marshal(p); return string(b) }
func decodeProfile(h http.Header) (protocolProfile, error) {
	raw := h.Get(compatibilityHeader)
	if raw == "" {
		return legacyProfile(), nil
	}
	if len(raw) > 8192 {
		return protocolProfile{}, fmt.Errorf("compatibility header too large")
	}
	var p protocolProfile
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return p, fmt.Errorf("invalid compatibility header")
	}
	if len(p.Version) == 0 || len(p.Version) > 128 || len(p.Revision) > 128 || p.WireMin == 0 || p.WireMax < p.WireMin || len(p.Operations) > 128 {
		return p, fmt.Errorf("invalid compatibility profile")
	}
	for k, v := range p.Operations {
		op, err := strconv.ParseUint(k, 16, 16)
		if err != nil || fmt.Sprintf("%04x", op) != k || v.Min == 0 || v.Max < v.Min || v.Emit < v.Min || v.Emit > v.Max {
			return p, fmt.Errorf("invalid operation semantics")
		}
	}
	return p, nil
}
func normalizeProfile(p protocolProfile) protocolProfile {
	if p.WireMin == 0 {
		return legacyProfile()
	}
	return p
}
func wireCompatible(a, b protocolProfile) bool {
	a, b = normalizeProfile(a), normalizeProfile(b)
	return a.WireMin <= b.WireMax && b.WireMin <= a.WireMax
}
func (p protocolProfile) semantics(op uint16) operationSemantics {
	if len(p.Operations) > 0 {
		if v, ok := p.Operations[fmt.Sprintf("%04x", op)]; ok {
			return v
		}
	}
	if baselineOpcode(op) {
		return operationSemantics{Emit: 1, Min: 1, Max: 1}
	}
	return operationSemantics{}
}
func checkPeerOperation(local, remote protocolProfile, op uint16) error {
	// Stable bootstrap is still usable to diagnose a completely incompatible node.
	if op == client.Hello || op == client.NodeHelloMessage || op == client.Ping {
		return nil
	}
	if !wireCompatible(local, remote) {
		return fail(426, "incompatible peer wire protocol; node retained in SHORT-like unavailable state")
	}
	a, b := local.semantics(op), remote.semantics(op)
	if a.Emit == 0 || b.Emit == 0 || a.Emit < b.Min || a.Emit > b.Max || b.Emit < a.Min || b.Emit > a.Max {
		return fail(426, fmt.Sprintf("incompatible semantics for opcode 0x%04x", op))
	}
	return nil
}
func (s *Server) rememberVersion(id client.ID, p protocolProfile) {
	s.versions.Lock()
	if s.versions.peers == nil {
		s.versions.peers = make(map[string]protocolProfile)
	}
	prior, exists := s.versions.peers[id.String()]
	changed := !exists || profileHeader(prior) != profileHeader(p)
	if changed && (exists || len(s.versions.peers) < 10000) {
		s.versions.peers[id.String()] = normalizeProfile(p)
	}
	s.versions.Unlock()
	if changed {
		s.notifyStats()
	}
}
func (s *Server) versionsView() map[string]nodeVersionStatus {
	local := s.protocolVersion()
	s.versions.Lock()
	profiles := make(map[string]protocolProfile, len(s.versions.peers)+1)
	for id, p := range s.versions.peers {
		profiles[id] = p
	}
	s.versions.Unlock()
	profiles[s.running.Node.ID] = local
	newest := local.ProtocolVersion
	for _, p := range profiles {
		newest = max(newest, p.ProtocolVersion)
	}
	result := make(map[string]nodeVersionStatus, len(profiles))
	for id, p := range profiles {
		// A legacy profile without the manual field is unknown, not ordered by
		// its old build-derived release. Opcode/wire safety checks still apply.
		v := nodeVersionStatus{Profile: p, Outdated: p.ProtocolVersion != 0 && p.ProtocolVersion < newest, Incompatible: !wireCompatible(local, p)}
		if v.Incompatible {
			v.Reason = "Kein gemeinsames Wire-Protokoll; wartet auf kompatible Version"
		}
		for key := range local.Operations {
			n, _ := strconv.ParseUint(key, 16, 16)
			if checkPeerOperation(local, p, uint16(n)) != nil {
				v.Restricted = true
			}
		}
		for key := range p.Operations {
			n, _ := strconv.ParseUint(key, 16, 16)
			if checkPeerOperation(local, p, uint16(n)) != nil {
				v.Restricted = true
			}
		}
		if !v.Incompatible && v.Restricted {
			v.Reason = "Einzelne Requests erfordern andere Semantik"
		}
		result[id] = v
	}
	return result
}

// Push the complete small status view to dashboard subscribers. Bounded queues
// never block storage; a full queue retries on the next ordinary stats publish.
func (s *Server) publishVersions() {
	body := jsonText(s.versionsView())
	wire := client.EncodeMessage(client.Message{Kind: client.Push, Opcode: nodeVersionsMessage, Body: body})
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	for ws := range s.sockets {
		if ws.authorization == nil || !ws.authorization.admin || ws.authorization.invalid.Load() {
			continue
		}
		ws.subMu.Lock()
		if ws.versionsSubscribed && ws.versionSent != string(body) {
			select {
			case ws.responses <- wire:
				ws.versionSent = string(body)
			default:
			}
		}
		ws.subMu.Unlock()
	}
}

// Wrappers retain the semantics of their inner request. Adding another wrapper
// requires extending this function; trusting the outer opcode alone is unsafe.
func checkPeerMessage(local, remote protocolProfile, m client.Message) error {
	if err := checkPeerOperation(local, remote, m.Opcode); err != nil {
		return err
	}
	d := client.Decoder{B: m.Body}
	var inner uint16
	switch m.Opcode {
	case client.PeerAdminRequest, peerBucketCopy:
		d.ID()
		d.Text()
		d.ID()
		inner = d.U16()
		d.Blob()
	case peerForceScopeEnvelope:
		d.U16()
		d.Blob()
		inner = d.U16()
		d.Blob()
	case peerForceAdmin:
		schema := d.U16()
		inner = client.ForceDisconnect
		if schema == 2 {
			inner = d.U16()
		} else if schema != 1 {
			return nil
		}
		d.ID()
		d.Text()
		d.ID()
		d.Blob()
	default:
		return nil
	}
	if d.Done() != nil {
		return nil
	} // Original decoder rejects malformed wrappers.
	return checkPeerOperation(local, remote, inner)
}

func (s *Server) issuerProfile(id client.ID) protocolProfile {
	if id.String() == s.running.Node.ID {
		return s.protocolVersion()
	}
	s.versions.Lock()
	defer s.versions.Unlock()
	if p, ok := s.versions.peers[id.String()]; ok {
		return p
	}
	return legacyProfile()
}

// Frozen pre-COW registry. Do NOT add future opcodes: advertise them explicitly
// in Operations on both implementations instead. Unknown = unsupported.
func baselineOpcode(op uint16) bool {
	switch op {
	case 0x0001, 0x0002, 0x0004, 0x0008, 0x0009, 0x000a, 0x000b, 0x000c, 0x0100, 0x0101:
		return true
	case 0x0102, 0x0103, 0x0104, 0x0105, 0x0106, 0x0107, 0x0108, 0x010b, 0x010c, 0x0110:
		return true
	case 0x0111, 0x0112, 0x0113, 0x0114, 0x0115, 0x0116, 0x0117, 0x0118, 0x0119, 0x011a:
		return true
	case 0x011b, 0x011c, 0x011d, 0x1000, 0x1004, 0x1007, 0x101c, 0x101d, 0x101e, 0x101f:
		return true
	case 0x1020, 0x1036, 0x1037, 0x1038, 0x1040, 0x1041, 0x1042, 0x1043, 0x1044, 0x1045:
		return true
	case 0x1046, 0x1047, 0x1048, 0x1049, 0x104a, 0x104b, 0x104c, 0x104d, 0x104e, 0x1050:
		return true
	case 0x1051, 0x1052, 0x1053, 0x1054, 0x1055, 0x1056, 0x1057, 0x1058, 0x1059, 0x105a:
		return true
	case 0x105b, 0x105c, 0x105d, 0x105e, 0x105f, 0x1060, 0x1061, 0x1062, 0x1063, 0x1064:
		return true
	case 0x1065, 0x1066, 0x1067, 0x1068, 0x1069, 0x1070, 0x1071, 0x1072, 0x1073, 0x1074:
		return true
	case 0x1075, 0x1076, 0x1077, 0x1088, 0x1089, 0x2000, 0x2001, 0x2002, 0x2003, 0x2004:
		return true
	case 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200a, 0x200b, 0x200c, 0x200d, 0x200e:
		return true
	case 0x2010, 0x2011, 0x2012, 0x2013, 0x2014, 0x2015, 0x2016, 0x2017, 0x2019, 0x201a:
		return true
	case 0x201b, 0x201c, 0x201d, 0x201e, 0x201f, 0x2020, 0x2021, 0x2022, 0x2023, 0x2024:
		return true
	case 0x2025, 0x2026, 0x2027, 0x2028, 0x2029, 0x202a, 0x2030, 0x2031, 0x2032, 0x2033:
		return true
	case 0x2034, 0x2035, 0x2036, 0x2037, 0x2038, 0x2039:
		return true
	}
	return false
}

// The envelope and discovery remain v1. Unversioned object payloads, byte CAS,
// clone-aware control/state transfer and subscriptions use semantics revision 2.
// Safe unchanged messages (PING, stats, directory, config) remain revision 1.
func currentOperationSemantics() map[string]operationSemantics {
	result := map[string]operationSemantics{}
	result[fmt.Sprintf("%04x", client.PlacementHintsMessage)] = operationSemantics{Emit: 1, Min: 1, Max: 1}
	result[fmt.Sprintf("%04x", client.ConnectionSync)] = operationSemantics{Emit: 1, Min: 1, Max: 1}
	result[fmt.Sprintf("%04x", client.BucketFormatOpen)] = operationSemantics{Emit: 1, Min: 1, Max: 1}
	result[fmt.Sprintf("%04x", peerBucketFormat)] = operationSemantics{Emit: 1, Min: 1, Max: 1}
	for _, op := range []uint16{0x0100, 0x0101, 0x0102, 0x0103, 0x0104, 0x0105, 0x0106, 0x0107, 0x0108, client.ObjectSubscribe, client.ObjectUpdateMessage, client.BucketScan, client.BucketObjectList, client.ObjectAccepted} {
		result[fmt.Sprintf("%04x", op)] = operationSemantics{Emit: 2, Min: 2, Max: 2}
	}

	for _, op := range []uint16{client.BucketClone, client.BucketClear, client.BucketCreate, client.BucketCreateInGroup, client.BucketSetPolicy, client.BucketDelete, client.PeerControlReserve, client.PeerControlPrepare, client.PeerControlCommit, client.PeerControlApply, client.PeerControlRelease, client.PeerControlAbort, peerControlInfo, peerSystemChunk, peerRangeObject, peerRangeProbe, peerRangeReceipts, peerReplicaPrepare, peerReplicaReady, peerReplicaCommit, peerReplicaCommitted, peerReplicaAbort, peerReplicaChunk, peerReplicaForget, client.ObjectReference} {
		result[fmt.Sprintf("%04x", op)] = operationSemantics{Emit: 2, Min: 2, Max: 2}
	}
	for op := uint16(0x1050); op <= 0x105f; op++ {
		result[fmt.Sprintf("%04x", op)] = operationSemantics{Emit: 2, Min: 2, Max: 2}
	}
	for _, op := range []uint16{peerPGRebuildRequest, peerPGRebuildState, peerPGRebuildArchive, peerPGRebuildCodec, peerPGRebuildRanges, peerBucketPurge, peerBucketCodec, peerBucketInventory, peerReplicaTopologyReady, peerReplicaTopologyCleanup, peerReplicaAbortHistory, peerBucketLease, peerForceAdmin, peerForceScopeEnvelope, peerForceScopes, peerForceControlState, peerForceConstruction, peerForceAdoptCommit, peerRecoveryReady} {
		result[fmt.Sprintf("%04x", op)] = operationSemantics{Emit: 2, Min: 2, Max: 2}
	}
	for op := peerCowCapture; op <= peerCowSync; op++ {
		result[fmt.Sprintf("%04x", op)] = operationSemantics{Emit: 1, Min: 1, Max: 1}
	}
	result[fmt.Sprintf("%04x", client.BucketClone)] = operationSemantics{Emit: 3, Min: 3, Max: 3}
	for _, op := range []uint16{client.PeerControlReserve, client.PeerControlPrepare, client.PeerControlCommit, client.PeerControlApply, client.PeerControlRelease, client.PeerControlAbort} {
		result[fmt.Sprintf("%04x", op)] = operationSemantics{Emit: 3, Min: 3, Max: 3}
	}
	result[fmt.Sprintf("%04x", client.BucketDelete)] = operationSemantics{Emit: 3, Min: 2, Max: 3}
	result[fmt.Sprintf("%04x", peerBucketLease)] = operationSemantics{Emit: 3, Min: 3, Max: 3}
	result[fmt.Sprintf("%04x", peerCowCapture)] = operationSemantics{Emit: 2, Min: 2, Max: 2}
	for _, op := range []uint16{peerForceReplicaState, clusterJoinStatus, clusterJoinInfo, clusterJoinPrepare} {
		result[fmt.Sprintf("%04x", op)] = operationSemantics{Emit: 1, Min: 1, Max: 1}
	}
	return result
}
