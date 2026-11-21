// SPDX-License-Identifier: GPL-3.0-only
package client

// Peer discovery is a set of contact addresses, not membership admission or
// placement authority. The authenticated sender broadcasts it after connecting.
const PeerDirectoryMessage uint16 = 0x1038

type PeerDirectoryAnnouncement struct {
	Cluster, Sender ID
	Nodes           []NodeAddress
}

func (m PeerDirectoryAnnouncement) MarshalBinary() ([]byte, error) {
	if m.Cluster == (ID{}) || m.Sender == (ID{}) || len(m.Nodes) > 10000 {
		return nil, ErrProtocol
	}
	e := Encoder{}
	e.ID(m.Cluster)
	e.ID(m.Sender)
	e.V(uint64(len(m.Nodes)))
	seen := make(map[ID]bool)
	for _, node := range m.Nodes {
		if node.Node == (ID{}) || seen[node.Node] || len(node.Addresses) > 64 {
			return nil, ErrProtocol
		}
		seen[node.Node] = true
		e.ID(node.Node)
		if err := writeAddresses(&e, node.Addresses); err != nil {
			return nil, err
		}
		if len(e.B) > MaxMessage-20 {
			return nil, ErrProtocol
		}
	}
	return e.B, nil
}

func DecodePeerDirectory(body []byte) (PeerDirectoryAnnouncement, error) {
	if len(body) > MaxMessage-20 {
		return PeerDirectoryAnnouncement{}, ErrProtocol
	}
	d := Decoder{B: body}
	m := PeerDirectoryAnnouncement{Cluster: d.ID(), Sender: d.ID()}
	n := d.Count(10000)
	if n > len(d.B)/17 {
		return m, ErrProtocol
	}
	m.Nodes = make([]NodeAddress, n)
	seen := make(map[ID]bool)
	for i := range m.Nodes {
		v := &m.Nodes[i]
		v.Node, v.Addresses = d.ID(), readAddresses(&d)
		if v.Node == (ID{}) || seen[v.Node] {
			return m, ErrProtocol
		}
		seen[v.Node] = true
	}
	if d.Done() != nil || m.Cluster == (ID{}) || m.Sender == (ID{}) {
		return m, ErrProtocol
	}
	return m, nil
}
