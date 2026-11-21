// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"reflect"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// A grow-only discovery directory is persisted as an ordinary system object.
// Its version is local; union is independent of arrival order. Excluding a node
// from membership is a separate control decision and cannot be undone by gossip.
type peerDirectoryImage struct {
	Schema   uint16               `json:"schema"`
	Cluster  client.ID            `json:"cluster"`
	Revision uint64               `json:"revision"`
	Nodes    []client.NodeAddress `json:"nodes"`
	Hints    []string             `json:"bootstrap_hints,omitempty"`
}

func mergePeerAddresses(base, additions []client.NodeAddress) ([]client.NodeAddress, error) {
	byID := make(map[client.ID]map[string]bool)
	byAddress := make(map[string]client.ID)
	for _, entries := range [][]client.NodeAddress{base, additions} {
		for _, v := range entries {
			if v.Node == (client.ID{}) {
				return nil, client.ErrProtocol
			}
			if byID[v.Node] == nil {
				byID[v.Node] = make(map[string]bool)
			}
			for _, address := range v.Addresses {
				if err := validateURL(address); err != nil {
					return nil, err
				}
				if prior, exists := byAddress[address]; exists && prior != v.Node {
					return nil, fail(409, "discovered address has conflicting node identities")
				}
				byAddress[address] = v.Node
				byID[v.Node][address] = true
			}
			if len(byID) > 10000 || len(byID[v.Node]) > 64 {
				return nil, fail(507, "peer directory exceeds contact budget")
			}
		}
	}
	entries := make([]client.NodeAddress, 0, len(byID))
	for id, addresses := range byID {
		v := client.NodeAddress{Node: id, Addresses: make([]string, 0, len(addresses))}
		for address := range addresses {
			v.Addresses = append(v.Addresses, address)
		}
		sort.Strings(v.Addresses)
		entries = append(entries, v)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Node.String() < entries[j].Node.String() })
	return entries, nil
}

func directoryContacts(image peerDirectoryImage, own string) []PeerConfig {
	contacts := []PeerConfig{}
	for _, v := range image.Nodes {
		if v.Node.String() == own {
			continue
		}
		for _, address := range v.Addresses {
			contacts = append(contacts, PeerConfig{NodeID: v.Node.String(), URL: address})
		}
	}
	// Unknown initial seeds are retained only while no discovered contact exists.
	if len(contacts) == 0 {
		for _, hint := range image.Hints {
			contacts = append(contacts, PeerConfig{URL: hint})
		}
	}
	return contacts
}

func (s *Server) initPeerDirectory() error {
	if s.cfg.PeerDirectoryVersion > 1 {
		return errors.New("unsupported system peer-directory schema")
	}
	var image peerDirectoryImage
	err := s.readBlob("peers", &image)
	if errors.Is(err, volume.ErrNotFound) && s.cfg.PeerDirectoryVersion == 0 {
		cluster, _ := parseID(s.cfg.ClusterID)
		image = peerDirectoryImage{Schema: 1, Cluster: client.ID(cluster), Revision: 1}
		for _, peer := range s.cfg.Peers {
			if peer.NodeID == "" {
				image.Hints = append(image.Hints, peer.URL)
			} else {
				id, _ := parseID(peer.NodeID)
				image.Nodes = append(image.Nodes, client.NodeAddress{Node: client.ID(id), Addresses: []string{peer.URL}})
			}
		}
		err = nil
	}
	if err != nil {
		return err
	}
	if image.Schema != 1 || image.Cluster.String() != s.cfg.ClusterID || image.Revision == 0 {
		return errors.New("invalid system peer directory; refusing config-only recovery")
	}
	for _, hint := range image.Hints {
		if err := validateURL(hint); err != nil {
			return err
		}
	}
	node, _ := parseID(s.cfg.Node.ID)
	own := client.NodeAddress{Node: client.ID(node)}
	if s.cfg.Node.Advertise != "" {
		own.Addresses = []string{s.cfg.Node.Advertise}
	}
	image.Nodes, err = mergePeerAddresses(image.Nodes, []client.NodeAddress{own})
	if err != nil {
		return err
	}
	if err = s.writeBlob("peers", image); err != nil {
		return err
	}
	s.cfg.Peers = directoryContacts(image, s.cfg.Node.ID)
	s.cfg.PeerDirectoryVersion = 1
	s.directory.Store(&image)
	return nil
}

func (s *Server) mergePeerDirectory(additions []client.NodeAddress) error {
	s.opMu.Lock()
	prior := s.directory.Load()
	if prior == nil {
		s.opMu.Unlock()
		return fail(503, "system peer directory is not loaded")
	}
	entries, err := mergePeerAddresses(prior.Nodes, additions)
	if err != nil || reflect.DeepEqual(entries, prior.Nodes) {
		s.opMu.Unlock()
		return err
	}
	image := *prior
	image.Nodes = entries
	image.Revision++
	if image.Revision == 0 {
		s.opMu.Unlock()
		return fail(507, "peer directory sequence exhausted")
	}
	s.mu.RLock()
	own, _ := parseID(s.cfg.Node.ID)
	s.mu.RUnlock()
	if _, err := (client.PeerDirectoryAnnouncement{Cluster: image.Cluster, Sender: client.ID(own), Nodes: entries}).MarshalBinary(); err != nil {
		s.opMu.Unlock()
		return fail(507, "peer directory exceeds the wire transfer budget")
	}
	if err = s.writeBlob("peers", image); err != nil {
		s.opMu.Unlock()
		return err
	}
	s.mu.Lock()
	c := copyConfig(s.cfg)
	c.Peers = directoryContacts(image, c.Node.ID)
	s.cfg = c
	s.mu.Unlock()
	s.directory.Store(&image)
	err = saveConfig(s.configPath, c) // derived local cache, not the authority
	s.opMu.Unlock()
	s.startKnownPeers()
	s.broadcastPeerDirectory()
	return err
}

func (s *Server) peerDirectoryBody() []byte {
	image := s.directory.Load()
	if image == nil {
		return nil
	}
	s.mu.RLock()
	id, _ := parseID(s.cfg.Node.ID)
	s.mu.RUnlock()
	body, _ := (client.PeerDirectoryAnnouncement{Cluster: image.Cluster, Sender: client.ID(id), Nodes: image.Nodes}).MarshalBinary()
	return body
}

func (s *Server) broadcastPeerDirectory() {
	body := s.peerDirectoryBody()
	if len(body) == 0 {
		return
	}
	s.peerStatsMu.RLock()
	var peers []*peerConnection
	for _, o := range s.peerStats {
		if o.online && o.connection != nil {
			peers = append(peers, o.connection)
		}
	}
	s.peerStatsMu.RUnlock()
	for _, p := range peers {
		p.sendDirectory(body)
	}
}

func (p *peerConnection) sendDirectory(body []byte) {
	select {
	case p.outgoing <- client.Message{Kind: client.FireAndForget, Opcode: client.PeerDirectoryMessage, ID: client.NewRequestID(), Body: body}:
	case <-p.ctx.Done():
	default:
		p.close() // reconnect resends the complete durable directory
	}
}
