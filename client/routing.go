// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"encoding/binary"
	"net/url"
	"sort"
)

func objectMessage(m Message) bool {
	return (m.Opcode >= 0x0100 && m.Opcode <= 0x0110 || m.Opcode == BucketObjectList) && len(m.Body) >= 80
}

func (c *Client) routeMessage(m *Message) (string, ID, error) {
	if !objectMessage(*m) {
		return c.endpoint, ID{}, nil
	}
	var bucket ID
	var object ObjectID
	copy(bucket[:], m.Body[:16])
	copy(object[:], m.Body[16:48])
	if bucket != c.bucket {
		return c.endpoint, ID{}, nil
	}
	entry, revision, _, _, known := c.routes.Lookup(bucket, object)
	if !known {
		return c.endpoint, ID{}, nil
	}
	var bound ID
	copy(bound[:], m.Body[64:80])
	if bound != (ID{}) && bound != entry.Runtime {
		return "", ID{}, ErrGone
	}
	m.Body = append([]byte{}, m.Body...)
	binary.LittleEndian.PutUint64(m.Body[48:56], revision)
	binary.LittleEndian.PutUint64(m.Body[56:64], entry.OwnerEpoch)
	copy(m.Body[64:80], entry.Runtime[:])
	// Endpoints are obtained from the learned segment belonging to this exact
	// object. Other objects retain independent routes and transport queues.
	_, _, _, addresses, _ := c.routes.Lookup(bucket, object)
	var owner ID
	for _, loc := range entry.Locations {
		if loc.Role == 0 {
			owner = loc.Node
			break
		}
	}
	if len(addresses) == 0 {
		return c.endpoint, ID{}, nil
	}
	// A failed primary transport may change a read source, never the write
	// authority. Select only nodes in this object's committed replica set.
	read := m.Opcode == 0x0100 || m.Opcode == 0x0101 || m.Opcode == 0x0107
	c.mu.Lock()
	if read && c.paused[owner] {
		for _, loc := range entry.Locations {
			if loc.Role != 1 || c.paused[loc.Node] {
				continue
			}
			found := false
			for _, address := range addresses {
				u, e := url.Parse(address)
				if e != nil {
					continue
				}
				u.Scheme = "ws"
				u.Path = "/api/ws"
				if c.routers[u.String()] == loc.Node {
					owner = loc.Node
					addresses = []string{address}
					found = true
					break
				}
			}
			if found {
				break
			}
		}
	}
	c.mu.Unlock()
	u, err := url.Parse(addresses[0])
	if err != nil {
		return c.endpoint, ID{}, nil
	}
	u.Scheme = "ws"
	u.Path = "/api/ws"
	c.mu.Lock()
	seed, alias := c.routers[c.endpoint], c.seedRoute
	c.mu.Unlock()
	if owner == seed && owner != (ID{}) && u.String() == alias {
		return c.endpoint, owner, nil
	}
	return u.String(), owner, nil
}

func (c *Client) learnRedirect(m Message, status *StatusError) error {
	if !objectMessage(m) {
		return status
	}
	patch, err := DecodeMapPatch(status.Headers[2])
	if err != nil || patch.Bucket != c.bucket {
		return ErrProtocol
	}
	var object ObjectID
	copy(object[:], m.Body[16:48])
	if compareObject(object, patch.Begin) < 0 || !beforeEnd(object, patch.End, patch.ToEnd) {
		return ErrProtocol
	}
	_, err = c.routes.Apply(patch)
	if err == nil {
		c.rememberRouters(patch)
	}
	return err
}

func (c *Client) rememberRouters(patch MapPatch) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.routers == nil {
		c.routers = make(map[string]ID)
	}
	for _, node := range patch.Addresses {
		if patch.Membership > c.departed[node.Node] {
			delete(c.departed, node.Node)
		}
		for _, address := range node.Addresses {
			u, err := url.Parse(address)
			if err != nil {
				continue
			}
			u.Scheme = "ws"
			u.Path = "/api/ws"
			if node.Node == c.routers[c.endpoint] && c.seedRoute == "" {
				c.seedRoute = u.String()
			}
			c.routers[u.String()] = node.Node
		}
	}
	for node, membership := range c.departed {
		c.routes.ForgetNode(node, membership)
	}
}

// A terminal push is processed before pending requests are released, so retries
// cannot dial a departed owner using an obsolete cached route.
func (c *Client) learnGoAway(endpoint string, m Message) error {
	d := Decoder{B: m.Body}
	reason := d.U8()
	operation := d.ID()
	count := d.Count(10000)
	alternatives := make([]NodeAddress, count)
	seen := make(map[ID]bool, count)
	for i := range alternatives {
		alternatives[i].Node = d.ID()
		alternatives[i].Addresses = readAddresses(&d)
		if alternatives[i].Node == (ID{}) || seen[alternatives[i].Node] || len(alternatives[i].Addresses) == 0 {
			d.Err = ErrProtocol
		}
		seen[alternatives[i].Node] = true
	}
	membership, revision := d.U64(), d.U64()
	if d.Done() != nil || reason > 3 || operation == (ID{}) || membership == 0 || revision == 0 {
		return ErrProtocol
	}
	c.mu.Lock()
	node := c.routers[endpoint]
	if c.departed == nil {
		c.departed = make(map[ID]uint64)
	}
	if c.paused == nil {
		c.paused = make(map[ID]bool)
	}
	if reason == 0 || reason == 3 {
		if membership > c.departed[node] {
			c.departed[node] = membership
		}
		// Hold the client mutex through invalidation: the next retry must see
		// both the removed segments and the alternative router addresses.
		c.routes.ForgetNode(node, membership)
	} else {
		c.paused[node] = true
	}
	if c.routers == nil {
		c.routers = make(map[string]ID)
	}
	for _, alternative := range alternatives {
		for _, address := range alternative.Addresses {
			u, _ := url.Parse(address) // readAddresses validated the address.
			u.Scheme, u.Path = "ws", "/api/ws"
			c.routers[u.String()] = alternative.Node
		}
	}
	c.mu.Unlock()
	return nil
}

// Bind a range/runtime before the first object request, including a first RAM
// mutation whose reply could be lost across process death. Learned ranges avoid
// subsequent round trips; another known member can answer when the seed pauses.
func (c *Client) ensureRoute(ctx context.Context, m Message) error {
	c.mu.Lock()
	enabled := c.clusterRouting
	nodes := make(map[string]ID, len(c.routers))
	priorities := make(map[string]int, len(c.routers))
	for endpoint, node := range c.routers {
		if c.departed[node] != 0 {
			continue
		}
		nodes[endpoint] = node
		if c.paused[node] {
			priorities[endpoint] = 1
		}
	}
	c.mu.Unlock()
	if !enabled {
		return nil
	}
	var object ObjectID
	copy(object[:], m.Body[16:48])
	if _, _, _, _, known := c.routes.Lookup(c.bucket, object); known {
		return nil
	}
	endpoints := make([]string, 0, len(nodes))
	for endpoint := range nodes {
		endpoints = append(endpoints, endpoint)
	}
	sort.Slice(endpoints, func(i, j int) bool {
		if priorities[endpoints[i]] != priorities[endpoints[j]] {
			return priorities[endpoints[i]] < priorities[endpoints[j]]
		}
		return endpoints[i] < endpoints[j]
	})
	var last error
	for _, endpoint := range endpoints {
		t, err := c.connectionFor(ctx, endpoint, nodes[endpoint])
		if err != nil {
			last = err
			continue
		}
		e := Encoder{}
		e.ID(c.bucket)
		e.B = append(e.B, object[:]...)
		reply, err := t.roundTrip(ctx, Message{Kind: Request, Opcode: MapGetMessage, ID: NewRequestID(), Body: e.B}, false, nil)
		if err != nil {
			last = err
			continue
		}
		patch, err := DecodeMapPatch(reply.result)
		reply.close()
		if err != nil || patch.Bucket != c.bucket || compareObject(object, patch.Begin) < 0 || !beforeEnd(object, patch.End, patch.ToEnd) {
			return ErrProtocol
		}
		if _, err := c.routes.Apply(patch); err != nil {
			return err
		}
		c.rememberRouters(patch)
		return nil
	}
	if last == nil {
		last = ErrProtocol
	}
	return last
}

func (c *Client) connectionFor(ctx context.Context, endpoint string, node ID) (*objectTransport, error) {
	if endpoint == c.endpoint {
		t, err := c.connection(ctx)
		if err == nil && node != (ID{}) && t.node != node {
			return nil, ErrProtocol
		}
		return t, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if c.routeGates == nil {
		c.routeGates = make(map[string]chan struct{})
		c.routed = make(map[string]*objectTransport)
	}
	gate := c.routeGates[endpoint]
	if gate == nil {
		gate = make(chan struct{}, 1)
		c.routeGates[endpoint] = gate
	}
	c.mu.Unlock()
	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrClosed
	}
	defer func() { <-gate }()
	c.mu.Lock()
	t := c.routed[endpoint]
	c.mu.Unlock()
	if t != nil {
		select {
		case <-t.done:
			t = nil
		default:
		}
	}
	if t == nil {
		var err error
		t, err = c.connectEndpoint(ctx, endpoint)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			t.fail(ErrClosed)
			t.workers.Wait()
			return nil, ErrClosed
		}
		c.routed[endpoint] = t
		c.mu.Unlock()
	}
	if node != (ID{}) && t.node != node {
		return nil, ErrProtocol
	}
	return t, nil
}
