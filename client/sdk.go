// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"weak"

	"github.com/gorilla/websocket"
)

// Client is an authenticated connection to exactly one bucket. Object methods
// cannot switch buckets, resolve bucket names, administer the cluster or fetch
// cluster statistics. IDs are native 256-bit object addresses, never rehashed.
// It is safe to call methods concurrently; Close is idempotent.
type Client struct {
	durabilityError error

	durability map[ID]pendingDurability

	subMu                        sync.Mutex
	subscriptions                map[ID]weak.Pointer[Subscription]
	subscriptionSources          map[ID]*objectTransport
	endpoint, username, password string
	bucket                       BucketID
	runtime                      ID
	mu                           sync.Mutex
	transport                    *objectTransport
	maintenanceToken             ID
	distributedMaintenance       bool
	maintenance                  *objectTransport // pinned session; never reconnect during fsck
	dialGate                     chan struct{}
	closed                       bool
	closeOnce                    sync.Once
	life                         context.Context
	cancel                       context.CancelFunc
	done                         chan struct{}
	routes                       PartialMap
	cluster                      ID
	routed                       map[string]*objectTransport
	routeGates                   map[string]chan struct{}
	clusterRouting               bool
	routers                      map[string]ID
	seedRoute                    string // first advertised address represented by the caller's seed URL
	departed                     map[ID]uint64
	paused                       map[ID]bool
}

// Dial establishes the bucket connection. All four connection parameters are
// mandatory. ctx controls establishment; individual operations take their own
// contexts. There is no TLS or implicit HTTP redirect/credential forwarding.
func Dial(ctx context.Context, rawURL, username, password string, bucket BucketID) (*Client, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	u, e := url.Parse(rawURL)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Scheme != "http" && u.Scheme != "ws" || u.Path != "" && u.Path != "/" && u.Path != "/api/ws" {
		return nil, errors.New("client: URL must be http://host:port or ws://host:port/api/ws without embedded credentials")
	}
	if username == "" || strings.Contains(username, ":") || password == "" || bucket == (BucketID{}) {
		return nil, errors.New("client: URL, username, password and a nonzero bucket ID are required")
	}
	u.Scheme = "ws"
	u.Path = "/api/ws"
	life, cancel := context.WithCancel(context.Background())
	c := &Client{endpoint: u.String(), username: username, password: password, bucket: bucket, done: make(chan struct{}), dialGate: make(chan struct{}, 1), life: life, cancel: cancel}
	t, e := c.connect(ctx)
	if e != nil {
		cancel()
		return nil, e
	}
	c.transport = t
	return c, nil
}
func (c *Client) connect(ctx context.Context) (*objectTransport, error) {
	return c.connectEndpoint(ctx, c.endpoint)
}
func (c *Client) connectEndpoint(ctx context.Context, endpoint string) (*objectTransport, error) {
	merged, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.life, cancel)
	defer stop()
	defer cancel()
	ctx = merged
	headers := http.Header{}
	headers.Set(BucketHeader, c.bucket.String())
	c.mu.Lock()
	token := c.maintenanceToken
	distributed := c.distributedMaintenance
	c.mu.Unlock()
	if distributed {
		headers.Set("X-STQR-Maintenance", token.String())
	}
	headers.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(c.username+":"+c.password)))
	dialer := websocket.Dialer{Subprotocols: []string{"stqr.v2"}, EnableCompression: false, HandshakeTimeout: 0}
	var stopSocket func() bool
	defer func() {
		if stopSocket != nil {
			stopSocket()
		}
	}()
	// Cancel an HTTP upgrade waiting for response headers as well as TCP dial.
	// Stop this hook before ownership passes to the long-lived transport.
	dialer.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e == nil {
			stopSocket = context.AfterFunc(ctx, func() { conn.Close() })
		}
		return conn, e
	}
	conn, response, e := dialer.DialContext(ctx, endpoint, headers)
	if e != nil {
		if response != nil {
			response.Body.Close()
			return nil, &StatusError{Status: response.StatusCode, Message: "connection rejected"}
		}
		return nil, e
	}
	if conn.Subprotocol() != "stqr.v2" {
		conn.Close()
		return nil, ErrProtocol
	}
	t := newObjectTransport(conn, c, func(m Message) error { return c.learnGoAway(endpoint, m) })
	hello := Encoder{}
	hello.U16(2)
	hello.U16(2)
	hello.U64(CapabilityDurability)
	hello.Text("stqr-go")
	r, e := t.roundTrip(ctx, Message{Kind: Request, Opcode: Hello, ID: NewRequestID(), Body: hello.B}, false, nil)
	if e == nil {
		d := Decoder{B: r.result}
		version := d.U16()
		capabilities := d.U64()
		t.capabilities = capabilities
		t.endpoint = endpoint
		t.node = d.ID()
		cluster := d.ID()
		membership := d.U64()
		d.U64()
		if d.Done() != nil || version != 2 {
			e = ErrProtocol
		}
		c.mu.Lock()
		if capabilities&CapabilityClusterControl != 0 {
			c.clusterRouting = true
		}
		if c.cluster != (ID{}) && c.cluster != cluster {
			e = ErrProtocol
		} else {
			c.cluster = cluster
		}
		if e == nil {
			if c.routers == nil {
				c.routers = make(map[string]ID)
			}
			c.routers[endpoint] = t.node
			delete(c.paused, t.node)
			if membership > c.departed[t.node] {
				delete(c.departed, t.node)
			}
		}
		c.mu.Unlock()
	}
	r.close()
	if e == nil {
		e = ctx.Err()
	}
	if e != nil {
		t.fail(e)
		t.workers.Wait()
		return nil, e
	}
	return t, nil
}
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		owner := c.maintenance
		distributed := c.distributedMaintenance
		c.closed = true
		c.mu.Unlock()
		if owner != nil && distributed && transportAlive(owner) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			e := Encoder{}
			c.objectRef(&e, ObjectID{})
			r, _ := owner.roundTrip(ctx, Message{Kind: Request, Opcode: BucketMaintenanceEnd, ID: NewRequestID(), Body: e.B}, false, nil)
			r.close()
			cancel()
		}
		if c.cancel != nil {
			c.cancel()
		}
		if c.done != nil {
			close(c.done)
		}
		c.mu.Lock()
		c.closed = true
		t := c.transport
		var routed []*objectTransport
		for _, transport := range c.routed {
			routed = append(routed, transport)
		}
		c.mu.Unlock()
		if t != nil {
			t.fail(ErrClosed)
			t.workers.Wait()
		}
		for _, transport := range routed {
			transport.fail(ErrClosed)
			transport.workers.Wait()
		}
	})
	return nil
}
func (c *Client) activeTransport() (*objectTransport, error) {
	if c == nil {
		return nil, ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.done == nil {
		return nil, ErrClosed
	}
	select {
	case <-c.done:
		return nil, ErrClosed
	default:
	}
	if c.transport != nil {
		select {
		case <-c.transport.done:
		default:
			return c.transport, nil
		}
	}
	return nil, nil
}
func (c *Client) connection(ctx context.Context) (*objectTransport, error) {
	if t, e := c.activeTransport(); t != nil || e != nil {
		return t, e
	}
	c.mu.Lock()
	pinned := c.maintenance
	c.mu.Unlock()
	if pinned != nil {
		return nil, ErrClosed
	}
	// A caller must be able to cancel while another caller is reconnecting. Never
	// hold the state mutex across dial/HELLO; the gate is context-aware.
	select {
	case c.dialGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrClosed
	}
	defer func() { <-c.dialGate }()
	if t, e := c.activeTransport(); t != nil || e != nil {
		return t, e
	}
	t, e := c.connect(ctx)
	if e != nil {
		return nil, e
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		t.fail(ErrClosed)
		t.workers.Wait()
		return nil, ErrClosed
	}
	select {
	case <-c.done:
		c.mu.Unlock()
		t.fail(ErrClosed)
		t.workers.Wait()
		return nil, ErrClosed
	default:
	}
	c.transport = t
	c.mu.Unlock()
	return t, nil
}
func (c *Client) ready(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if c == nil {
		return ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.done == nil {
		return ErrClosed
	}
	select {
	case <-c.done:
		return ErrClosed
	default:
	}
	return nil
}
func (c *Client) do(ctx context.Context, opcode uint16, body []byte, id RequestID, mutation bool, upload uploadFunc) (wireReply, error) {
	if e := c.ready(ctx); e != nil {
		return wireReply{}, e
	}
	if id == (RequestID{}) {
		id = NewRequestID()
	}
	m := Message{Kind: Request, Opcode: opcode, ID: id, Body: body}
	c.mu.Lock()
	maintenance := c.maintenance
	distributed := c.distributedMaintenance
	c.mu.Unlock()
	if maintenance != nil && distributed && !transportAlive(maintenance) {
		return wireReply{}, ErrClosed
	}
	if maintenance != nil && !distributed {
		// A reconnect loses exclusive ownership. Never resume a sweep against a
		// potentially changed graph; the operator must start a fresh fsck.
		outgoing := m
		if mutation && upload == nil && ctx.Value(acceptedContextKey{}) == true && maintenance.capabilities&CapabilityDurability != 0 {
			outgoing = acceptedMessage(m)
		}
		r, err := maintenance.roundTrip(ctx, outgoing, mutation, upload)
		if err == nil {
			if mark := r.headers[DurabilityHeader]; len(mark) > 0 {
				if err = c.observeDurability(maintenance, mark); err != nil {
					r.close()
					return wireReply{}, err
				}
			}
		}
		return r, err
	}
	backoff := 100 * time.Millisecond
	for {
		if maintenance != nil && distributed && !transportAlive(maintenance) {
			return wireReply{}, ErrClosed
		}
		var t *objectTransport
		var e error
		var routeNode ID
		if objectMessage(m) {
			e = c.ensureRoute(ctx, m)
		}
		if e == nil {
			endpoint, node, routeErr := c.routeMessage(&m)
			routeNode = node
			if routeErr != nil {
				return wireReply{}, routeErr
			}
			t, e = c.connectionFor(ctx, endpoint, node)
		}
		if e == nil {
			if m.Opcode == ObjectSubscribe && len(m.Body) >= 96 {
				var subscription ID
				copy(subscription[:], m.Body[80:96])
				c.bindSubscriptionTransport(subscription, t)
			}
			outgoing := m
			if mutation && upload == nil && ctx.Value(acceptedContextKey{}) == true && t.capabilities&CapabilityDurability != 0 {
				if err := c.syncExcept(ctx, t); err != nil {
					return wireReply{}, err
				}
				outgoing = acceptedMessage(m)
			}
			r, er := t.roundTrip(ctx, outgoing, mutation, upload)
			if er == nil {
				if b := r.headers[DurabilityHeader]; len(b) > 0 {
					if err := c.observeDurability(t, b); err != nil {
						r.close()
						return wireReply{}, err
					}
				}
				return r, nil
			}
			e = er
			var status *StatusError
			if errors.As(e, &status) && (status.Status == 301 || status.Status == 302) {
				if err := c.learnRedirect(m, status); err != nil {
					return wireReply{}, err
				}
				continue
			}
			if errors.As(e, &status) && status.Status != 503 || errors.Is(e, ErrWouldBlock) || errors.Is(e, ErrProtocol) {
				return wireReply{}, e
			}
		}
		if ctx.Err() != nil {
			if mutation {
				return wireReply{}, &UnknownOutcomeError{RequestID: id, Cause: ctx.Err()}
			}
			return wireReply{}, ctx.Err()
		}
		if errors.Is(e, ErrClosed) {
			select {
			case <-c.done:
				return wireReply{}, ErrClosed
			default:
			}
		}
		var local *uploadError
		if errors.As(e, &local) {
			if mutation {
				return wireReply{}, &UnknownOutcomeError{RequestID: id, Cause: local.Cause}
			}
			return wireReply{}, local.Cause
		}
		var status *StatusError
		if errors.As(e, &status) && status.Status != 503 {
			return wireReply{}, e
		}
		var network net.Error
		var closed *websocket.CloseError
		if !(status != nil && status.Status == 503) && !errors.As(e, &network) && !errors.As(e, &closed) && !errors.Is(e, io.EOF) && !errors.Is(e, ErrClosed) && !errors.Is(e, websocket.ErrCloseSent) {
			return wireReply{}, e
		}
		if objectMessage(m) {
			c.mu.Lock()
			if c.paused == nil {
				c.paused = make(map[ID]bool)
			}
			c.paused[routeNode] = true
			var object ObjectID
			copy(object[:], m.Body[16:48])
			c.routes.ForgetObject(c.bucket, object)
			c.mu.Unlock()
		}
		// A broken transport is not a failed node or a failed mutation. Reconnect
		// and resend identical logical input under the same persistent request ID.
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			if mutation {
				return wireReply{}, &UnknownOutcomeError{RequestID: id, Cause: ctx.Err()}
			}
			return wireReply{}, ctx.Err()
		case <-c.done:
			timer.Stop()
			return wireReply{}, ErrClosed
		case <-timer.C:
		}
		backoff = min(3*time.Second, backoff*2)
	}
}

// StatusError retains the original protocol status and optional follow headers.
// Redirects carry validated bucket-specific map segments; authenticated
// destinations are selected independently for each learned range.
type StatusError struct {
	Status    int
	Message   string
	RequestID RequestID
	Headers   map[uint64][]byte
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("client: status %d: %s", e.Status, e.Message)
}
func (e *StatusError) Is(target error) bool {
	switch e.Status {
	case 404:
		return target == ErrNotFound
	case 410:
		return target == ErrGone
	case 412:
		return target == ErrPrecondition
	case 507:
		return target == ErrFull
	case 501:
		return target == ErrNotImplemented
	case 401, 403:
		return target == ErrUnauthorized
	case 301, 302:
		return target == ErrRedirect
	}
	return false
}

// UnknownOutcomeError means cancellation/disconnection cannot prove a mutation
// absent. Retry with RequestID unchanged; never manufacture a new logical write.
type UnknownOutcomeError struct {
	RequestID RequestID
	Cause     error
}

func (e *UnknownOutcomeError) Error() string {
	return fmt.Sprintf("client: outcome unknown for %s: %v", e.RequestID, e.Cause)
}
func (e *UnknownOutcomeError) Unwrap() error { return e.Cause }

// acceptObjectInfo learns the single-node bucket runtime after its first reply.
// A learned RAM identity must survive reconnects so a pre-reset retry cannot be
// replayed as a new mutation. Multinode will bind this to learned range entries.
func (c *Client) acceptObjectInfo(info ObjectInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clusterRouting {
		return nil
	} // each request is bound to its range runtime
	if c.runtime != (ID{}) && c.runtime != info.RuntimeID {
		return ErrGone
	}
	if info.RuntimeID != (ID{}) {
		c.runtime = info.RuntimeID
	}
	return nil
}

func transportAlive(t *objectTransport) bool {
	select {
	case <-t.done:
		return false
	default:
		return true
	}
}
