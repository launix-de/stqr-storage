// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

type Config struct {
	URL      string `json:"uri"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func LoadConfig(path string) (Config, error) {
	var c Config
	f, e := os.Open(path)
	if e != nil {
		return c, e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil {
		return c, e
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > 65536 {
		return c, fmt.Errorf("credentials file must be private (0600), regular and <=64 KiB")
	}
	d := json.NewDecoder(io.LimitReader(f, 65537))
	d.DisallowUnknownFields()
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	if e = d.Decode(new(any)); e != io.EOF {
		return c, fmt.Errorf("credentials file must contain exactly one JSON object")
	}
	if c.URL == "" || c.Username == "" || c.Password == "" {
		return c, fmt.Errorf("uri, username and password required")
	}
	return c, nil
}
func ResolveBucket(ctx context.Context, c Config, name string) (client.ID, error) {
	e := client.Encoder{}
	e.Text(name)
	m := client.Message{Kind: client.Request, Opcode: client.BucketInfoGet, ID: client.NewRequestID(), Body: e.B}
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(c.URL, "/")+"/api/message", bytes.NewReader(client.EncodeMessage(m)))
	if err != nil {
		return client.ID{}, err
	}
	req.SetBasicAuth(c.Username, c.Password)
	req.Header.Set("Content-Type", "application/octet-stream")
	h := http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	r, err := h.Do(req)
	if err != nil {
		return client.ID{}, err
	}
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, client.MaxMessage+1))
	if err != nil {
		return client.ID{}, err
	}
	reply, err := client.DecodeMessage(raw)
	if err != nil || reply.ID != m.ID || reply.Opcode != m.Opcode || reply.Kind != client.Response {
		return client.ID{}, client.ErrProtocol
	}
	d := client.Decoder{B: reply.Body}
	status := d.V()
	d.Blob()
	if d.Err != nil {
		return client.ID{}, client.ErrProtocol
	}
	if status != 200 {
		return client.ID{}, fmt.Errorf("bucket resolution status %d", status)
	}
	id := d.ID()
	if d.Err != nil || id == (client.ID{}) {
		return id, client.ErrProtocol
	}
	return id, nil
}
