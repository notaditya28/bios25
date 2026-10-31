// Package rpc is the broker-to-broker transport: JSON over HTTP, plus a link blocker used to
// simulate network partitions in tests and chaos runs.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/transport/httpapi"
)

// FromHeader carries the sending broker's id on internal requests.
const FromHeader = "X-Hydra-From"

var (
	ErrBlocked     = errors.New("rpc: link blocked")
	ErrUnknownPeer = errors.New("rpc: unknown peer")
)

// Net is a set of peers this broker can't talk to (both directions). Empty = healthy network.
type Net struct {
	mu      sync.RWMutex
	blocked map[string]bool
}

func NewNet() *Net { return &Net{blocked: map[string]bool{}} }

func (n *Net) Block(ids ...string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, id := range ids {
		n.blocked[id] = true
	}
}

func (n *Net) Unblock(ids ...string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, id := range ids {
		delete(n.blocked, id)
	}
}

// Set replaces the blocked set.
func (n *Net) Set(ids []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = map[string]bool{}
	for _, id := range ids {
		n.blocked[id] = true
	}
}

func (n *Net) Blocked(id string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.blocked[id]
}

func (n *Net) List() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]string, 0, len(n.blocked))
	for id := range n.blocked {
		out = append(out, id)
	}
	return out
}

// Middleware drops internal requests from blocked peers, as a partitioned network would.
func (n *Net) Middleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if from := r.Header.Get(FromHeader); from != "" && n.Blocked(from) {
			http.Error(w, "link blocked", http.StatusServiceUnavailable)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// Client calls other brokers by id.
type Client struct {
	self  string
	addrs map[string]string
	net   *Net
	hc    *http.Client
}

func NewClient(self string, addrs map[string]string, net *Net) *Client {
	return &Client{
		self:  self,
		addrs: addrs,
		net:   net,
		hc: &http.Client{Transport: &http.Transport{
			MaxIdleConnsPerHost: 128,
			IdleConnTimeout:     90 * time.Second,
		}},
	}
}

func (c *Client) Self() string { return c.self }

func (c *Client) Addr(id string) string { return c.addrs[id] }

// Call POSTs in to path on peer and decodes the response into out (if non-nil).
// Non-2xx responses carrying a broker error are returned as *api.Error.
func (c *Client) Call(ctx context.Context, peer, path string, in, out any) error {
	if c.net.Blocked(peer) {
		return fmt.Errorf("%w: %s -> %s", ErrBlocked, c.self, peer)
	}
	addr, ok := c.addrs[peer]
	if !ok {
		return fmt.Errorf("%w %q", ErrUnknownPeer, peer)
	}
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(FromHeader, c.self)
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var eb struct {
			Error *api.Error `json:"error"`
		}
		if json.Unmarshal(body, &eb) == nil && eb.Error != nil {
			return eb.Error
		}
		return fmt.Errorf("rpc %s %s: HTTP %d: %s", peer, path, resp.StatusCode, bytes.TrimSpace(body))
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type fromKey struct{}

// From returns the id of the broker that sent the internal request handled under ctx ("" if unknown).
func From(ctx context.Context) string {
	s, _ := ctx.Value(fromKey{}).(string)
	return s
}

// Handle registers a JSON handler for POST path.
func Handle[Req, Resp any](mux *http.ServeMux, path string, fn func(context.Context, *Req) (*Resp, error)) {
	mux.HandleFunc("POST "+path, func(w http.ResponseWriter, r *http.Request) {
		var req Req
		if !httpapi.Decode(w, r, &req) {
			return
		}
		ctx := context.WithValue(r.Context(), fromKey{}, r.Header.Get(FromHeader))
		resp, err := fn(ctx, &req)
		if err != nil {
			httpapi.WriteError(w, err)
			return
		}
		httpapi.WriteJSON(w, http.StatusOK, resp)
	})
}
