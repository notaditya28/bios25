// Package client is a Go client for the broker's HTTP API.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
)

type Client struct {
	base string
	hc   *http.Client
}

// New returns a client for the broker at addr ("host:port" or a full http URL).
func New(addr string) *Client {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	return &Client{
		base: strings.TrimRight(addr, "/"),
		hc:   &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second}},
	}
}

func (c *Client) Info(ctx context.Context) (api.Info, error) {
	var out api.Info
	return out, c.do(ctx, http.MethodGet, "/v1/broker", nil, &out)
}

func (c *Client) CreateTopic(ctx context.Context, cfg api.TopicConfig) (api.TopicInfo, error) {
	var out api.TopicInfo
	return out, c.do(ctx, http.MethodPost, "/v1/topics", cfg, &out)
}

func (c *Client) Topics(ctx context.Context) ([]api.TopicInfo, error) {
	var out struct {
		Topics []api.TopicInfo `json:"topics"`
	}
	return out.Topics, c.do(ctx, http.MethodGet, "/v1/topics", nil, &out)
}

func (c *Client) Topic(ctx context.Context, name string) (api.TopicInfo, error) {
	var out api.TopicInfo
	return out, c.do(ctx, http.MethodGet, "/v1/topics/"+url.PathEscape(name), nil, &out)
}

func (c *Client) Produce(ctx context.Context, topic string, recs []api.ProduceRecord) (api.ProduceResponse, error) {
	var out api.ProduceResponse
	return out, c.do(ctx, http.MethodPost, "/v1/topics/"+url.PathEscape(topic)+"/records", api.ProduceRequest{Records: recs}, &out)
}

// ProduceRequest sends a full produce request (e.g. an idempotent batch).
func (c *Client) ProduceRequest(ctx context.Context, topic string, req api.ProduceRequest) (api.ProduceResponse, error) {
	var out api.ProduceResponse
	return out, c.do(ctx, http.MethodPost, "/v1/topics/"+url.PathEscape(topic)+"/records", req, &out)
}

func (c *Client) Addr() string { return strings.TrimPrefix(c.base, "http://") }

func (c *Client) Fetch(ctx context.Context, req api.FetchRequest) (api.FetchResponse, error) {
	q := url.Values{}
	q.Set("offset", strconv.FormatUint(req.Offset, 10))
	if req.MaxRecords > 0 {
		q.Set("max_records", strconv.Itoa(req.MaxRecords))
	}
	if req.MaxBytes > 0 {
		q.Set("max_bytes", strconv.Itoa(req.MaxBytes))
	}
	if req.Wait > 0 {
		q.Set("wait_ms", strconv.FormatInt(req.Wait.Milliseconds(), 10))
	}
	path := fmt.Sprintf("/v1/topics/%s/partitions/%d/records?%s", url.PathEscape(req.Topic), req.Partition, q.Encode())
	var out api.FetchResponse
	return out, c.do(ctx, http.MethodGet, path, nil, &out)
}

// do sends a request; non-2xx responses are returned as *api.Error.
func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var eb struct {
			Error *api.Error `json:"error"`
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if json.Unmarshal(data, &eb) == nil && eb.Error != nil {
			return eb.Error
		}
		return &api.Error{Code: api.CodeInternal, Message: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(data))}
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Get fetches any JSON endpoint on the broker (e.g. the /admin/v1 views).
func (c *Client) Get(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, out)
}
