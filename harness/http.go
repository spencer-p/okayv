package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
)

type NetTopology interface {
	Reachable(string, string) bool
}

type ClientPool struct {
	servers map[string]http.Handler
	topo    NetTopology
}

type Client struct {
	pool    *ClientPool
	forNode string
}

func NewClientPool(topo NetTopology) *ClientPool {
	return &ClientPool{
		servers: make(map[string]http.Handler),
		topo:    topo,
	}
}

func (p *ClientPool) ClientFor(node string, mux http.Handler) (*Client, error) {
	p.servers[node] = mux
	if len(p.servers) > 1 {
		var addrs []string
		for srv, _ := range p.servers {
			addrs = append(addrs, "http://"+srv)
		}
		// Choose a random server and send it the new view.
		for name, handler := range p.servers {
			if handler == mux {
				continue // But not the one we just added.
			}
			if err := viewChange(name, handler, addrs); err != nil {
				return nil, err
			}
			break
		}
	}
	return &Client{
		pool:    p,
		forNode: node,
	}, nil
}

func (c *Client) Do(r *http.Request) (*http.Response, error) {
	origin := c.forNode // This client is for the node requesting.
	target := r.Host    // The request has the server the client wants to reach.
	if !c.pool.topo.Reachable(origin, target) {
		return nil, context.DeadlineExceeded
	}
	targetServer, ok := c.pool.servers[target]
	if !ok {
		return nil, context.DeadlineExceeded
	}
	recorder := httptest.NewRecorder()
	targetServer.ServeHTTP(recorder, r)
	return recorder.Result(), nil
}

type DoFunc func(*http.Request) (*http.Response, error)

func (f DoFunc) Do(r *http.Request) (*http.Response, error) {
	return f(r)
}

func (p *ClientPool) AlwaysReachable() DoFunc {
	return func(r *http.Request) (*http.Response, error) {
		target := r.Host
		targetServer, ok := p.servers[target]
		if !ok {
			return nil, fmt.Errorf("could not find host %q: %w", target, context.DeadlineExceeded)
		}
		recorder := httptest.NewRecorder()
		targetServer.ServeHTTP(recorder, r)
		return recorder.Result(), nil
	}
}

func viewChange(name string, handler http.Handler, addrs []string) error {
	var body bytes.Buffer
	req := map[string]any{
		"replicas": addrs,
	}
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return err
	}

	httpreq, err := http.NewRequest(http.MethodPut, name+"/view-change", &body)
	if err != nil {
		return err
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httpreq)
	return nil
}
