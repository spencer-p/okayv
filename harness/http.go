package harness

import (
	"context"
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

func (p *ClientPool) ClientFor(node string, mux http.Handler) *Client {
	p.servers[node] = mux
	return &Client{
		pool:    p,
		forNode: node,
	}
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
