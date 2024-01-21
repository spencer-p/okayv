package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
)

type nothing struct{}

type NetTopology interface {
	Reachable(string, string) bool
}

type ClientPool struct {
	recorder *Recorder
	servers  map[string]http.Handler
	topo     NetTopology
}

type Client struct {
	pool    *ClientPool
	forNode string
}

func NewClientPool(topo NetTopology, recorder *Recorder) *ClientPool {
	return &ClientPool{
		recorder: recorder,
		servers:  make(map[string]http.Handler),
		topo:     topo,
	}
}

func (p *ClientPool) ClientFor(node string, mux http.Handler) (*Client, error) {
	p.servers[node] = p.recorder.MakeHandler(mux)
	return &Client{
		pool:    p,
		forNode: node,
	}, nil
}

func (p *ClientPool) ViewChange(newnode string) error {
	if len(p.servers) > 1 {
		var addrs []string
		for srv := range p.servers {
			addrs = append(addrs, "http://"+srv)
		}
		// Choose a random server and send it the new view.
		for name, handler := range p.servers {
			if name == newnode {
				continue // But not the one we just added.
			}
			if err := viewChange(name, handler, addrs); err != nil {
				return err
			}
			break
		}
	}
	return nil
}

func (c *Client) Do(r *http.Request) (*http.Response, error) {
	origin := c.forNode // This Client object is only for forNode; it is always the origin.
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

	httpreq, err := http.NewRequest(http.MethodPut, "http://"+name+"/view-change", &body)
	if err != nil {
		return err
	}
	httpreq.Header.Set("User-Agent", "test-harness")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httpreq)
	if recorder.Code != http.StatusOK {
		return fmt.Errorf("view change failed with code %d: %s", recorder.Code, recorder.Body.String())
	}
	return nil
}

type RecordedMessage struct {
	dst, src string
	method   string
	path     string
	request  string
	response string
	code     int
}

type Recorder struct {
	m      sync.Mutex
	record []RecordedMessage
}

func (rec *Recorder) MakeHandler(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		origin := r.Header.Get("User-Agent")
		method := r.Method
		var body []byte
		bodycopy, err := r.GetBody()
		if err == nil {
			body, _ = io.ReadAll(bodycopy)
			bodycopy.Close()
		}

		rec.m.Lock()
		// Record the request.
		rec.record = append(rec.record, RecordedMessage{
			dst:     r.Host,
			src:     origin,
			path:    r.URL.Path,
			method:  method,
			request: string(body),
		})
		rec.m.Unlock()

		inner.ServeHTTP(w, r)
		var response string
		var code int
		if testresponse, ok := w.(*httptest.ResponseRecorder); ok {
			response = testresponse.Body.String()
			code = testresponse.Code
		}

		// Record the response.
		rec.m.Lock()
		defer rec.m.Unlock()
		rec.record = append(rec.record, RecordedMessage{
			dst:      origin,
			src:      r.Host,
			path:     r.URL.Path,
			response: response,
			code:     code,
		})
	})
}

func (r *Recorder) ToSequence() string {
	participants := make(map[string]nothing)
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "sequenceDiagram\n")
	for _, msg := range r.record {
		if _, ok := participants[msg.src]; !ok {
			fmt.Fprintf(&buf, "\tparticipant %s\n", msg.src)
			participants[msg.src] = nothing{}
		}
		if _, ok := participants[msg.dst]; !ok {
			fmt.Fprintf(&buf, "\tparticipant %s\n", msg.dst)
			participants[msg.dst] = nothing{}
		}

		fmt.Fprintf(&buf, "\t%s->>%s: ", msg.src, msg.dst)
		if msg.request != "" {
			fmt.Fprintf(&buf, "%s %s%s", msg.method, msg.path, maybeKVString(msg.request))
		} else {
			fmt.Fprintf(&buf, "%d%s", msg.code, maybeKVString(msg.response))
		}
		fmt.Fprintf(&buf, "\n")

		if msg.path == "/gossip" {
			fmt.Fprintf(&buf, "\tNote over %s, %s: %s\n", msg.src, msg.dst, gossipLength(msg.response+msg.request))
		}

		if ctx := maybeCtx(msg.response); ctx != "" && msg.response != "" && (msg.path == "/write" || msg.path == "/read") {
			fmt.Fprintf(&buf, "\tNote over %s: %s\n", msg.dst, ctx)
		}
	}
	return buf.String()
}

func maybeKVString(in string) string {
	result := ""
	buf := bytes.NewBufferString(in)
	var blob map[string]any
	if err := json.NewDecoder(buf).Decode(&blob); err != nil {
		return result
	}
	key, hasKey := blob["key"]
	if hasKey {
		result += " " + key.(string)
	}
	value, hasValue := blob["value"]
	if hasValue {
		result += "=" + value.(string)
	}
	return result
}

func maybeCtx(in string) string {
	buf := bytes.NewBufferString(in)
	var blob map[string]any
	if err := json.NewDecoder(buf).Decode(&blob); err != nil {
		return ""
	}
	ctx, hasCtx := blob["causal-context"]
	if !hasCtx {
		return ""
	}
	return fmt.Sprintf(" %v", ctx)
}

func gossipLength(in string) string {
	buf := bytes.NewBufferString(in)
	var blob map[string]any
	if err := json.NewDecoder(buf).Decode(&blob); err != nil {
		return ""
	}
	cols, hasCols := blob["Columns"]
	if !hasCols {
		return ""
	}
	if s, ok := cols.([]any); ok {
		return fmt.Sprintf(" %d cols", len(s))
	}
	return "done"
}
