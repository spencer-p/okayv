package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sync"

	"github.com/google/uuid"
)

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type nothing struct{}

type Column struct {
	Key, Value string
	Clock      CausalClock
}

type CausalClock struct {
	ID         uuid.UUID
	Context    CausalContext
	Replicated map[string]nothing
}

type Server struct {
	client HTTPClient
	name   string
	peers  []*url.URL

	lock  sync.RWMutex
	maxcc CausalContext
	data  map[string]Column
}

func NewServer(mux *http.ServeMux, client HTTPClient, name string) *Server {
	srv := &Server{
		name:   name,
		client: client,
		maxcc:  make(CausalContext),
		data:   make(map[string]Column),
	}
	mux.HandleFunc("/read", JSONHandler(srv.read))
	mux.HandleFunc("/write", JSONHandler(srv.write))
	mux.HandleFunc("/view-change", JSONHandler(srv.viewChange))
	mux.HandleFunc("/gossip", JSONHandler(srv.recvGossip))
	return srv
}

type withError struct {
	any   `json:,inline`
	Error string `json:"error"`
}

func JSONHandler[In any, Out any](h func(In) (Out, error)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var in In
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		out, err := h(in)
		if err != nil {
			code := http.StatusInternalServerError
			if withcode, ok := err.(HttpError); ok {
				code = withcode.Code()
			}

			w.WriteHeader(code)
			if err := json.NewEncoder(w).Encode(withError{
				any:   out,
				Error: err.Error(),
			}); err != nil {
				log.Printf("Failed to marshal JSON to write response: %v", err)
				return
			}
		}

		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(&out); err != nil {
			log.Printf("Failed to marshal JSON to write response: %v", err)
			return
		}
	}
}

type KV struct {
	Key     string        `json:"key"`
	Value   string        `json:"value"`
	Context CausalContext `json:"causal-context,omitempty"`
}

type CausalContext map[string]int

func (cc *CausalContext) Mark(node string) {
	if *cc == nil {
		*cc = make(CausalContext)
	}
	(*cc)[node] = (*cc)[node] + 1
}

func (left CausalContext) AtMost(right CausalContext) bool {
	for srv, ctr := range left {
		if right[srv] < ctr {
			return false
		}
	}
	return true
}

func (a CausalContext) TakeMax(b CausalContext) CausalContext {
	next := maps.Clone(a)
	for node, ctr := range b {
		if ctr > next[node] {
			next[node] = ctr
		}
	}
	return next
}

func (s *Server) read(in KV) (KV, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if !in.Context.AtMost(s.maxcc) {
		return KV{}, newerr(http.StatusServiceUnavailable, fmt.Errorf("cannot service client"))
	}

	col, ok := s.data[in.Key]
	if !ok {
		return KV{}, newerr(http.StatusNotFound, fmt.Errorf("read %q: does not exist", in.Key))
	}
	return KV{
		Key:     col.Key,
		Value:   col.Value,
		Context: col.Clock.Context.TakeMax(in.Context),
	}, nil
}

func (s *Server) write(in KV) (KV, error) {
	return s.update(in, true)
}

func (s *Server) update(in KV, allowRewrite bool) (KV, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !in.Context.AtMost(s.maxcc) {
		return KV{}, newerr(http.StatusServiceUnavailable, fmt.Errorf("cannot service client"))
	}

	existing, alreadyExists := s.data[in.Key]
	if alreadyExists && !allowRewrite {
		return KV{
			Key:     existing.Key,
			Value:   existing.Value,
			Context: existing.Clock.Context,
		}, newerr(http.StatusBadRequest, fmt.Errorf("already exists"))
	}

	in.Context.Mark(s.name)
	newclock := CausalClock{
		ID:         uuid.New(),
		Context:    in.Context,
		Replicated: map[string]nothing{s.name: {}},
	}

	s.data[in.Key] = Column{
		Key:   in.Key,
		Value: in.Value,
		Clock: newclock,
	}
	s.maxcc = s.maxcc.TakeMax(in.Context)
	return in, nil // NB: `in` was updated in place.
}

type ViewChange struct {
	Replicas     []string `json:"replicas"`
	DoNotForward bool     `json:"donotforward,omitempty"`
}

func (s *Server) viewChange(in ViewChange) (nothing, error) {
	var next []*url.URL
	for _, replica := range in.Replicas {
		addr, err := url.Parse(replica)
		if err != nil {
			return nothing{}, err
		}
		if addr.Host == s.name {
			continue
		}
		next = append(next, addr)
		if !in.DoNotForward {
			if err := s.forwardViewChange(in, replica); err != nil {
				return nothing{}, err
			}
		}
	}
	s.peers = next // TODO: Data race/hazard here.
	// TODO: Maybe wait for replication or manually start replication.
	return nothing{}, nil
}

func (s *Server) forwardViewChange(in ViewChange, addr string) error {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(ViewChange{
		Replicas:     in.Replicas[:],
		DoNotForward: true,
	}); err != nil {
		return err
	}

	httpreq, err := http.NewRequest(http.MethodPut, addr+"/view-change", &body)
	if err != nil {
		return err
	}
	httpreq.Header.Set("User-Agent", s.name)
	resp, err := s.client.Do(httpreq)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return newerr(http.StatusInternalServerError, fmt.Errorf("forward view change to %s failed: %d", addr, resp.StatusCode))
	}
	return nil
}
