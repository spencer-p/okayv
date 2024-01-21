package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	"github.com/google/uuid"
)

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type nothing struct{}

type Column struct {
	Key, Value string
	Clock      CausalClock
	Timestamp  time.Time
}

type CausalClock struct {
	ID         uuid.UUID
	Context    VectorClock
	Replicated map[string]nothing
}

type Server struct {
	*log.Logger
	client     HTTPClient
	name       string
	peers      []*url.URL
	gossipFreq time.Duration

	lock   sync.RWMutex
	maxcc  VectorClock
	events []Column
	latest map[string]int
	acked  map[string]int
	byid   map[string]int
}

func NewServer(mux *http.ServeMux, client HTTPClient, name string, gossipFreq time.Duration) *Server {
	srv := &Server{
		Logger: log.NewWithOptions(os.Stderr, log.Options{
			Prefix: fmt.Sprintf("[%s]", name),
		}),
		name:       name,
		client:     client,
		maxcc:      make(VectorClock),
		latest:     make(map[string]int),
		acked:      make(map[string]int),
		byid:       make(map[string]int),
		gossipFreq: gossipFreq,
	}
	mux.HandleFunc("/read", JSONHandler(srv.read))
	mux.HandleFunc("/write", JSONHandler(srv.write))
	mux.HandleFunc("/view-change", JSONHandler(srv.viewChange))
	mux.HandleFunc("/gossip", JSONHandler(srv.recvGossip))
	srv.Infof("Starting")
	return srv
}

func (s *Server) RunBackground(ctx context.Context) {
	tick := time.NewTicker(s.gossipFreq)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.Gossip()
		}
	}
}

func (s *Server) Gossip() {
	if len(s.peers) == 0 {
		return
	}
	i := rand.Intn(len(s.peers))
	s.gossipOnce(s.peers[i])
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

		w.Header().Set("Content-Type", "application/json")
		out, err := h(in)
		if err != nil {
			code := http.StatusInternalServerError
			if withcode, ok := err.(HttpError); ok {
				code = withcode.Code()
			}

			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(withError{
				any:   out,
				Error: err.Error(),
			})
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(&out)
	}
}

type KV struct {
	Key     string      `json:"key"`
	Value   string      `json:"value"`
	Context VectorClock `json:"causal-context,omitempty"`
}

func (s *Server) read(in KV) (KV, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	s.Info("Read", "key", in.Key, "ctx", in.Context)

	if s.maxcc.Behind(in.Context) {
		return KV{}, newerr(http.StatusServiceUnavailable, fmt.Errorf("cannot service client"))
	}

	col, ok := s.lookup(in.Key)
	if !ok {
		return in, newerr(http.StatusNotFound, fmt.Errorf("read %q: does not exist", in.Key))
	}
	newctx := col.Clock.Context.Clone()
	newctx.TakeMax(in.Context)
	return KV{
		Key:     col.Key,
		Value:   col.Value,
		Context: newctx,
	}, nil
}

func (s *Server) write(in KV) (KV, error) {
	s.Info("Write", "key", in.Key, "val", in.Value, "ctx", in.Context)
	return s.update(in, true)
}

func (s *Server) update(in KV, allowRewrite bool) (KV, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.maxcc.Behind(in.Context) {
		return KV{}, newerr(http.StatusServiceUnavailable, fmt.Errorf("cannot service client"))
	}

	existing, alreadyExists := s.lookup(in.Key)
	if alreadyExists && !allowRewrite {
		return KV{
			Key:     existing.Key,
			Value:   existing.Value,
			Context: existing.Clock.Context,
		}, newerr(http.StatusBadRequest, fmt.Errorf("already exists"))
	}

	// If the client is writing something we already have, ack w/o doing
	// anything but advance their clock if needed.
	if alreadyExists && in.Key == existing.Key && in.Value == existing.Value {
		in.Context.TakeMax(existing.Clock.Context)
		return in, nil
	}

	s.maxcc.TakeMax(in.Context)
	s.maxcc.Mark(s.name)
	newclock := CausalClock{
		ID:         uuid.New(),
		Context:    s.maxcc.Clone(),
		Replicated: map[string]nothing{s.name: {}},
	}

	s.events = append(s.events, Column{
		Key:       in.Key,
		Value:     in.Value,
		Clock:     newclock,
		Timestamp: time.Now(),
	})
	s.latest[in.Key] = len(s.events) - 1
	s.byid[newclock.ID.String()] = len(s.events) - 1
	return KV{
		Key:     in.Key,
		Value:   in.Value,
		Context: newclock.Context,
	}, nil
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

func (s *Server) gossipOnce(dst *url.URL) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	startIdx := s.indexNotAcked(dst.Host)
	replicate := s.events[startIdx:]
	if len(replicate) == 0 {
		return nil
	}
	req := Gossip{
		Host:    s.name,
		Columns: replicate,
	}

	// Push to other server.
	s.Info("Send gossip", "dst", dst.Host, "cols", len(req.Columns))
	var resp GossipResponse
	if err := s.JSONRequest(http.MethodPut, dst.String()+"/gossip", req, &resp); err != nil {
		return err
	}

	// Play back the columns we got back, then ack them to the dst.
	// At this point resp is expected to be empty.
	req.Columns = s.playLog(dst.Host, resp.Columns)
	if len(req.Columns) == 0 {
		// End gossip if there is nothing to ack.
		return nil
	}
	s.Info("Acking gossip", "dst", dst.Host, "acks", len(req.Columns))
	if err := s.JSONRequest(http.MethodPut, dst.String()+"/gossip", req, &resp); err != nil {
		return err
	}
	if len(resp.Columns) > 0 {
		s.Warn("Expected no more gossip, dropping", "dst", dst.Host, "cols", len(resp.Columns))
	}

	return nil
}

type Gossip struct {
	Host    string
	Columns []Column
}

type GossipResponse struct {
	Columns []Column
}

func (s *Server) recvGossip(in Gossip) (GossipResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.Info("Receiving gossip", "src", in.Host, "cols", len(in.Columns))

	updated := s.playLog(in.Host, in.Columns)
	startIdx := s.indexNotAcked(in.Host)
	replicate := s.events[startIdx:]
	s.Info("Gossip reply", "cols", len(replicate), "acks", len(updated))
	resp := GossipResponse{
		Columns: append(replicate, updated...),
	}

	return resp, nil
}

// playLog plays a log of columns onto history and returns a list of updated
// columns. Not all columns may be played.
// playLog assumes the write lock is held.
func (s *Server) playLog(host string, log []Column) (updated []Column) {
	for _, col := range log {
		// If the event is already recorded, only update the replication data.
		if existing, ok := s.lookupID(col.Clock.ID); ok {
			if !existing.Clock.Equal(col.Clock) {
				s.Info("Updating replication metadata", "key", col.Key)
				existing.Clock.Merge(col.Clock)
				s.maxcc.TakeMax(existing.Clock.Context)
				updated = append(updated, existing)
			} else {
				s.Info("Skipping prev. acked", "key", col.Key)
			}
			continue
		}

		// Stop processing if we find events that are too far in the future.
		concurrent := s.maxcc.Concurrent(col.Clock.Context)
		happensafter := s.maxcc.AheadOneN(col.Clock.Context, len(col.Clock.Replicated))
		if !(concurrent || happensafter) {
			s.Warn("Cannot ack further", "key", col.Key, "val", col.Value, "us", s.maxcc, "them", col.Clock.Context, "repl", col.Clock.Replicated)
			return updated
		}

		// If there is a merge conflict on concurrent writes, choose a winner.
		// If the local copy wins, stop processing.
		if concurrent {
			if existing, exists := s.lookup(col.Key); exists {
				s.Warn("Breaking tie by timestamp",
					"key", col.Key,
					"localval", existing.Value,
					"remoteval", col.Value,
					"localtime", existing.Timestamp,
					"remotetime", col.Timestamp)
				if existing.Timestamp.After(col.Timestamp) {
					s.Warn("Dropping remote write", "key", col.Key, "val", col.Value)
					// Instead of short-circuiting, we take the event count but
					// drop the column, simulating if the event had happened and
					// was overwritten.
					s.maxcc.TakeMax(col.Clock.Context)
					continue
				}
			}
		}

		// The event was concurrent w.r.t us or it's one event after, we can
		// accept it.
		s.Info("Logging event", "key", col.Key, "val", col.Value, "ctx", col.Clock.Context, "repl", col.Clock.Replicated)

		// Build the new clock, marking it as an event ourselves.
		s.maxcc.TakeMax(col.Clock.Context)
		s.maxcc.Mark(s.name)
		col.Clock.Context = s.maxcc.Clone()
		col.Clock.Replicated[s.name] = nothing{}

		// Append it to history.
		s.events = append(s.events, col)
		s.latest[col.Key] = len(s.events) - 1
		s.byid[col.Clock.ID.String()] = len(s.events) - 1
		updated = append(updated, col)
	}
	return updated
}

func (s *Server) lookup(key string) (Column, bool) {
	idx, ok := s.latest[key]
	if !ok {
		return Column{}, false
	}
	return s.events[idx], true
}

func (s *Server) lookupID(id uuid.UUID) (Column, bool) {
	idx, ok := s.byid[id.String()]
	if !ok {
		return Column{}, false
	}
	return s.events[idx], true
}

func (s *Server) JSONRequest(method string, addr string, input any, output any) error {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(input); err != nil {
		return err
	}

	httpreq, err := http.NewRequest(method, addr, &body)
	if err != nil {
		return err
	}
	httpreq.Header.Set("User-Agent", s.name)
	httpreq.Header.Set("Content-Type", "application/json")
	httpresp, err := s.client.Do(httpreq)
	if err != nil {
		return err
	}
	defer httpresp.Body.Close()

	if err := json.NewDecoder(httpresp.Body).Decode(output); err != nil {
		return err
	}
	return nil
}

func (cc *CausalClock) Merge(other CausalClock) {
	cc.Context.TakeMax(other.Context)
	for replicated := range other.Replicated {
		cc.Replicated[replicated] = nothing{}
	}
}

func (cc *CausalClock) Equal(other CausalClock) bool {
	return cc.ID == other.ID && maps.Equal(cc.Context, other.Context) && maps.Equal(cc.Replicated, cc.Replicated)
}

func (s *Server) indexNotAcked(remote string) int {
	// NB: Acked holds the index such that all prior indices are acked.
	for ; s.acked[remote] < len(s.events); s.acked[remote]++ {
		i := s.acked[remote]
		_, acked := s.events[i].Clock.Replicated[remote]
		if !acked {
			break
		}
	}
	return s.acked[remote]
}
