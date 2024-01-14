package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type column struct {
	key, value string
	clock      any // TODO
}

type Server struct {
	client HTTPClient

	data map[string][]column
}

func NewServer(mux *http.ServeMux, client HTTPClient) *Server {
	srv := &Server{
		client: client,
		data:   make(map[string][]column),
	}
	mux.HandleFunc("/read", JSONHandler(srv.read))
	mux.HandleFunc("/write", JSONHandler(srv.write))
	return srv
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(&out); err != nil {
			log.Printf("Failed to marshal JSON to write response: %v", err)
			return
		}
	}
}

type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Server) read(in KV) (KV, error) {
	cols, ok := s.data[in.Key]
	if !ok || len(cols) == 0 {
		return KV{}, fmt.Errorf("not found")
	}
	col := cols[len(cols)-1]
	return KV{
		Key:   col.key,
		Value: col.value,
	}, nil
}

func (s *Server) write(in KV) (struct{}, error) {
	s.data[in.Key] = append(s.data[in.Key], column{
		key:   in.Key,
		value: in.Value,
	})
	return struct{}{}, nil
}
