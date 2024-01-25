package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/charmbracelet/log"
	"github.com/spencer-p/okayv/server"
)

func main() {
	name := os.Getenv("HOST")
	if name == "" {
		var err error
		name, err = os.Hostname()
		if err != nil {
			fmt.Fprintf(os.Stderr, "no hostname: %v", err)
			return
		}
	}
	l := log.WithPrefix(fmt.Sprintf("[%s]", name))

	mux := http.NewServeMux()
	cli := http.DefaultClient
	s := server.NewServer(mux,
		server.Opts{
			Logger:     l,
			Client:     cli,
			Name:       name,
			GossipFreq: 1 * time.Second,
		})
	go s.RunBackground(context.TODO())

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	httpServer := http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  1 * time.Minute,
		WriteTimeout: 1 * time.Minute,
	}

	l.Info("Serving", "addr", httpServer.Addr)
	l.Error("Exiting", "err", httpServer.ListenAndServe())
}
