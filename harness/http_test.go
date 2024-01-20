package harness

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/spencer-p/okayv/client"
	"github.com/spencer-p/okayv/server"
	"github.com/spencer-p/okayv/tsgen"
)

func TestSimple(t *testing.T) {
	p := tsgen.Program{
		tsgen.RegisterNode{
			Node: "a",
		},
		tsgen.Write{
			Client: "alice",
			Node:   "a",
			Key:    "x",
			Value:  "1",
		},
		tsgen.Read{
			Client: "alice",
			Node:   "a",
			Key:    "x",
		},
		tsgen.Read{
			Client: "alice",
			Node:   "a",
			Key:    "y",
		},
		tsgen.RegisterNode{
			Node: "b",
		},
		tsgen.Write{
			Client: "alice",
			Node:   "b",
			Key:    "y",
			Value:  "2",
		},
		tsgen.Read{
			Client: "alice",
			Node:   "b",
			Key:    "x",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	model := tsgen.NewModel()
	recorder := Recorder{}
	impl := &MyImpl{
		ctx:            ctx,
		srvclientpool:  NewClientPool(model, &recorder),
		realclientpool: make(map[string]*client.Client),
	}
	for _, instr := range p {
		if err := instr.Apply(model, impl); err != nil {
			t.Errorf("%#v error: %v", instr, err)
		}
	}
	err := tsgen.ValidateCausality(impl.Record)
	if err != nil {
		t.Logf("causality violated: %v", err)
		for i, r := range impl.Record {
			t.Logf("%d\t%#v", i, r)
		}
	}
	file, err := writeSequenceHTML(recorder.ToSequence())
	if err != nil {
		t.Errorf("failed to write sequence: %v", err)
	} else {
		t.Logf("wrote sequence to %s", file)
	}
}

type MyImpl struct {
	ctx            context.Context
	servers        []*server.Server
	srvclientpool  *ClientPool
	realclientpool map[string]*client.Client
	Record         []any
}

var _ tsgen.Impl = &MyImpl{}

func (i *MyImpl) CreateNode(nodename string) error {
	mux := http.NewServeMux()
	cli, err := i.srvclientpool.ClientFor(nodename, mux)
	if err != nil {
		return err
	}
	s := server.NewServer(mux, cli, nodename, 10*time.Millisecond)
	if err := i.srvclientpool.ViewChange(nodename); err != nil {
		return err
	}
	i.servers = append(i.servers, s)

	return nil
}

func (i *MyImpl) Read(clientname, node, key string) error {
	c := i.realClient(clientname)
	c.SetAddress("http://" + node)
	result, err := c.Read(key)
	i.Record = append(i.Record, tsgen.ReadResult{
		Client:   clientname,
		Node:     node,
		Key:      key,
		Value:    result,
		Error:    errors.Is(err, client.ErrUnavailable),
		NotFound: errors.Is(err, client.ErrNotFound),
	})
	// Return the error only if it is not an expected error.
	if err != nil &&
		!errors.Is(err, client.ErrNotFound) &&
		!errors.Is(err, client.ErrUnavailable) {
		return err
	}
	return nil
}

func (i *MyImpl) Write(clientname, node, key, value string) error {
	c := i.realClient(clientname)
	c.SetAddress("http://" + node)
	err := c.Write(key, value)
	if errors.Is(err, client.ErrUnavailable) {
		return nil // Not a fatal error for test, but not a sucessful write.
	} else if err != nil {
		return err
	}
	i.Record = append(i.Record, tsgen.Write{
		Client: clientname,
		Node:   node,
		Key:    key,
		Value:  value,
	})
	return nil
}

func (i *MyImpl) realClient(clientname string) *client.Client {
	c, ok := i.realclientpool[clientname]
	if ok {
		return c
	}

	c = client.NewClient(i.srvclientpool.AlwaysReachable(), clientname, "address-to-be-replaced")
	i.realclientpool[clientname] = c
	return c
}

func writeSequenceHTML(contents string) (string, error) {
	w, err := os.CreateTemp("", "sequence-*.html")
	if err != nil {
		return "", err
	}
	defer w.Close()
	w.Write([]byte(`<!DOCTYPE html>
<html>
<script type="module">
  import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
</script>
<body style="background-color: #1b1b1f"
<pre class=mermaid>
---
title: Test Run Sequence Diagram
config:
  theme: dark
  loglevel: debug
---
`))
	w.Write([]byte(contents))
	w.Write([]byte("</pre></body></html>"))
	return "file://" + w.Name(), nil
}
