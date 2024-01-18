package harness

import (
	"errors"
	"net/http"
	"testing"

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

	model := tsgen.NewModel()
	impl := &MyImpl{
		srvclientpool:  NewClientPool(model),
		realclientpool: make(map[string]*client.Client),
	}
	for _, instr := range p {
		if err := instr.Apply(model, impl); err != nil {
			t.Errorf("%#v error: %v", instr, err)
		}
	}
	err := tsgen.ValidateCausality(impl.Record)
	if err != nil {
		t.Errorf("causality violated: %v", err)
		for i, r := range impl.Record {
			t.Logf("%d\t%#v", i, r)
		}
	}
}

type MyImpl struct {
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
	_ = server.NewServer(mux, cli, nodename)

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
	if err != nil &&
		!errors.Is(err, client.ErrUnavailable) {
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

	c = client.NewClient(i.srvclientpool.AlwaysReachable(), "temp")
	i.realclientpool[clientname] = c
	return c
}
