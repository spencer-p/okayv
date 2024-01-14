package harness

import (
	"net/http"
	"testing"

	"github.com/spencer-p/okayv/client"
	"github.com/spencer-p/okayv/server"
	"github.com/spencer-p/okayv/tsgen"
)

func TestSimple(t *testing.T) {
	p := tsgen.Program{
		tsgen.RegisterNode{"a"},
		tsgen.Write{
			ClientName: "alice",
			NodeName:   "http://a",
			Key:        "x",
			Value:      "1",
		},
		tsgen.Read{
			ClientName: "alice",
			NodeName:   "http://a",
			Key:        "x",
		},
		tsgen.Read{
			ClientName: "alice",
			NodeName:   "http://a",
			Key:        "y",
		},
	}

	model := tsgen.NewModel()
	impl := &MyImpl{
		srvclientpool:  NewClientPool(model),
		realclientpool: make(map[string]client.Client),
	}
	for _, instr := range p {
		if err := instr.Apply(model, impl); err != nil {
			t.Errorf("%#v error: %v", instr, err)
		}
	}
}

type MyImpl struct {
	srvclientpool  *ClientPool
	realclientpool map[string]client.Client
}

var _ tsgen.Impl = &MyImpl{}

func (i *MyImpl) CreateNode(nodename string) error {
	mux := http.NewServeMux()
	cli := i.srvclientpool.ClientFor(nodename, mux)
	_ = server.NewServer(mux, cli)
	return nil
}

func (i *MyImpl) Read(client, node, key string) error {
	c := i.realClient(client)
	c.SetAddress(node)
	_, err := c.Read(key)
	if err != nil {
		return err
	}
	return nil
}

func (i *MyImpl) Write(client, node, key, value string) error {
	c := i.realClient(client)
	c.SetAddress(node)
	err := c.Write(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (i *MyImpl) realClient(clientname string) client.Client {
	c, ok := i.realclientpool[clientname]
	if ok {
		return c
	}

	c = client.NewClient(i.srvclientpool.AlwaysReachable(), "temp")
	i.realclientpool[clientname] = c
	return c
}
