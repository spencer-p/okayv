package harness

import (
	"context"
	"math/rand"
	"os"
	"testing"

	"github.com/spencer-p/okayv/client"
	"github.com/spencer-p/okayv/tsgen"
)

func FuzzOkayV(f *testing.F) {
	f.Add([]byte{
		0, 1, // Register node 1.
		0, 2, // Register node 2.
		3, 0, 1, 2, 2, // Alice writes 2=2 to node 1.
		4, 0, 1, 9, // Alice reads 2 from node 1.
		3, 0, 1, 3, 3, // Alice writes 3=3 from node 1.
		3, 0, 2, 3, // Alice reads 3 from node 2.
	})
	f.Fuzz(func(t *testing.T, input []byte) {
		program, err := tsgen.Parse(input)
		if err != nil {
			t.Skipf("program invalid: %v", err)
		}
		if len(program) == 0 {
			t.Skipf("empty program")
		}

		for i := 0; i < 10; i++ {
			rand.Seed(int64(i))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			model := tsgen.NewModel()
			recorder := Recorder{}
			impl := &MyImpl{
				ctx:            ctx,
				srvclientpool:  NewClientPool(model, &recorder),
				realclientpool: make(map[string]*client.Client),
			}
			for _, instr := range program {
				if err := instr.Apply(model, impl); err != nil {
					t.Errorf("%#v error: %v", instr, err)
				}
				// Randomly allow all servers to gossip.
				if rand.Int()%2 == 0 {
					for _, s := range impl.servers {
						s.Gossip()
					}
				}
			}

			err = tsgen.ValidateCausality(impl.Record)
			if err != nil {
				t.Errorf("causality violated: %v", err)
				for i, r := range impl.Record {
					t.Logf("%d\t%#v", i, r)
				}
			}

			debug := os.Getenv("DEBUG") != ""
			if t.Failed() || debug {
				t.Logf("program:")
				for i, instr := range program {
					t.Logf("%d\t%#v", i, instr)
				}
				file, err := writeSequenceHTML(recorder.ToSequence())
				if err != nil {
					t.Errorf("failed to write sequence: %v", err)
				} else {
					t.Logf("wrote sequence to %s", file)
				}
				return // end test
			}
		}
	})
}
