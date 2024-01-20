package harness

import (
	"testing"

	"github.com/spencer-p/okayv/client"
	"github.com/spencer-p/okayv/tsgen"
)

func FuzzOkayV(f *testing.F) {
	f.Fuzz(func(t *testing.T, input []byte) {
		program, err := tsgen.Parse(input)
		if err != nil {
			t.Skipf("program invalid: %v", err)
		}

		model := tsgen.NewModel()
		recorder := Recorder{}
		impl := &MyImpl{
			srvclientpool:  NewClientPool(model, &recorder),
			realclientpool: make(map[string]*client.Client),
		}
		for _, instr := range program {
			if err := instr.Apply(model, impl); err != nil {
				t.Errorf("%#v error: %v", instr, err)
			}
		}

		err = tsgen.ValidateCausality(impl.Record)
		if err != nil {
			t.Errorf("causality violated: %v", err)
			for i, r := range impl.Record {
				t.Logf("%d\t%#v", i, r)
			}
			file, err := writeSequenceHTML(recorder.ToSequence())
			if err != nil {
				t.Errorf("failed to write sequence: %v", err)
			} else {
				t.Logf("wrote sequence to %s", file)
			}
		}
	})
}
