package tsgen

import "testing"

func TestParse(t *testing.T) {
	raw := []byte{
		0, 1, // Register node 1.
		1, 0, 1, 9, 9, // Alice writes 9=9 to node 1.
		2, 0, 1, 9, // Alice reads 9 from node 1.
	}

	p, err := Parse(raw)
	if err != nil {
		t.Errorf("failed to parse: %v", err)
	}
	if len(p) != 3 {
		t.Errorf("got %d instructions, wanted %d", len(p), 3)
	}
}
