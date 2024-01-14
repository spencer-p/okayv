package tsgen

import "testing"

func TestModelReachable(t *testing.T) {
	table := []struct {
		name          string
		nodes         []string
		cuts          []string
		connect       []string
		wantReachable bool
	}{{
		name:          "no partitions",
		nodes:         []string{"a", "b"},
		cuts:          []string{},
		wantReachable: true,
	}, {
		name:          "simple partition",
		nodes:         []string{"a", "b"},
		cuts:          []string{"a", "b"},
		wantReachable: false,
	}, {
		name:          "swapped simple partition",
		nodes:         []string{"a", "b"},
		cuts:          []string{"b", "a"},
		wantReachable: false,
	}, {
		name:  "indirect reachable",
		nodes: []string{"a", "c", "d", "b"},
		cuts: []string{
			"a", "d",
			"a", "b",
			"c", "b",
			"d", "a",
		},
		wantReachable: true,
	}, {
		name:  "indirect unreachable",
		nodes: []string{"a", "c", "d", "b"},
		cuts: []string{
			"a", "d",
			"a", "b",
			"c", "b",
			"d", "a",
			"c", "d", // This cut breaks the chain.
		},
		wantReachable: false,
	}, {
		name:  "reconnect unreachable",
		nodes: []string{"a", "c", "d", "b"},
		cuts: []string{
			"a", "d",
			"a", "b",
			"c", "b",
			"d", "a",
			"c", "d", // This cut breaks the chain.
		},
		connect: []string{
			"c", "d",
		},
		wantReachable: true,
	}}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			m := NewModel()
			for _, n := range tc.nodes {
				m.Add(n)
			}

			for i := 0; i+1 < len(tc.cuts); i += 2 {
				m.Partition(tc.cuts[i], tc.cuts[i+1])
			}

			for i := 0; i+1 < len(tc.connect); i += 2 {
				m.Connect(tc.connect[i], tc.connect[i+1])
			}

			result := m.Reachable("a", "b")
			if result != tc.wantReachable {
				t.Errorf("m.Reachable(a, b) = %t, wanted %t", result, tc.wantReachable)
			}
		})
	}
}
