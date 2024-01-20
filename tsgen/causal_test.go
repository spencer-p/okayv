package tsgen

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	table := []struct {
		name    string
		actions []any
		valid   bool
	}{{
		name: "one replica, one read",
		actions: []any{
			w("alice to a: x=1"),
			r("alice from a: x=1"),
		},
		valid: true,
	}, {
		name: "one replica, invalid read",
		actions: []any{
			w("alice to a: x=1"),
			r("alice from a: x=2"),
		},
		valid: false,
	}, {
		name: "unordered read",
		actions: []any{
			w("alice to a: x=true"),
			w("bob to b: x=false"),
			r("alice from a: x=false"),
		},
		valid: true,
	}, {
		name: "time reversal, want invalid",
		actions: []any{
			w("alice to a: x=1"),
			w("alice to a: x=2"),
			r("alice from a: x=2"),
			r("alice from a: x=1"),
		},
		valid: false,
	}, {
		name: "rename me",
		actions: []any{
			w("alice to a: X=x"),
			w("alice to b: Y=y"),
			r("bob from b: Y=y"),
			w("bob to a: Z=z"),
		},
		valid: true,
	}, {
		name: "collab",
		actions: []any{
			w("alice to a: X=x"),
			w("bob to b: Y=y"),
			r("alice from a: Y=y"),
			w("alice to a: Z=z"),
			r("bob from b: Z=z"),
			r("bob from a: X=x"),
		},
		valid: true,
	}, {
		name: "collab invalid",
		actions: []any{
			w("alice to a: X=x"),
			w("bob to b: Y=y"),
			r("alice from a: Y=y"),
			w("alice to a: Z=z"),
			r("bob from b: Z=z"),
			r("bob from a: X=notfound"),
		},
		valid: false,
	}, {
		name: "valid error",
		actions: []any{
			w("alice to a: X=x"),
			r("bob from a: X=error"),
		},
		valid: true,
	}, {
		name: "valid notfound",
		actions: []any{
			r("bob from a: X=notfound"),
		},
		valid: true,
	}, {
		name: "invalid notfound",
		actions: []any{
			w("bob to a: X=1"),
			r("bob from a: X=notfound"),
		},
		valid: false,
	}, {
		name: "write failure, ok to 404",
		actions: []any{
			w("bob to a: X=error"),
			r("bob from a: X=notfound"),
		},
		valid: true,
	}, {
		name: "my odd question answer 1",
		actions: []any{
			w("c1 to n1: x=1"),
			r("c2 from n2: x=notfound"),
			w("c1 to n1: y=2"),
			r("c2 from n2: y=notfound"),
		},
		valid: true,
	}, {
		name: "my odd question answer 2",
		actions: []any{
			w("c1 to n1: x=1"),
			r("c2 from n2: x=notfound"),
			w("c1 to n1: y=2"),
			r("c2 from n2: y=2"),
		},
		valid: true,
	}, {
		name: "my odd question reversed",
		actions: []any{
			w("c1 to n1: x=1"),
			w("c1 to n1: y=2"),
			r("c2 from n2: y=2"),
			r("c2 from n2: x=notfound"),
		},
		valid: false,
	}, {
		name: "false positive fuzz #1",
		actions: []any{
			w("c1 to n1: x=1"),
			r("c2 from n1: x=1"),
			w("c1 to n1: x=2"),
			r("c2 from n1: x=2"),
		},
		valid: true,
	}, {
		name: "false positive fuzz #2 - divergent writes",
		actions: []any{
			w("c1 to n1: x=1"),
			r("c2 from n1: x=1"),
			w("c1 to n1: x=2"),
			w("c2 to n1: y=3"),
			r("c2 from n1: x=2"),
		},
		valid: true,
	}, {
		name: "false positive fuzz #2 - divergent writes plus concurrent write",
		actions: []any{
			w("c1 to n1: x=1"),
			r("c2 from n1: x=1"),
			w("c1 to n1: x=2"),
			w("c2 to n1: y=3"),
			w("c3 to n2: x=4"),
			r("c2 from n1: x=4"),
		},
		valid: true,
	}}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCausality(tc.actions)
			got := err == nil
			if got != tc.valid {
				t.Errorf("Validate returned %t, wanted %t", got, tc.valid)
				t.Errorf("error: %v", err)
				for _, root := range err.(CausalError).Roots {
					printTree(t, 0, root)
				}
				cursors := err.(CausalError).Cursors
				for client, curs := range cursors {
					for i, c := range curs {
						t.Logf("client %s has cursor %d at %s=%s", client, i, c.key, c.value)
					}
				}
			}
		})
	}
}

var writeRegex = regexp.MustCompile(strings.Join([]string{
	"(\\w+)", // Client.
	" to ",
	"(\\w+)", // Node.
	": ",
	"(\\w+)", // Key.
	"=",
	"(\\w+)", // Value.
}, ""))

func w(s string) Write {
	result := writeRegex.FindStringSubmatch(s)
	if len(result) != 5 {
		panic(fmt.Errorf("invalid write syntax: %q", s))
	}
	return Write{
		Client: result[1],
		Node:   result[2],
		Key:    result[3],
		Value:  result[4],
	}
}

var readRegex = regexp.MustCompile(strings.Join([]string{
	"(\\w+)", // Client.
	" from ",
	"(\\w+)", // Node.
	": ",
	"(\\w+)", // Key.
	"=",
	"(\\w+|error|notfound)", // Value or error.
}, ""))

func r(s string) ReadResult {
	result := readRegex.FindStringSubmatch(s)
	if len(result) != 5 {
		panic(fmt.Errorf("invalid read syntax: %q", s))
	}
	return ReadResult{
		Client:   result[1],
		Node:     result[2],
		Key:      result[3],
		Value:    result[4],
		Error:    result[4] == "error",
		NotFound: result[4] == "notfound",
	}
}

func printTree(t *testing.T, depth int, root *treenode) {
	s := ""
	for i := 0; i < depth; i++ {
		s += "  "
	}

	s += fmt.Sprintf("%s=%s", root.key, root.value)
	t.Logf(s)
	for _, next := range root.after {
		printTree(t, depth+1, next)
	}
}
