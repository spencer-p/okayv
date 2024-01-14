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
	}}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.actions)
			got := err == nil
			if got != tc.valid {
				t.Errorf("Validate returned %t, wanted %t", got, tc.valid)
				t.Errorf("error: %v", err)
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
