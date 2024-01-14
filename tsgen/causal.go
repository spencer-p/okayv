package tsgen

import "fmt"

type ReadResult struct {
	Client, Node string
	Key, Value   string
	Error        bool
	NotFound     bool
}

type treenode struct {
	key, value string
	deleted    bool
	after      []*treenode
	before     []*treenode
}

// Validate validates a slice of ordered actions which must be Write or
// ReadResult.
// TODO: Acknowledge write errors.
// TODO: Accept delete instructions.
func Validate(actions []any) error {
	roots := map[string]*treenode{}
	cursors := map[string][]*treenode{}
	for actioni, a := range actions {
		switch v := a.(type) {
		case Write:
			n := &treenode{
				key:   v.Key,
				value: v.Value,
			}
			curs, ok := cursors[v.Client]
			if !ok {
				// Client is new, no current context, creates a new root.
				roots[v.Client] = n
			} else {
				// Client's new write is causally related to its current
				// cursors. Add happens-before relationship to tree.
				for _, cur := range curs {
					// Read: cur "happens before" n.
					n.before = append(n.before, cur)
					cur.after = append(cur.after, n)
				}
			}
			// Drop all the prior cursors, as they are all causal precursors to
			// the new one.
			cursors[v.Client] = []*treenode{n}
		case ReadResult:
			if v.Error {
				// The store is always allowed to be unavailable.
				// Shrug.
				continue
			}
			err := func() error {
				considered := 0
				for idx, cursor := range cursors[v.Client] {
					// Find immediate instances of Key and see if they have Value.
					// If not found allowed to traverse disconnected trees
					candidates := searchhistory(v.Key, cursor, happenedbefore)
					for _, c := range candidates {
						if c.value == v.Value || (c.deleted && v.NotFound) {
							// Valid read in prior history.
							return nil
						}
					}
					considered += len(candidates)

					// Find candidates in the "future".
					candidates = searchhistory(v.Key, cursor, happenedafter)
					for _, c := range candidates {
						if c.value == v.Value || (c.deleted && v.NotFound) {
							// Valid read from the future.
							// This cursor has now advanced.
							cursors[v.Client][idx] = c
							return nil
						}
					}
					considered += len(candidates)
				}
				// If no cursor for the client could see a value and we got a
				// 404, then that's valid.
				if v.NotFound && considered == 0 {
					return nil
				}
				// Start descending roots to add a new cursor.
				for _, root := range roots {
					independent, ok := searchunrelated(v.Key, v.Value, root, cursors[v.Client])
					if ok {
						// Valid independent read.
						// Creates new cursor
						cursors[v.Client] = append(cursors[v.Client], independent)
						return nil
					}
				}
				return fmt.Errorf("%s cannot read %s=%s at index %d", v.Client, v.Key, v.Value, actioni)
			}()
			if err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("type error: invalid action type %T", a))
		}
	}
	return nil
}

// searchhistory searches history starting from roots for any reachable matching
// keys. The paths from root to matching key never contain the key itself.
func searchhistory(key string, root *treenode, next func(*treenode) []*treenode) []*treenode {
	matches := []*treenode{}
	queue := []*treenode{root}
	queued := map[*treenode]struct{}{}
	for _, q := range queue {
		queued[q] = struct{}{}
	}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur.key == key {
			matches = append(matches, cur)
			continue // Never traverse past a valid response, as that would skip history.
		}
		for _, toq := range next(cur) {
			if _, seen := queued[toq]; seen {
				continue
			}
			queue = append(queue, toq)
			queued[toq] = struct{}{}
		}
	}
	return matches

}

// Find a node with k,v such that that node cannot reach anything in unrelated.
func searchunrelated(k, v string, root *treenode, unrelated []*treenode) (*treenode, bool) {
	queue := []*treenode{root}
	queued := map[*treenode]struct{}{
		root: {},
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur.key == k && cur.value == v {
			success := func() bool {
				for i := range unrelated {
					if related(cur, unrelated[i]) {
						return false
					}
				}
				return true
			}()
			if success {
				return cur, true
			}
		}
		for _, toq := range happenedafter(cur) {
			if _, seen := queued[toq]; seen {
				continue
			}
			queue = append(queue, toq)
			queued[toq] = struct{}{}
		}
	}
	// No match.
	return root, false

}

func related(a, b *treenode) bool {
	queue := []*treenode{a}
	queued := map[*treenode]struct{}{
		a: {},
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == b {
			return true
		}
		for _, toq := range cur.before {
			if _, seen := queued[toq]; seen {
				continue
			}
			queue = append(queue, toq)
			queued[toq] = struct{}{}
		}
		for _, toq := range cur.after {
			if _, seen := queued[toq]; seen {
				continue
			}
			queue = append(queue, toq)
			queued[toq] = struct{}{}
		}
	}
	// No relation.
	return false
}

func happenedafter(t *treenode) []*treenode {
	return t.after
}

func happenedbefore(t *treenode) []*treenode {
	return t.before
}
