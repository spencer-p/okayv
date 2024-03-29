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

type CausalError struct {
	error
	Roots   map[string]*treenode
	Cursors map[string][]*treenode
}

// Validate validates a slice of ordered actions which must be Write or
// ReadResult.
// TODO: Accept delete instructions.
func ValidateCausality(actions []any) error {
	roots := map[string]*treenode{}
	cursors := map[string][]*treenode{}
	for actioni, a := range actions {
		switch v := a.(type) {
		case Write:
			// Skip errors.
			// TODO: This should not be a magic string.
			if v.Value == "error" {
				continue
			}
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
				var considered []*treenode
				for _, cursor := range cursors[v.Client] {
					// Find immediate instances of Key and see if they have Value.
					// If not found allowed to traverse disconnected trees
					candidates := searchhistory(v.Key, cursor, true /*happenedbefore*/)
					for _, c := range candidates {
						if c.value == v.Value ||
							(c.deleted && v.NotFound) {
							// Valid read in prior history.
							return nil
						}
					}
					// If we couldn't find anything in prior history and we got
					// a 404, that's OK. This may be a lagging replica.
					if v.NotFound && len(candidates) == 0 {
						return nil
					}
					considered = append(considered, candidates...)

					// Find candidates in the "future".
					candidates = searchhistory(v.Key, cursor, false /*happenedafter*/)
					for _, c := range candidates {
						if c.value == v.Value ||
							(c.deleted && v.NotFound) {
							// Valid read from the future.
							// This cursor has now advanced.
							cursors[v.Client] = append(cursors[v.Client], c)
							//cursors[v.Client][idx] = c
							return nil
						}
					}
					considered = append(considered, candidates...)
				}
				// If no cursor for the client could see a value and we got a
				// 404, then that's valid.
				if v.NotFound && len(considered) == 0 {
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
				wantedvals := []string{}
				for _, c := range considered {
					wantedvals = append(wantedvals, c.value)
				}
				return CausalError{
					error:   fmt.Errorf("%s cannot read %s=%s at index %d, wanted %v", v.Client, v.Key, v.Value, actioni, wantedvals),
					Roots:   roots,
					Cursors: cursors,
				}
			}()
			if err != nil {
				return err
			}
			// Try to drop any redundant cursors superseded by the cursor that
			// may have been added..
			if len(cursors[v.Client]) > 0 {
				var newCursors []*treenode
				lastCursor := cursors[v.Client][len(cursors[v.Client])-1]
				for i := 0; i+1 < len(cursors[v.Client]); i++ {
					if !relateddir(lastCursor, cursors[v.Client][i], happenedbefore) {
						newCursors = append(newCursors, cursors[v.Client][i])
					}
				}
				cursors[v.Client] = append(newCursors, lastCursor)
			}
		default:
			panic(fmt.Sprintf("type error: invalid action type %T", a))
		}
	}
	return nil
}

// searchhistory searches history starting from roots for any reachable matching
// keys. The paths from root to matching key never contain the key itself.
func searchhistory(key string, root *treenode, before bool) []*treenode {
	next := happenedafter
	if before {
		next = happenedbefore
	}
	matches := []*treenode{}
	queue := []*treenode{root}
	queued := map[*treenode]struct{}{
		root: {},
	}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		if cur.key == key {
			matches = append(matches, cur)
			if before { // Read: If traversing backwards.
				continue // Never traverse backwards past a valid response, as that would skip history.
			}
			// It is actually legal to traverse past a valid
			// response if we are fast-forwarding in history.
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
	return relateddir(a, b, happenedafter) || relateddir(a, b, happenedbefore)
}

func relateddir(a, b *treenode, next func(*treenode) []*treenode) bool {
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
		for _, toq := range next(cur) {
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
