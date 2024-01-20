package server

import "maps"

type VectorClock map[string]int

func (cc *VectorClock) Mark(node string) {
	if *cc == nil {
		*cc = make(VectorClock)
	}
	(*cc)[node] = (*cc)[node] + 1
}

func (left VectorClock) AtMost(right VectorClock) bool {
	for srv, ctr := range left {
		if right[srv] < ctr {
			return false
		}
	}
	return true
}

// TakeMax copies a and raises all its items less than b to b.
func (a VectorClock) TakeMax(b VectorClock) VectorClock {
	next := maps.Clone(a)
	for node, ctr := range b {
		if ctr > next[node] {
			next[node] = ctr
		}
	}
	return next
}

// Behind returns true if us is behind them in any way.
// The clocks may be unrelated or equal and Behind would still be true.
func (us VectorClock) Behind(them VectorClock) bool {
	for key := range them {
		if us[key] < them[key] {
			return true
		}
	}
	return false
}

// Before returns true if us happens-before them.
func (us VectorClock) Before(them VectorClock) bool {
	// Must be true that us <= them and for one index, us < them.
	strict := false
	for key := range zipkeys(us, them) {
		if !(us[key] <= them[key]) {
			return false
		}
		if us[key] < them[key] {
			strict = true
		}
	}
	return strict
}

// After returns true if us happens-after them.
func (us VectorClock) After(them VectorClock) bool {
	// Must be true that us >= them and for one index, us > them.
	strict := false
	for key := range zipkeys(us, them) {
		if !(us[key] >= them[key]) {
			return false
		}
		if us[key] > them[key] {
			strict = true
		}
	}
	return strict
}

// Concurrent returns true if there is no causal relationship between us and
// them.
func (us VectorClock) Concurrent(them VectorClock) bool {
	return !us.Before(them) && !us.After(them)
}

func zipkeys(a, b VectorClock) map[string]struct{} {
	var result map[string]struct{}
	for key := range a {
		result[key] = struct{}{}
	}
	for key := range b {
		result[key] = struct{}{}
	}
	return result
}
