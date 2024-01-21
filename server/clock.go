package server

import (
	"maps"
)

type VectorClock map[string]int

func (v VectorClock) Clone() VectorClock {
	return maps.Clone(v)
}

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

// TakeMax raises all its items less than b to b.
func (a *VectorClock) TakeMax(b VectorClock) {
	if *a == nil {
		*a = make(VectorClock)
	}
	for node, ctr := range b {
		if ctr > (*a)[node] {
			(*a)[node] = ctr
		}
	}
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

// AheadOne returns true if us and them are equal except for one index where
// them = us+1.
func (us VectorClock) AheadOne(them VectorClock) bool {
	foundPlusOne := false
	for key := range zipkeys(us, them) {
		diff := them[key] - us[key]
		switch {
		case diff < 0:
			return false
		case diff == 1 && foundPlusOne:
			return false // Too many plus ones.
		case diff == 1 && !foundPlusOne:
			foundPlusOne = true
		case diff > 1:
			return false
		case diff == 0:
			continue
		}
	}
	return foundPlusOne
}

// AheadOneN returns true if us and them are equal except for N indices where
// them = us+1.
func (us VectorClock) AheadOneN(them VectorClock, n int) bool {
	numPlusOnes := 0
	for key := range zipkeys(us, them) {
		diff := them[key] - us[key]
		switch {
		case diff < 0:
			return false
		case diff == 1 && numPlusOnes == n:
			return false // Too many plus ones.
		case diff == 1:
			numPlusOnes++
		case diff > 1:
			return false
		case diff == 0:
			continue
		}
	}
	return numPlusOnes == n
}

func (us VectorClock) Equal(them VectorClock) bool {
	for key := range zipkeys(us, them) {
		if us[key] != them[key] {
			return false
		}
	}
	return true
}

func zipkeys(a, b VectorClock) map[string]struct{} {
	result := make(map[string]struct{})
	for key := range a {
		result[key] = struct{}{}
	}
	for key := range b {
		result[key] = struct{}{}
	}
	return result
}
