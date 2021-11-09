package internal

import "sync"

type IntSet struct {
	values map[uint64]bool
	lock   sync.RWMutex
}

// Size of set.
func (is *IntSet) Size() int {
	is.lock.RLock()
	defer is.lock.RUnlock()
	return len(is.values)
}

// Add element to set. Returns true if was no value before.
func (is *IntSet) Add(value uint64) bool {
	is.lock.Lock()
	defer is.lock.Unlock()
	if is.values == nil {
		is.values = map[uint64]bool{}
	}
	old := is.values[value]
	is.values[value] = true
	return !old
}

// Remove element from set. Return true if was value before.
func (is *IntSet) Remove(value uint64) bool {
	is.lock.Lock()
	defer is.lock.Unlock()
	if is.values == nil {
		return false
	}
	old := is.values[value]
	delete(is.values, value)
	return old
}
