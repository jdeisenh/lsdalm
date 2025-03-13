package lsdalm

import (
	"time"
)

type SpliceList []time.Time

// AddIfNew adds an element to the splicelist, if not already there
func (sl *SpliceList) AddIfNew(newone time.Time) bool {
	for _, e := range *sl {
		if e == newone {
			return false
		}
	}
	*sl = append(*sl, newone)
	return true
}

// Expire deletes events older than one minute
func (sl *SpliceList) Expire() {
	expiration := time.Now().Add(-time.Minute)
	nl := []time.Time(*sl)[:0]
	for _, e := range *sl {
		if e.Before(expiration) {
			nl = append(nl, e)
		}
	}
	*sl = nl
}

// InRange returns the first splice in the given range
func (sl *SpliceList) FirstInRange(from, to time.Time) time.Time {
	for _, e := range *sl {
		if !e.Before(from) && !e.After(to) {
			return e
		}
	}
	return time.Time{}
}

// InRange iterates over the elements in range
func (sl *SpliceList) InRange(from, to time.Time) func(func(int, time.Time) bool) {
	return func(yield func(int, time.Time) bool) {
		for i, e := range *sl {
			if !e.Before(from) && !e.After(to) {
				if !yield(i, e) {
					return
				}
			}
		}
	}
}
