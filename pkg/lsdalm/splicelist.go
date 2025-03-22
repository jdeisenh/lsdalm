package lsdalm

import (
	"time"
)

type SpliceEvent struct {
	At time.Time
	Id string
}

type SpliceList []SpliceEvent

const ExpirationTimeout = time.Minute

// AddIfNew adds an element to the splicelist, if not already there
// Todo: Enforce they are sorted
func (sl *SpliceList) AddIfNew(newone time.Time, id string) bool {
	for _, e := range *sl {
		if e.At == newone {
			return false
		}
	}
	*sl = append(*sl, SpliceEvent{newone, id})
	return true
}

// Expire deletes events older than fixed timeout
func (sl *SpliceList) Expire() {
	expiration := time.Now().Add(-ExpirationTimeout)
	nl := make([]SpliceEvent, 0, 5)
	for _, e := range *sl {
		if e.At.Before(expiration) {
			nl = append(nl, e)
		}
	}
	*sl = nl
}

// InRange returns the first splice in the given range
func (sl *SpliceList) FirstInRange(from, to time.Time) time.Time {
	for _, e := range *sl {
		if !e.At.Before(from) && !e.At.After(to) {
			return e.At
		}
	}
	return time.Time{}
}

// InRange iterates over the elements in range
func (sl *SpliceList) InRange(from, to time.Time) func(func(int, SpliceEvent) bool) {
	return func(yield func(int, SpliceEvent) bool) {
		for i, e := range *sl {
			if !e.At.Before(from) && !e.At.After(to) {
				if !yield(i, e) {
					return
				}
			}
		}
	}
}
