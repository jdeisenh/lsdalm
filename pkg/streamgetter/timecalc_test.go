package streamgetter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTLP2Duration(t *testing.T) {
	var testdata = []struct {
		pts, timescale uint64
		expect         time.Duration
	}{
		{10000, 10000, time.Second},
		{1, 1000000000, time.Nanosecond},
		{3000000000, 1000000000, 3 * time.Second},
	}
	for _, elem := range testdata {
		assert.Equal(t, TLP2Duration(elem.pts, elem.timescale), elem.expect)
	}

}

func TestDuration2TLP(t *testing.T) {
	var testdata = []struct {
		duration          time.Duration
		timescale, expect uint64
	}{
		{time.Second, 10000, 10000},
		{time.Millisecond, 1000, 1},
		{3 * time.Second, 1000000000, 3000000000},
	}
	for _, elem := range testdata {
		assert.Equal(t, elem.expect, Duration2TLP(elem.duration, elem.timescale))
	}

}
