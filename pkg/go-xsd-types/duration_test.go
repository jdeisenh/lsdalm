package xsd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(t *testing.T) {

	var testdata = []struct {
		value  int64
		ret    int
		expect string
	}{
		{0, 11, "0"},
		{1, 1, "000000001"},
		{3, 1, "000000003"},
		{1231123, 1, "001231123"},
		{1231123, 1, "001231123"},
		{999999999, 1, "999999999"},
		{900000000, 9, "9"},
	}

	for _, elem := range testdata {
		var buf [10]byte = [10]byte{32, 32, 32, 32, 32, 32, 32, 32, 32, 32}
		w := len(buf)
		r := fmtNano(buf[:w], elem.value)
		assert.Equal(t, elem.ret, r)
		if r <= w {
			result := string(buf[r:])
			assert.EqualValues(t, elem.expect, result)
		}
	}
}
