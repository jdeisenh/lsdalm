package streamgetter

import (
	"time"

	"github.com/unki2aut/go-xsd-types"
)

// TLP2Duration Converts timestamp withtimescale to Duration
func TLP2Duration(pts uint64, timescale uint64) time.Duration {
	secs := pts / timescale
	nsecs := (pts * timescale) % 1000000000
	return time.Duration(secs)*time.Second + time.Duration(nsecs)*time.Nanosecond
}

// Round duration to 100s of seconds
func RoundTo(in time.Duration, to time.Duration) time.Duration {
	return in / to * to
}

func Round(in time.Duration) time.Duration {
	return RoundTo(in, time.Millisecond*10)
}

func DurationToXsdDuration(duration time.Duration) xsd.Duration {

	return xsd.Duration{
		Hours:       int64(duration / time.Hour),
		Minutes:     int64(duration / time.Minute % 60),
		Seconds:     int64(duration / time.Second % 60),
		Nanoseconds: int64(duration / time.Nanosecond % 1000000000),
	}

}
