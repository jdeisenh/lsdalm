package streamgetter

import (
	"time"

	"github.com/unki2aut/go-xsd-types"
)

// TLP2Duration converts timestamp with timescale to time.Duration
func TLP2Duration(pts uint64, timescale uint64) time.Duration {
	secs := pts / timescale
	nsecs := (pts % timescale) * 1000000000 / timescale
	return time.Duration(secs)*time.Second + time.Duration(nsecs)*time.Nanosecond
}

func Duration2TLP(duration time.Duration, timescale uint64) uint64 {
	// Basically duration/time.Second*timescale
	// Careful about overflow. timescale can be big We calculate in microseconds,
	//
	/*
		if duration > time.Hour || timescale > 10000 {
			return uint64(duration) / 1000 * timescale * 1000 / uint64(time.Second)
		}
		return uint64(duration) * timescale / uint64(time.Second)
	*/
	secs := duration / time.Second
	nsecs := duration % time.Second * time.Nanosecond

	return uint64(secs)*timescale + uint64(nsecs)*timescale/uint64(time.Second)
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
