package streamgetter

import (
	"time"

	"github.com/jdeisenh/lsdalm/pkg/go-xsd-types"
)

// TLP2Duration converts timestamp with timescale to time.Duration
func TLP2Duration(pts int64, timescale uint64) time.Duration {
	secs := pts / int64(timescale)
	nsecs := (pts % int64(timescale)) * 1000000000 / int64(timescale)
	return time.Duration(secs)*time.Second + time.Duration(nsecs)*time.Nanosecond
}

func Duration2TLP(duration time.Duration, timescale uint64) int64 {
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

	return int64(secs)*int64(timescale) + int64(nsecs)*int64(timescale)/int64(time.Second)
}

// Round duration to 100s of seconds
func RoundTo(in time.Duration, to time.Duration) time.Duration {
	return in / to * to
}

// Round turation to 10 milliseconds
func Round(in time.Duration) time.Duration {
	return RoundTo(in, time.Millisecond*10)
}

// RoundToS Rounds a duration to full seconds
func RoundToS(in time.Duration) time.Duration {
	return RoundTo(in, time.Second)
}

func DurationToXsdDuration(duration time.Duration) xsd.Duration {

	return xsd.Duration{
		Hours:       int64(duration / time.Hour),
		Minutes:     int64(duration / time.Minute % 60),
		Seconds:     int64(duration / time.Second % 60),
		Nanoseconds: int64(duration / time.Nanosecond % 1000000000),
	}

}

// shortT returns a short string representation of the time
func shortT(in time.Time) string {
	return in.UTC().Format("15:04:05.00")
}
