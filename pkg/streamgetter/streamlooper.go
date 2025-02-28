package streamgetter

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"time"

	//"gitlab.com/nowtilus/eventinjector/pkg/go-mpd"
	"github.com/rs/zerolog"
	"github.com/unki2aut/go-mpd"
)

// Data about our stream. Hardcoded from testing, must be dynamic
const (
	segmentSize         = 384 * time.Millisecond // 3.84s
	timeShiftWindowSize = 1 * time.Minute        //
)

// HistoryElement is metadata about a stored Manifest
type HistoryElement struct {
	At   time.Time
	Name string
}

type StreamLooper struct {
	dumpdir     string
	manifestDir string

	logger  zerolog.Logger
	history []HistoryElement
	start   time.Time

	// statistics
}

func NewStreamLooper(dumpdir string, logger zerolog.Logger) (*StreamLooper, error) {

	st := &StreamLooper{
		dumpdir:     dumpdir,
		manifestDir: path.Join(dumpdir, ManifestPath),
		logger:      logger,
		history:     make([]HistoryElement, 0, 1000),
		start:       time.Now(),
	}
	st.fillData()
	if len(st.history) < 10 {
		return nil, fmt.Errorf("Not enough manifests")
	}
	st.ShowStats()
	return st, nil
}

// fillData reads add the manifests
// Todo: detect gaps
func (sc *StreamLooper) fillData() error {
	files, err := os.ReadDir(sc.manifestDir)
	if err != nil {
		sc.logger.Error().Err(err).Msg("Scan directories")
		return err
	}
	var lasttime time.Time
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		sc.logger.Trace().Msg(f.Name())
		ctime, err := time.Parse(ManifestFormat, f.Name())
		if err != nil {
			sc.logger.Warn().Err(err).Msg("Parse String")
			continue
		}
		if !lasttime.IsZero() && (ctime.Sub(lasttime) > time.Second*30) {
			sc.logger.Error().Msgf("Too large a gap between %s and %s, dropping",
				lasttime.Format(time.TimeOnly), ctime.Format(time.TimeOnly))
			sc.history = sc.history[:0]
		}

		sc.history = append(sc.history, HistoryElement{At: ctime, Name: f.Name()})
	}
	return nil
}

// findSub finds a HistoryElement recursively
func findSub(hist []HistoryElement, want time.Time) *HistoryElement {

	if len(hist) == 0 {
		return nil
	}
	/*
		tf := "15:04:05.00"
		fmt.Printf("%d %s-%s-%s\n", len(hist), hist[0].at.Format(tf), hist[len(hist)/2].at.Format(tf), hist[len(hist)-1].at.Format(tf))
	*/
	if len(hist) == 1 {
		if want.Before(hist[0].At) {
			return nil
		} else {
			return &hist[0]
		}
	}
	pivot := len(hist) / 2
	if want.Before(hist[pivot].At) {
		return findSub(hist[:pivot], want)
	} else {
		return findSub(hist[pivot:], want)
	}
}

// FindHistory returns the newest element from history older than 'want'
func (sc *StreamLooper) FindHistory(want time.Time) *HistoryElement {

	ret := findSub(sc.history, want)
	if ret == nil {
		return nil
	}
	acopy := *ret
	acopy.Name = path.Join(sc.manifestDir, ret.Name)
	return &acopy
}

// Return loop metadata
// for the position at, return offset (to recording), timeshift und loop duration
func (sc *StreamLooper) getLoopMeta(at time.Time) (offset, shift, duration time.Duration, start time.Time) {

	// Data from history buffer
	// This is inexact, the last might not have all Segments downloaded
	start = sc.history[0].At
	last := sc.history[len(sc.history)-1].At
	// Round duration down to segmentSize
	duration = last.Sub(start) / segmentSize * segmentSize

	offset = at.Sub(start) % duration
	shift = at.Sub(start) / duration * duration
	//sc.logger.Info().Msgf("%s %s", at, start)
	return
}

// AdjustMpd adds a time offset to each Period in the Manifest
func (sc *StreamLooper) AdjustMpd(mpde *mpd.MPD, shift time.Duration) {
	if len(mpde.Period) == 0 {
		return
	}
	for _, period := range mpde.Period {
		/*
			Delete?
			if len(period.BaseURL) > 0 {
				period.BaseURL[0].Value = sc.rewriteBaseUrl(period.BaseURL[0].Value, sc.sourceUrl)
			}
		*/

		// Shift periods
		if period.Start != nil {
			startmed, _ := (*period.Start).ToNanoseconds()
			start := time.Duration(startmed)
			*period.Start = DurationToXsdDuration(start + shift)
		}
	}
	return
}

// Find a manifest at time 'at'
func (sc *StreamLooper) loadHistoricMpd(at time.Time) (*mpd.MPD, error) {

	sourceElement := sc.FindHistory(at)

	if sourceElement == nil {
		return nil, errors.New("No source found")
	}
	buf, err := os.ReadFile(sourceElement.Name)
	if err != nil {
		return nil, err
	}
	mpde := new(mpd.MPD)
	if err := mpde.Decode(buf); err != nil {
		return nil, err
	}
	return mpde, nil
}

// RoundToS Rounds a duration to full seconds
func RoundToS(in time.Duration) time.Duration {
	return RoundTo(in, time.Second)
}

// filterMpd drops every segment reference outside from-to timeframe from the mpd
func (sc *StreamLooper) filterMpd(mpde *mpd.MPD, from, to time.Time) *mpd.MPD {
	var ast time.Time
	if mpde.AvailabilityStartTime != nil {
		ast = time.Time(*mpde.AvailabilityStartTime)
	}
	sc.logger.Info().Msgf("Filter:%s:%s", from.Format(time.TimeOnly), to.Format(time.TimeOnly))
	for periodIdx, period := range mpde.Period {
		if len(period.AdaptationSets) == 0 {
			continue
		}
		// Calculate period start
		var start time.Duration
		if period.Start != nil {
			startmed, _ := (*period.Start).ToNanoseconds()
			start = time.Duration(startmed)
		}
		periodStart := ast.Add(start)
		if period.AdaptationSets != nil {
			// Could be under Representation
			for asidx, as := range period.AdaptationSets {
				if as.SegmentTemplate == nil || as.SegmentTemplate.SegmentTimeline == nil {
					continue
				}
				from, to := sumSegmentTemplate(as.SegmentTemplate, periodStart)
				sc.logger.Info().Msgf("AS1 %d: %s-%s", asidx, from.Format(time.TimeOnly), to.Format(time.TimeOnly))
				filterSegmentTemplate(
					as.SegmentTemplate,
					periodStart,
					func(t time.Time, d time.Duration) bool {
						//sc.logger.Info().Msgf("Seg:%s:%s", t, d)
						//sc.logger.Info().Msgf("Ragnge%s:%s", from, to)
						return t.After(from) && t.Add(d).Before(to)
					})
				from, to = sumSegmentTemplate(as.SegmentTemplate, periodStart)
				sc.logger.Info().Msgf("AS2 %d: %s-%s", asidx, from.Format(time.TimeOnly), to.Format(time.TimeOnly))
			}
		}

		sc.logger.Info().Msgf("Period %d start: %s", periodIdx, periodStart)

	}
	return mpde
}

// mergeMpd appends the periods from mpd2 into mpd1,
func (sc *StreamLooper) mergeMpd(mpd1, mpd2 *mpd.MPD) *mpd.MPD {
	mpd1.Period = append(mpd1.Period, mpd2.Period...)
	return mpd1
}

// GetLooped generates a Manifest by finding the manifest before now%duration
func (sc *StreamLooper) GetLooped(at time.Time) ([]byte, error) {

	offset, shift, duration, start := sc.getLoopMeta(at)
	sc.logger.Info().Msgf("Offset: %s TimeShift: %s LoopDuration: %s LoopStart:%s At %s",
		RoundToS(offset), RoundToS(shift), RoundToS(duration), start.Format(time.TimeOnly), at.Format(time.TimeOnly))
	mpde, err := sc.loadHistoricMpd(start.Add(offset))
	if err != nil {
		return []byte{}, err
	}
	sc.AdjustMpd(mpde, shift) // Manipulate

	if offset < timeShiftWindowSize {
		sc.logger.Info().Msg("At loop point")
		mpdp, _ := sc.loadHistoricMpd(start.Add(duration))
		if duration < timeShiftWindowSize {
			sc.logger.Error().Msg("unhandled case")
		} else {
			sc.AdjustMpd(mpdp, shift-duration) // Manipulate
			loopPoint := start.Add(shift)
			sc.logger.Info().Msgf("Looppoint: %s", loopPoint.Format(time.TimeOnly))
			mpdp = sc.filterMpd(mpdp, at.Add(offset-timeShiftWindowSize), loopPoint)
			mpde = sc.filterMpd(mpde, loopPoint, at)
		}

		mpde = sc.mergeMpd(mpdp, mpde)
		// Get mpd from the end of the period
		// Shift one period less
		// cut from time-shift-window to seam
		// and seam to live-edge
		// shrink
		// combine
	}

	// re-encode
	afterEncode, err := mpde.Encode()
	if err != nil {
		return nil, err
	}
	return afterEncode, nil
}

// Handler serves manifests
func (sc *StreamLooper) Handler(w http.ResponseWriter, r *http.Request) {
	/*
		loopstart, _ := time.Parse(time.RFC3339, "2025-02-27T09:48:00Z")
		startat= loopstart.Add(time.Now().Sub(sc.start))

	*/
	startat := time.Now()

	// Parse time from query Args

	ts := r.URL.Query()["to"]
	if len(ts) > 0 {
		t, err := strconv.Atoi(ts[0])
		if err != nil {
			sc.logger.Warn().Err(err).Msg("Parse time")
		} else if t < 1e9 && t > 1e9 {
			sc.logger.Warn().Msg("Implausable time offset, ignoring")
		} else {
			startat = startat.Add(-time.Duration(t) * time.Second)
		}
	}

	buf, err := sc.GetLooped(startat)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Add("Content-Type", "application/dash+xml")
	w.Write(buf)
}

// FileHanlder serves data
func (sc *StreamLooper) FileHandler(w http.ResponseWriter, r *http.Request) {
	//urlpath := strings.TrimPrefix(r.URL.Path, "/dash/")
	filepath := path.Join(sc.dumpdir, r.URL.Path)
	sc.logger.Trace().Str("path", filepath).Msg("Access")
	http.ServeFile(w, r, filepath)
}

func (sc *StreamLooper) ShowStats() {
	if len(sc.history) > 0 {
		first := sc.history[0].At
		last := sc.history[len(sc.history)-1].At
		sc.logger.Info().Msgf("Recorded %d manifests from %s to %s (%s)",
			len(sc.history),
			first.Format(time.TimeOnly),
			last.Format(time.TimeOnly),
			last.Sub(first),
		)
	}

}

// rewriteBaseUrl will return a URL concatenating upstream with base
func (sc *StreamLooper) rewriteBaseUrl(base string, upstream *url.URL) string {
	// Check for relative URL
	ur, e := url.Parse(base)
	if e != nil {
		sc.logger.Warn().Err(e).Msg("URL parsing")
		return base
	}
	if ur.IsAbs() {
		return base
	}
	return upstream.ResolveReference(ur).String()
}
