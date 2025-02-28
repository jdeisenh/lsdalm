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
	segmentSize         = 3840 * time.Millisecond // 3.84s
	timeShiftWindowSize = 25 * time.Second        //
	LoopPointOffset     = 8 * time.Second
)

// HistoryElement is metadata about a stored Manifest
type HistoryElement struct {
	At   time.Time
	Name string
}

type StreamLooper struct {
	dumpdir     string
	manifestDir string

	logger                   zerolog.Logger
	history                  []HistoryElement
	historyStart, historyEnd time.Time

	// statistics
}

func shortT(in time.Time) string {
	return in.Format(time.TimeOnly)
}

func NewStreamLooper(dumpdir string, logger zerolog.Logger) (*StreamLooper, error) {

	st := &StreamLooper{
		dumpdir:     dumpdir,
		manifestDir: path.Join(dumpdir, ManifestPath),
		logger:      logger,
		history:     make([]HistoryElement, 0, 1000),
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
	for fi, f := range files {
		if fi > 1000 {
			// Debug limit
			break
		}
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
	// Find last pts in both first and last manifest
	first, err := sc.loadHistoricMpd(sc.history[0].At)
	if err != nil {
		sc.logger.Warn().Err(err).Msg("Cannot load")
	}
	_, from, _ := sc.getPtsRange(first, "video/mp4")
	last, err := sc.loadHistoricMpd(sc.history[len(sc.history)-1].At)
	if err != nil {
		sc.logger.Warn().Err(err).Msg("Cannot load")
	}
	_, to, _ := sc.getPtsRange(last, "video/mp4")

	sc.logger.Warn().Msgf("From %s to %s", shortT(from), shortT(to))
	sc.historyStart = from
	sc.historyEnd = to

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

func (sc *StreamLooper) getRecordingRange() (from, to time.Time) {
	from = sc.history[0].At
	to = sc.history[len(sc.history)-1].At
	return
}

// Return loop metadata
// for the position at, return offset (to recording), timeshift und loop duration
func (sc *StreamLooper) getLoopMeta(at, now time.Time) (offset, shift, duration time.Duration, start time.Time) {

	// Calculate the offset in the recording buffer and the timeshift (added to timestamps)
	// Invariants:
	// start+offset+shift=now => shift=now-offset-start
	// We play at at%duration => (start+offset)%duration == at%duration => offset=(at-start)%duration

	// Data from history buffer
	// This is inexact, the last might not have all Segments downloaded
	start, last := sc.getRecordingRange()
	// Round duration down to segmentSize
	duration = last.Sub(start) / segmentSize * segmentSize

	offset = at.Sub(start) % duration
	shift = now.Add(-offset).Sub(start)
	//sc.logger.Info().Msgf("%s %s", at, start)
	return
}

// AdjustMpd adds a time offset to each Period in the Manifest, shifting the PresentationTime
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
	retained := 0
	//sc.logger.Info().Msgf("Filter:%s-%s", shortT(from.UTC()), shortT(to.UTC()))
	for _, period := range mpde.Period {
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
			first := true
			for asidx, as := range period.AdaptationSets {
				if as.SegmentTemplate == nil || as.SegmentTemplate.SegmentTimeline == nil {
					continue
				}
				buf_from, buf_to := sumSegmentTemplate(as.SegmentTemplate, periodStart)
				if first {
					sc.logger.Info().Msgf("Timestamps before %d: %s-%s", asidx, shortT(buf_from), shortT(buf_to))
				}
				// Filter the SegmentTimeline for timestamps
				total, filtered := filterSegmentTemplate(
					as.SegmentTemplate,
					periodStart,
					func(t time.Time, d time.Duration) bool {
						r := t.After(from) && t.Add(d).Before(to)
						//sc.logger.Info().Msgf("Seg %s %s %v", shortT(t), Round(d), r)
						return r
					})
				buf_from, buf_to = sumSegmentTemplate(as.SegmentTemplate, periodStart)
				if first {
					//sc.logger.Info().Msgf("Total %d Filtered %d", total, filtered)
					sc.logger.Info().Msgf("After %d: %s-%s", asidx, shortT(buf_from), shortT(buf_to))
				}
				first = false
				retained += total - filtered
			}
		}

		//sc.logger.Info().Msgf("Period %d start: %s", periodIdx, periodStart)

	}
	if retained == 0 {
		return nil
	}
	return mpde
}

// mergeMpd appends the periods from mpd2 into mpd1,
func (sc *StreamLooper) mergeMpd(mpd1, mpd2 *mpd.MPD) *mpd.MPD {
	mpd1.Period = append(mpd1.Period, mpd2.Period...)
	return mpd1
}

// GetLooped generates a Manifest by finding the manifest before now%duration
func (sc *StreamLooper) GetLooped(at, now time.Time) ([]byte, error) {

	offset, shift, duration, startOfRecording := sc.getLoopMeta(at, now)
	sc.logger.Info().Msgf("Offset: %s TimeShift: %s LoopDuration: %s LoopStart:%s At %s",
		RoundToS(offset), RoundToS(shift), RoundToS(duration), shortT(startOfRecording), shortT(at))
	mpdCurrent, err := sc.loadHistoricMpd(startOfRecording.Add(offset))
	if err != nil {
		return []byte{}, err
	}
	sc.AdjustMpd(mpdCurrent, shift) // Manipulate

	reframePeriods(mpdCurrent, fmt.Sprintf("Id-%d", shift/duration), startOfRecording.Add(shift))
	//sc.logger.Info().Msgf("Move period: %s", startOfRecording.Add(shift))

	if offset < timeShiftWindowSize {
		// We are just over the edge. We need an older manifest to add to our window
		// Load the last segment
		mpdPrevious, _ := sc.loadHistoricMpd(sc.history[len(sc.history)-1].At) //startOfRecording.Add(duration))
		if duration < timeShiftWindowSize {
			sc.logger.Error().Msg("unhandled case")
		} else {
			sc.AdjustMpd(mpdPrevious, shift-duration) // add Below
			loopPoint := startOfRecording.Add(shift).Add(-LoopPointOffset)
			sc.logger.Info().Msgf("Loop-point: %s", shortT(loopPoint))

			// Cut segments to become non-overlapping
			mpdPrevious = sc.filterMpd(mpdPrevious, now.Add(-timeShiftWindowSize).Add(-LoopPointOffset), loopPoint)
			mpdCurrent = sc.filterMpd(mpdCurrent, loopPoint, now)
			// Shift
			if mpdPrevious != nil {
				reframePeriods(mpdPrevious, fmt.Sprintf("Id-%d", shift/duration-1), startOfRecording.Add(shift).Add(-duration))
			}
		}
		if mpdPrevious != nil && mpdCurrent != nil {
			mpdCurrent = sc.mergeMpd(mpdPrevious, mpdCurrent)
		} else if mpdCurrent == nil {
			mpdCurrent = mpdPrevious
		} else {
			sc.logger.Info().Msg("Drop period")
		}
		// Get mpd from the end of the period
		// Shift one period less
		// cut from time-shift-window to seam
		// and seam to live-edge
		// shrink
		// combine
	}

	// re-encode
	afterEncode, err := mpdCurrent.Encode()
	if err != nil {
		return nil, err
	}
	return afterEncode, nil
}

// Iterate through all periods, representation, segmentTimeline and
func (sc *StreamLooper) getPtsRange(mpde *mpd.MPD, mimetype string) (time.Time, time.Time, error) {

	if len(mpde.Period) == 0 {
		return time.Time{}, time.Time{}, errors.New("No periods")
	}
	var ast time.Time
	if mpde.AvailabilityStartTime != nil {
		ast = time.Time(*mpde.AvailabilityStartTime)
	}

	var earliest, latest time.Time

	for _, period := range mpde.Period {
		for _, as := range period.AdaptationSets {

			// If there is no segmentTimeline, skip it
			if as.SegmentTemplate == nil || as.SegmentTemplate.SegmentTimeline == nil || len(as.SegmentTemplate.SegmentTimeline.S) == 0 {
				continue
			}
			if as.MimeType != mimetype {
				continue
			}

			// Calculate period start
			var start time.Duration
			if period.Start != nil {
				startmed, _ := (*period.Start).ToNanoseconds()
				start = time.Duration(startmed)
			}
			periodStart := ast.Add(start)
			from, to := sumSegmentTemplate(as.SegmentTemplate, periodStart)
			if from.IsZero() {
				for _, pres := range as.Representations {
					if pres.SegmentTemplate != nil {
						from, to = sumSegmentTemplate(pres.SegmentTemplate, periodStart)
						break
					}
				}
			}
			if earliest.IsZero() || from.Before(earliest) {
				earliest = from
			}
			if to.After(latest) {
				latest = to
			}
		}
	}
	return earliest, latest, nil
}

// Handler serves manifests
func (sc *StreamLooper) Handler(w http.ResponseWriter, r *http.Request) {
	/*
		loopstart, _ := time.Parse(time.RFC3339, "2025-02-27T09:48:00Z")
		startat= loopstart.Add(time.Now().Sub(sc.start))

	*/
	now := time.Now()
	startat := now

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

	buf, err := sc.GetLooped(startat, now)
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
