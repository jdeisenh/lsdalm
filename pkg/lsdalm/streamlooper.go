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

	"github.com/jdeisenh/lsdalm/pkg/go-mpd"
	"github.com/rs/zerolog"
)

// Data about our stream. Hardcoded from testing, must be dynamic
const (
	timeShiftWindowSize = 25 * time.Second // timeshift buffer size. Should be taken from manifest or from samples
	LoopPointOffset     = 10 * time.Second // move splicepoint back in time to be outside the live Delay
	maxMpdGap           = 30 * time.Second // maximum gap between mpd updates
)

// HistoryElement is metadata about a stored Manifest
type HistoryElement struct {
	At   time.Time
	Name string
}

type Element struct {
	d, r int64
}

type AdaptationSet struct {
	elements   []Element
	start, end int64
}

func NewAdaptationSet() *AdaptationSet {
	return &AdaptationSet{
		elements: make([]Element, 0, 100),
	}
}

var noncont = errors.New("Not in sequence")

type StreamLooper struct {
	dumpdir     string
	manifestDir string

	logger                   zerolog.Logger
	history                  []HistoryElement
	historyStart, historyEnd time.Time

	originalMpd *mpd.MPD
	Segments    []*AdaptationSet

	// statistics
}

func NewStreamLooper(dumpdir string, logger zerolog.Logger) (*StreamLooper, error) {

	st := &StreamLooper{
		dumpdir:     dumpdir,
		manifestDir: path.Join(dumpdir, ManifestPath),
		logger:      logger,
		history:     make([]HistoryElement, 0, 1000),
		Segments:    make([]*AdaptationSet, 0, 5),
	}
	st.fillData()
	if len(st.history) < 10 {
		return nil, fmt.Errorf("Not enough manifests")
	}
	st.ShowStats()
	return st, nil
}

// fillData reads and adds stored manifests
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
		if !lasttime.IsZero() && (ctime.Sub(lasttime) > maxMpdGap) {
			sc.logger.Error().Msgf("Too large a gap between %s and %s, dropping",
				lasttime.Format(time.TimeOnly), ctime.Format(time.TimeOnly))
			sc.history = sc.history[:0]
		}
		newOne := HistoryElement{At: ctime, Name: f.Name()}
		sc.history = append(sc.history, newOne)
		got, err := sc.loadHistoricMpd(newOne.At)
		if err != nil {
			sc.logger.Error().Err(err).Msg("Load manifest")
		}
		err = sc.AddMpdToHistory(got)
		if err != nil {
			sc.logger.Error().Err(err).Msg("Add manifest")
		}
	}

	for k, as := range sc.Segments {
		ras := sc.originalMpd.Period[0].AdaptationSets[k]

		sc.logger.Info().Msgf("%d: %s %d-%d Duration %d", len(as.elements), ras.MimeType, as.start, as.end, (as.end-as.start)*1000/int64(*ras.SegmentTemplate.Timescale))
	}
	// Find last pts in both first and last manifest
	fs := sc.history[0].At
	first, err := sc.loadHistoricMpd(fs)
	if err != nil {
		sc.logger.Warn().Err(err).Msg("Cannot load")
	}
	ff, fl, _ := sc.getPtsRange(first, "video/mp4")

	ls := sc.history[len(sc.history)-1].At
	last, err := sc.loadHistoricMpd(ls)
	if err != nil {
		sc.logger.Warn().Err(err).Msg("Cannot load")
	}
	lf, ll, _ := sc.getPtsRange(last, "video/mp4")
	sc.logger.Debug().Msgf("Start %s %s-%s", shortT(fs), Round(fs.Sub(ff)), Round(fs.Sub(fl)))
	sc.logger.Debug().Msgf("End %s %s-%s", shortT(ls), Round(ls.Sub(lf)), Round(ls.Sub(ll)))
	sc.historyStart = fl
	sc.historyEnd = ll

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

func (sc *StreamLooper) AddMpdToHistory(mpde *mpd.MPD) error {

	if len(mpde.Period) == 0 {
		return errors.New("No periods")
	}
	if len(mpde.Period) > 1 {
		return errors.New("Multiperiod not supported")
	}
	for _, p := range mpde.Period {
		for asi, as := range p.AdaptationSets {
			var tas *AdaptationSet
			if asi >= len(sc.Segments) {
				sc.Segments = append(sc.Segments, NewAdaptationSet())
			}
			tas = sc.Segments[asi]
			if st := as.SegmentTemplate; st != nil {
				if stl := st.SegmentTimeline; stl != nil {
					for t, d := range All(stl) {
						//sc.logger.Debug().Msgf("Add %d %d", t, d)
						e := tas.Add(int64(t), int64(d), 0)
						if e != nil {
							return e
						}
					}
				}
			}
			// Todo: Template under Adaptation
		}
	}
	if sc.originalMpd == nil {
		sc.originalMpd = mpde
	}
	return nil

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
func (sc *StreamLooper) getLoopMeta(at, now time.Time, requestDuration time.Duration) (offset, shift, duration time.Duration, start time.Time) {

	// Calculate the offset in the recording buffer and the timeshift (added to timestamps)
	// Invariants:
	// start+offset+shift=now => shift=now-offset-start
	// We play at at%duration => (start+offset)%duration == at%duration => offset=(at-start)%duration

	// Data from history buffer
	// This is inexact, the last might not have all Segments downloaded
	start, _ = sc.getRecordingRange()
	duration = sc.historyEnd.Sub(sc.historyStart)

	offset = at.Sub(start) % duration
	shift = now.Add(-offset).Sub(start)
	//sc.logger.Info().Msgf("%s %s", at, start)
	return
}

// AdjustMpd adds a time offset to each Period in the Manifest, shifting the PresentationTime
func (sc *StreamLooper) AdjustMpd(mpde *mpd.MPD, shift time.Duration) *mpd.MPD {
	if len(mpde.Period) == 0 {
		return nil
	}
	outMpd := new(mpd.MPD)
	*outMpd = *mpde
	outMpd.Period = make([]*mpd.Period, 0, 1)
	for _, period := range mpde.Period {
		// Shift periods
		np := new(mpd.Period)
		*np = *period
		if period.Start != nil {
			startmed, _ := (*period.Start).ToNanoseconds()
			start := time.Duration(startmed)
			ns := DurationToXsdDuration(start + shift)
			np.Start = &ns
		}
		outMpd.Period = append(outMpd.Period, np)
	}
	return outMpd
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
			for _, as := range period.AdaptationSets {
				if as.SegmentTemplate == nil || as.SegmentTemplate.SegmentTimeline == nil {
					continue
				}
				// Filter the SegmentTimeline for timestamps
				total, filtered := filterSegmentTemplate(
					as.SegmentTemplate,
					periodStart,
					func(st time.Time, sd time.Duration) bool {
						r := !st.Before(from) && !st.Add(sd).After(to)
						//sc.logger.Info().Msgf("Seg %s %s %v", shortT(t), Round(d), r)
						return r
					})
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
	if mpd1 == nil {
		return mpd2
	} else if mpd2 == nil {
		return mpd1
	} else {
		mpd1.Period = append(mpd1.Period, mpd2.Period...)
		return mpd1
	}
}

func (sc *StreamLooper) BuildMpd(shift time.Duration, id string, newstart, from, to time.Time) *mpd.MPD {
	mpde := sc.originalMpd
	outMpd := new(mpd.MPD)
	*outMpd = *mpde // Copy mpd

	period := mpde.Period[0] // There is only one

	// OUput period
	outMpd.Period = make([]*mpd.Period, 0, 1)
	// Loop over output periods in timeShiftWindow

	if len(period.AdaptationSets) == 0 {
		return outMpd
	}

	// Copy period
	np := new(mpd.Period)
	*np = *period

	var ast time.Time
	if mpde.AvailabilityStartTime != nil {
		ast = time.Time(*mpde.AvailabilityStartTime)
	}

	// Calculate period start
	var shiftValue time.Duration
	if period.Start != nil {
		startmed, _ := (*period.Start).ToNanoseconds()
		start := time.Duration(startmed)
		shiftValue = newstart.Sub(ast) - start - shift
		ns := DurationToXsdDuration(newstart.Sub(ast))
		np.Start = &ns
	}
	if period.ID != nil {
		np.ID = &id
	}

	np.AdaptationSets = make([]*mpd.AdaptationSet, 0, 5)
	for asi, as := range period.AdaptationSets {
		if as.SegmentTemplate == nil || as.SegmentTemplate.SegmentTimeline == nil {
			continue
		}
		nas := new(mpd.AdaptationSet)
		*nas = *as
		nst := new(mpd.SegmentTemplate)
		nas.SegmentTemplate = nst
		*nst = *as.SegmentTemplate
		nstl := new(mpd.SegmentTimeline)
		nst.SegmentTimeline = nstl
		*nstl = *nst.SegmentTimeline

		nstl.S = nstl.S[:0]
		elements := sc.Segments[asi]
		shiftPto(nst, shiftValue)
		//startrt := newstart.Add(-TLP2Duration(int64(ZeroIfNil(nst.PresentationTimeOffset)), ZeroIfNil(nst.Timescale)))
		start := elements.start
		timescale := ZeroIfNil(nst.Timescale)
		first := true
		for _, s := range elements.elements {
			for ri := int64(0); ri <= s.r; ri++ {
				ts := newstart.Add(TLP2Duration(int64(uint64(start)-ZeroIfNil(nst.PresentationTimeOffset)),
					timescale))
				d := TLP2Duration(s.d, timescale)
				if !ts.Before(from) && ts.Add(d).Before(to) {
					t := uint64(0)
					if first {
						t = uint64(start)
						first = false
					}
					AppendR(nstl, t, uint64(s.d), 0)
				}
				start += s.d
			}

		}
		np.AdaptationSets = append(np.AdaptationSets, nas)

	}
	//sc.logger.Info().Msgf("Period %d start: %s", periodIdx, periodStart)
	outMpd.Period = append(outMpd.Period, np)

	return outMpd
}

// GetLooped generates a Manifest by finding the manifest before now%duration
func (sc *StreamLooper) GetLooped(at, now time.Time, requestDuration time.Duration) ([]byte, error) {

	offset, shift, duration, startOfRecording := sc.getLoopMeta(at, now, requestDuration)
	sc.logger.Info().Msgf("Offset: %s TimeShift: %s LoopDuration: %s LoopStart:%s At %s",
		RoundToS(offset), RoundToS(shift), RoundToS(duration), shortT(startOfRecording), shortT(at))

	// Check if we are around the loop point
	var mpdCurrent *mpd.MPD
	if offset < timeShiftWindowSize {
		sc.logger.Info().Msgf("Loop point: %s", shortT(startOfRecording.Add(shift)))
		mpdPrevious := sc.BuildMpd(
			shift-duration,
			fmt.Sprintf("Id-%d", shift/duration-1),
			startOfRecording.Add(shift).Add(-duration),
			now.Add(-timeShiftWindowSize),
			startOfRecording.Add(shift),
		)
		mpdCurrent = sc.BuildMpd(
			shift,
			fmt.Sprintf("Id-%d", shift/duration),
			startOfRecording.Add(shift),
			startOfRecording.Add(shift),
			now,
		)
		mpdCurrent = sc.mergeMpd(mpdPrevious, mpdCurrent)
	} else {
		mpdCurrent = sc.BuildMpd(
			shift,
			fmt.Sprintf("Id-%d", shift/duration),
			startOfRecording.Add(shift),
			now.Add(-timeShiftWindowSize),
			now,
		)
	}
	// re-encode
	afterEncode, err := mpdCurrent.Encode()
	if err != nil {
		return nil, err
	}
	return afterEncode, nil
}

// GetLooped generates a Manifest by finding the manifest before now%duration
func (sc *StreamLooper) GetPlayback(at, now time.Time, requestDuration time.Duration) ([]byte, error) {

	offset, shift, duration, startOfRecording := sc.getLoopMeta(at, now, requestDuration)
	sc.logger.Info().Msgf("Offset: %s TimeShift: %s LoopDuration: %s LoopStart:%s At %s",
		RoundToS(offset), RoundToS(shift), RoundToS(duration), shortT(startOfRecording), shortT(at))
	mpdCurrent, err := sc.loadHistoricMpd(startOfRecording.Add(offset))
	if err != nil {
		return []byte{}, err
	}

	mpdCurrent = sc.AdjustMpd(mpdCurrent, shift) // shift PresentationTime

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

	for periodId, period := range mpde.Period {
		for _, as := range period.AdaptationSets {

			// If there is no segmentTimeline, skip it
			if as.SegmentTemplate == nil || as.SegmentTemplate.SegmentTimeline == nil || len(as.SegmentTemplate.SegmentTimeline.S) == 0 {
				continue
			}
			/*
				if as.MimeType != mimetype {
					continue
				}
			*/
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
			sc.logger.Debug().Msgf("Period %d As %s From %s to %s", periodId, as.MimeType, shortT(from), shortT(to))
			if as.MimeType != mimetype {
				continue
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
	var duration time.Duration

	// Parse time from query Args

	// to timeoffset
	ts := r.URL.Query()["to"]
	if len(ts) > 0 {
		t, err := strconv.Atoi(ts[0])
		if err != nil {
			sc.logger.Warn().Err(err).Msg("Parse time")
		} else if t < 0 && t > 1e6 {
			sc.logger.Warn().Msg("Implausable time offset, ignoring")
		} else {
			startat = startat.Add(-time.Duration(t) * time.Second)
		}
	}

	// ld loop duration
	ld := r.URL.Query()["ld"]
	if len(ld) > 0 {
		t, err := strconv.Atoi(ld[0])
		if err != nil {
			sc.logger.Warn().Err(err).Msg("Parse time")
		} else if t <= 0 && t > 1e5 {
			sc.logger.Warn().Msg("Implausable duration, ignoring")
		} else {
			duration = time.Duration(t) * time.Second
		}
	}

	buf, err := sc.GetLooped(startat, now, duration)
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

// shortT returns a short string representation of the time
func shortT(in time.Time) string {
	return in.UTC().Format("15:04:05.00")
}

func (as *AdaptationSet) Add(t, d, r int64) error {

	last := len(as.elements) - 1
	if last < 0 {
		as.elements = make([]Element, 0, 1000)
		as.start, as.end = t, t
	}
	if t < as.end {
		// We ignore smaller ones, not checking if they already exist
		return nil
	}
	if t > as.end {
		return noncont
	}
	if last < 0 || as.elements[last].d != d {
		// New element
		as.elements = append(as.elements, Element{d: d, r: r})
	} else {
		n := as.elements[last]
		as.elements[last] = Element{n.d, n.r + r + 1}
	}
	as.end += d * (r + 1)
	return nil
}
