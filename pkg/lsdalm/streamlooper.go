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
	timeShiftWindowSize = 25 * time.Second        // timeshift buffer size. Should be taken from manifest or from samples
	LoopPointOffset     = 10 * time.Second        // move splicepoint back in time to be outside the live Delay
	maxMpdGap           = 30 * time.Second        // maximum gap between mpd updates
	segmentSize         = 1920 * time.Millisecond // must be got from stream
)

var noncont = errors.New("Not in sequence")

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

type StreamLooper struct {
	dumpdir     string
	manifestDir string

	logger zerolog.Logger

	// Map timestamps to mpd files
	history []HistoryElement

	originalMpd *mpd.MPD

	// All the samples we have
	Segments []*AdaptationSet

	// All EventStreams
	EventStreamMap map[string]*mpd.EventStream

	// statistics
}

func NewStreamLooper(dumpdir string, logger zerolog.Logger) (*StreamLooper, error) {

	st := &StreamLooper{
		dumpdir:        dumpdir,
		manifestDir:    path.Join(dumpdir, ManifestPath),
		logger:         logger,
		history:        make([]HistoryElement, 0, 1000),
		Segments:       make([]*AdaptationSet, 0, 5),
		EventStreamMap: make(map[string]*mpd.EventStream),
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

	for sId, elem := range sc.EventStreamMap {
		sc.logger.Info().Msgf("Events: %s: %+v", sId, len(elem.Event))
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
		// Add events
		for _, ev := range p.EventStream {
			if ev.SchemeIdUri == nil {
				continue
			}
			sId := *ev.SchemeIdUri
			// Look up by scheme
			have, ok := sc.EventStreamMap[sId]
			if !ok {
				ev.Event = ev.Event[:0]
				sc.EventStreamMap[sId] = ev
				continue
			}
		inloop:
			// Range events
			for _, in := range ev.Event {
				// Compare to the ones already there
				for _, x := range have.Event {
					if x.Id == in.Id && ZeroIfNil(x.PresentationTime) == ZeroIfNil(in.PresentationTime) {
						sc.logger.Trace().Msgf("Found: %d@%d", in.Id, in.PresentationTime)
						continue inloop
					}
				}
				sc.logger.Info().Msgf("Add Events %s: %d@%d", sId, in.Id, in.PresentationTime)
				// Append Events if not there
				in.Content = "" // Clean up cruft
				have.Event = append(have.Event, in)
			}

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
	var end time.Time
	start, end = sc.getRecordingRange()
	duration = end.Sub(start) / segmentSize * segmentSize //sc.historyEnd.Sub(sc.historyStart)

	offset = at.Sub(start) % duration
	shift = now.Add(-offset).Sub(start)
	//sc.logger.Info().Msgf("%s %s", at, start)
	return
}

// AdjustMpd adds 'shift' to the 'start' attribute of each Period, shifting the PresentationTime
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
		pto := ZeroIfNil(nst.PresentationTimeOffset)
		first := true
		for _, s := range elements.elements {
			for ri := int64(0); ri <= s.r; ri++ {
				ts := newstart.Add(TLP2Duration(int64(uint64(start)-pto), timescale))
				d := TLP2Duration(s.d, timescale)
				if !ts.Add(d).Before(from) && ts.Add(d).Before(to) {
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
		if len(nstl.S) == 0 {
			nst.SegmentTimeline = nil
		}
		np.AdaptationSets = append(np.AdaptationSets, nas)

	}

	// Add Events
	np.EventStream = np.EventStream[:0]
	for _, ev := range sc.EventStreamMap {
		// Append all for all ranges: Todo: map offset, duration
		evs := new(mpd.EventStream)
		*evs = *ev
		pto := uint64(ZeroIfNil(ev.PresentationTimeOffset))
		timescale := ZeroIfNil(ev.Timescale)
		pto = uint64(int64(pto) + Duration2TLP(shiftValue, timescale))
		evs.PresentationTimeOffset = &pto
		fel := evs.Event[:0]
		for _, e := range evs.Event {
			ts := newstart.Add(TLP2Duration(int64(*e.PresentationTime-pto), timescale))
			d := TLP2Duration(int64(ZeroIfNil(e.Duration)), timescale)
			// Still in the future
			if ts.After(to) {
				sc.logger.Debug().Msgf("Skip Event %s %d at %s in the future of %s", EmptyIfNil(evs.SchemeIdUri), e.Id, shortT(ts), shortT(to))
				continue
			}
			// End of event in the past end > to
			if from.After(ts.Add(d)) {
				sc.logger.Debug().Msgf("Skip Event %s %d at ends %s before %s", EmptyIfNil(evs.SchemeIdUri), e.Id, shortT(ts.Add(d)), shortT(from))
				continue
			}
			sc.logger.Debug().Msgf("Add Event %s %d at %s-%s", EmptyIfNil(evs.SchemeIdUri), e.Id, shortT(ts), shortT(ts.Add(d)))
			fel = append(fel, e)

		}
		evs.Event = fel
		if len(evs.Event) > 0 {
			np.EventStream = append(np.EventStream, evs)
		}
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
		sc.logger.Debug().Msgf("Loop point: %s", shortT(startOfRecording.Add(shift)))
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
