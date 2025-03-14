package lsdalm

import (
	"errors"
	"fmt"
	"github.com/jdeisenh/lsdalm/pkg/go-mpd"
	"github.com/rs/zerolog"
	"os"
	"path"
	"time"
)

type Recording struct {
	manifestDir string
	// Map timestamps to mpd files
	history []HistoryElement

	originalMpd *mpd.MPD

	// All the samples we have
	Segments []*AdaptationSet

	// All EventStreams
	EventStreamMap map[string]*mpd.EventStream
}

// HistoryElement is metadata about a stored Manifest
type HistoryElement struct {
	At       time.Time
	Filename string
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

func NewRecording(manifestDir string) *Recording {

	return &Recording{
		manifestDir:    manifestDir,
		history:        make([]HistoryElement, 0, 1000),
		Segments:       make([]*AdaptationSet, 0, 5),
		EventStreamMap: make(map[string]*mpd.EventStream),
	}
}

// fillData reads and adds stored manifests
func (re *Recording) fillData(logger zerolog.Logger) error {
	files, err := os.ReadDir(re.manifestDir)
	if err != nil {
		logger.Error().Err(err).Msg("Scan directories")
		return err
	}
	var lasttime time.Time
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		logger.Trace().Msg(f.Name())
		ctime, err := time.Parse(ManifestFormat, f.Name())
		if err != nil {
			logger.Warn().Err(err).Msg("Parse String")
			continue
		}
		if !lasttime.IsZero() && (ctime.Sub(lasttime) > maxMpdGap) {
			logger.Error().Msgf("Too large a gap between %s and %s, dropping",
				lasttime.Format(time.TimeOnly), ctime.Format(time.TimeOnly))
			re.history = re.history[:0]
		}
		newOne := HistoryElement{At: ctime, Filename: f.Name()}
		re.history = append(re.history, newOne)
		got, err := re.loadHistoricMpd(newOne.At)
		if err != nil {
			logger.Error().Err(err).Msg("Load manifest")
		}
		err = re.AddMpdToHistory(got)
		if err != nil {
			logger.Error().Err(err).Str("path", f.Name()).Msg("Add manifest")
			break
		}
	}

	for k, as := range re.Segments {
		ras := re.originalMpd.Period[0].AdaptationSets[k]

		logger.Info().Msgf("%d: %s %d-%d Duration %d", len(as.elements), ras.MimeType, as.start, as.end, (as.end-as.start)*1000/int64(*ras.SegmentTemplate.Timescale))
	}

	for sId, elem := range re.EventStreamMap {
		logger.Info().Msgf("Events: %s: %+v", sId, len(elem.Event))
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

func (re *Recording) AddMpdToHistory(mpde *mpd.MPD) error {

	if len(mpde.Period) == 0 {
		return errors.New("No periods")
	}
	if len(mpde.Period) > 1 {
		return errors.New("Multiperiod not supported")
	}
	for _, p := range mpde.Period {
		for asi, as := range p.AdaptationSets {
			var tas *AdaptationSet
			if asi >= len(re.Segments) {
				re.Segments = append(re.Segments, NewAdaptationSet())
			}
			tas = re.Segments[asi]
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
			have, ok := re.EventStreamMap[sId]
			if !ok {
				ev.Event = ev.Event[:0]
				re.EventStreamMap[sId] = ev
				continue
			}
		inloop:
			// Range events
			for _, in := range ev.Event {
				// Compare to the ones already there
				for _, x := range have.Event {
					if x.Id == in.Id && ZeroIfNil(x.PresentationTime) == ZeroIfNil(in.PresentationTime) {
						//logger.Trace().Msgf("Found: %d@%d", in.Id, in.PresentationTime)
						continue inloop
					}
				}
				//logger.Info().Msgf("Add Events %s: %d@%d", sId, in.Id, in.PresentationTime)
				// Append Events if not there
				in.Content = "" // Clean up cruft
				have.Event = append(have.Event, in)
			}

		}
	}
	if re.originalMpd == nil {
		re.originalMpd = mpde
	}
	return nil

}

// FindHistory returns the newest element from history older than 'want'
func (re *Recording) FindHistory(want time.Time) *HistoryElement {

	ret := findSub(re.history, want)
	if ret == nil {
		return nil
	}
	acopy := *ret
	acopy.Filename = path.Join(re.manifestDir, ret.Filename)
	return &acopy
}

func (re *Recording) getTimelineRange() (from, to time.Time) {
	if re.originalMpd == nil || len(re.originalMpd.Period) == 0 {
		return
	}
	var ast time.Time
	if re.originalMpd.AvailabilityStartTime != nil {
		ast = time.Time(*re.originalMpd.AvailabilityStartTime)
	}
	// First period only
	period := re.originalMpd.Period[0]
	for asi, as := range period.AdaptationSets {
		asf := re.Segments[asi]
		timescale := ZeroIfNil(as.SegmentTemplate.Timescale)
		ls := ast.Add(TLP2Duration(asf.start, timescale))
		le := ast.Add(TLP2Duration(asf.end, timescale))
		if from.IsZero() || from.After(ls) {
			from = ls
		}
		if to.IsZero() || to.Before(le) {
			to = le
		}
	}
	return
}

// getRecordingRange gets first and last *sample* time for the manifests
func (re *Recording) getRecordingRange() (from, to time.Time) {
	from = re.history[0].At
	to = re.history[len(re.history)-1].At
	return
}

// Return loop metadata
// for the position at, return offset (to recording), timeshift und loop duration
func (re *Recording) getLoopMeta(at, now time.Time, requestDuration time.Duration) (offset, shift, duration time.Duration, start time.Time) {

	// Calculate the offset in the recording buffer and the timeshift (added to timestamps)
	// Invariants:
	// start+offset+shift=now => shift=now-offset-start
	// We play at at%duration => (start+offset)%duration == at%duration => offset=(at-start)%duration

	// Data from history buffer
	// This is inexact, the last might not have all Segments downloaded
	var end time.Time
	start, end = re.getRecordingRange()
	duration = end.Sub(start)

	offset = at.Sub(start) % duration
	shift = now.Add(-offset).Sub(start)
	//sc.logger.Info().Msgf("%s %s", at, start)
	return
}

// Find a manifest at time 'at'
func (re *Recording) loadHistoricMpd(at time.Time) (*mpd.MPD, error) {

	sourceElement := re.FindHistory(at)

	if sourceElement == nil {
		return nil, errors.New("No source found")
	}
	buf, err := os.ReadFile(sourceElement.Filename)
	if err != nil {
		return nil, err
	}
	mpde := new(mpd.MPD)
	if err := mpde.Decode(buf); err != nil {
		return nil, err
	}
	return mpde, nil
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
		return fmt.Errorf("%w on gap of %d appending to %d", noncont, t-as.end, as.end)
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

func (re *Recording) ShowStats(logger zerolog.Logger) {
	if len(re.history) > 0 {
		first := re.history[0].At
		last := re.history[len(re.history)-1].At
		logger.Info().Msgf("Recorded %d manifests from %s to %s (%s)",
			len(re.history),
			first.Format(time.TimeOnly),
			last.Format(time.TimeOnly),
			last.Sub(first),
		)
	}

}
