package streamgetter

import (
	"errors"
	"os"
	"path"
	"time"

	"github.com/jdeisenh/lsdalm/pkg/go-mpd"
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
