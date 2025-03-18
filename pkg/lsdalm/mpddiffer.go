package lsdalm

import (
	"fmt"
	"time"

	"github.com/jdeisenh/lsdalm/pkg/go-mpd"
	"github.com/jdeisenh/lsdalm/pkg/go-xsd-types"
	"github.com/rs/zerolog"
)

type MpdDiffer struct {
	logger  zerolog.Logger // Logger instance
	lastMpd *mpd.MPD
	ast     time.Time
}

func NewMpdDiffer(logger zerolog.Logger) *MpdDiffer {
	return &MpdDiffer{
		logger: logger,
	}
}

func Equal[C xsd.DateTime | xsd.Duration | string](a, b *C) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func AdaptationSetById(set []*mpd.AdaptationSet, id *string) *mpd.AdaptationSet {
	for _, p := range set {
		if Equal(p.Id, id) {
			return p
		}
	}
	return nil
}

func PeriodById(set []*mpd.Period, id *string) *mpd.Period {
	for _, p := range set {
		if Equal(p.ID, id) {
			return p
		}
	}
	return nil
}

func RepresentationById(set []mpd.Representation, id *string) *mpd.Representation {
	for _, p := range set {
		if Equal(p.ID, id) {
			return &p
		}
	}
	return nil
}

// EventBySchemeId returns the Event from the EventStream matching both schema, if not nil, and ID
// returns nil if not found
func EventBySchemeId(set []*mpd.EventStream, schema *string, id uint64) *mpd.Event {

	for _, es := range set {
		if Equal(es.SchemeIdUri, schema) {
			for _, event := range es.Event {
				if event.Id == id {
					return &event
				}
			}
		}
	}
	return nil
}

func (md *MpdDiffer) DiffRepresentations(old, cur *mpd.Representation, start time.Time) error {
	id := fmt.Sprintf("Rep: %s", EmptyIfNil(cur.ID))
	return md.DiffSegmentTemplate(old.SegmentTemplate, cur.SegmentTemplate, start, id)
}

func (md *MpdDiffer) DiffSegmentTimeline(old, cur *mpd.SegmentTimeline) error {
	if old != nil && len(old.S) > 0 && cur != nil && len(cur.S) > 0 && old.S[0].T != cur.S[0].T {
		md.logger.Info().Msgf("Timeline begin change: %d to %d", old.S[0].T, cur.S[0].T)
	}
	return nil
}

func (md *MpdDiffer) AddSegmentTemplate(cur *mpd.SegmentTemplate, start time.Time, id string) error {

	cf, ct := sumSegmentTemplate(cur, start)

	md.logger.Info().Msgf("%s: Dropped %s Added %s", id, "    ", Round(ct.Sub(cf)))
	return nil
}
func (md *MpdDiffer) DiffSegmentTemplate(old, cur *mpd.SegmentTemplate, start time.Time, id string) error {
	/*
		// Diff SegmentTimelines
		var oldt, curt *mpd.SegmentTimeline
		if old != nil {
			oldt = old.SegmentTimeline
		}
		if cur != nil {
			curt = cur.SegmentTimeline
		}

		md.DiffSegmentTimeline(oldt, curt)
	*/

	of, ot := sumSegmentTemplate(old, start)
	cf, ct := sumSegmentTemplate(cur, start)

	if cf != of || ot != ct {
		md.logger.Info().Msgf("%s: Dropped %8s Added %8s", id, Round(cf.Sub(of)), Round(ct.Sub(ot)))
	}

	return nil
}

func (md *MpdDiffer) AddAdaptationSet(cur *mpd.AdaptationSet, periodStart time.Time) error {
	for _, newr := range cur.Representations {
		md.logger.Warn().Msgf("New Representation %s", EmptyIfNil(newr.ID))
	}
	id := fmt.Sprintf("AS%2s:%15s:%-15s", EmptyIfNil(cur.Id), cur.MimeType, EmptyIfNil(cur.Codecs))
	md.AddSegmentTemplate(cur.SegmentTemplate, periodStart, id)

	return nil
}

func (md *MpdDiffer) DiffAdaptationSet(old, cur *mpd.AdaptationSet, periodStart time.Time) error {
	for _, oldr := range old.Representations {
		if curr := RepresentationById(cur.Representations, oldr.ID); curr != nil {
			md.DiffRepresentations(&oldr, curr, periodStart)
		} else {
			md.logger.Warn().Msgf("Representation Set %s gone", EmptyIfNil(oldr.ID))
		}
	}
	for _, newr := range cur.Representations {
		if oldr := RepresentationById(old.Representations, newr.ID); oldr == nil {
			md.logger.Warn().Msgf("New Representation %s", EmptyIfNil(newr.ID))
		}
	}
	id := fmt.Sprintf("AS%2s:%15s:%-15s", EmptyIfNil(cur.Id), cur.MimeType, EmptyIfNil(cur.Codecs))
	md.DiffSegmentTemplate(old.SegmentTemplate, cur.SegmentTemplate, periodStart, id)

	return nil
}

func PeriodStart(period *mpd.Period) (start time.Duration) {
	// Calculate period start
	if period.Start != nil {
		startmed, _ := (*period.Start).ToNanoseconds()
		start = time.Duration(startmed)
	}
	return
}

func (md *MpdDiffer) AddPeriod(cur *mpd.Period) error {
	periodStart := md.ast.Add(PeriodStart(cur))
	for _, newa := range cur.AdaptationSets {
		md.logger.Warn().Msgf("New AdaptationSet %s", EmptyIfNil(newa.Id))
		md.AddAdaptationSet(newa, periodStart)
	}
	for _, cures := range cur.EventStream {
		for _, curee := range cures.Event {
			to := ZeroIfNil(curee.PresentationTime) - ZeroIfNil(cures.PresentationTimeOffset)
			timescale := ZeroIfNil(cures.Timescale)
			at := periodStart.Add(TLP2Duration(int64(to), timescale))
			duration := TLP2Duration(int64(ZeroIfNil(curee.Duration)), timescale)
			md.OnNewEvent(&curee, EmptyIfNil(cures.SchemeIdUri), at, duration)
		}
	}
	return nil

}

func (md *MpdDiffer) DiffPeriod(old, cur *mpd.Period) error {

	// Todo: Check if start or duration changed
	// Todo: Check BaseURL, EventStream

	periodStart := md.ast.Add(PeriodStart(cur))
	for asi, olda := range old.AdaptationSets {
		var cura *mpd.AdaptationSet
		if olda.Id == nil {
			// No ID: Use same index
			if asi < len(cur.AdaptationSets) {
				cura = old.AdaptationSets[asi]
			}
		} else {
			cura = AdaptationSetById(cur.AdaptationSets, olda.Id)
		}
		if cura != nil {
			//md.logger.Warn().Msgf("Found AS ID %s: %+v", EmptyIfNil(olda.Id), cura)
			md.DiffAdaptationSet(olda, cura, periodStart)
		} else {
			md.logger.Warn().Msgf("AdaptationSet ID %s gone", EmptyIfNil(olda.Id))
		}
	}
	for _, newa := range cur.AdaptationSets {
		if olda := AdaptationSetById(old.AdaptationSets, newa.Id); olda == nil {
			md.logger.Warn().Msgf("New AdaptationSet %s", EmptyIfNil(newa.Id))
		}
	}

	// Diff EventStream
	for _, oldes := range old.EventStream {
		for _, oldee := range oldes.Event {
			if cure := EventBySchemeId(cur.EventStream, oldes.SchemeIdUri, oldee.Id); cure != nil {
				// Events should be the same, check
				//md.DiffEvent(olda, cura, periodStart)
				//md.logger.Info().Msgf("Found %s:%d", *oldes.SchemeIdUri, oldee.Id)
			} else {
				md.logger.Info().Msgf("Event %s:%d gone", EmptyIfNil(oldes.SchemeIdUri), oldee.Id)
			}
		}
	}
	for _, cures := range cur.EventStream {
		for _, curee := range cures.Event {
			if EventBySchemeId(old.EventStream, cures.SchemeIdUri, curee.Id) == nil {
				to := ZeroIfNil(curee.PresentationTime) - ZeroIfNil(cures.PresentationTimeOffset)
				timescale := ZeroIfNil(cures.Timescale)
				at := periodStart.Add(TLP2Duration(int64(to), timescale))
				duration := TLP2Duration(int64(ZeroIfNil(curee.Duration)), timescale)
				md.OnNewEvent(&curee, EmptyIfNil(cures.SchemeIdUri), at, duration)
			}
		}
	}

	return nil
}

func (md *MpdDiffer) Update(mpde *mpd.MPD) error {
	old := md.lastMpd
	cur := mpde
	defer func() { md.lastMpd = mpde }()
	if old == nil {
		// Firstone
		if mpde.AvailabilityStartTime != nil {
			md.ast = time.Time(*mpde.AvailabilityStartTime)
		}
		for _, newp := range cur.Period {
			md.AddPeriod(newp)
			md.OnNewPeriod(newp)
		}
		return nil
	}
	/*
		if !Equal(old.PublishTime, cur.PublishTime) {
			md.logger.Info().Msg("Publish Time update")
		}
	*/
	for _, oldp := range old.Period {
		if curp := PeriodById(cur.Period, oldp.ID); curp != nil {
			md.DiffPeriod(oldp, curp)
		} else {
			md.logger.Warn().Msgf("Period ID %s gone", EmptyIfNil(oldp.ID))
		}
	}
	for _, newp := range cur.Period {
		if oldp := PeriodById(old.Period, newp.ID); oldp == nil {
			md.OnNewPeriod(newp)
		}
	}
	return nil
}

func (md *MpdDiffer) OnNewPeriod(period *mpd.Period) {
	md.logger.Warn().Msgf("New Period %s starts %s", EmptyIfNil(period.ID), md.ast.Add(PeriodStart(period)))
}

func (md *MpdDiffer) OnNewEvent(event *mpd.Event, scheme string, at time.Time, duration time.Duration) {
	md.logger.Info().Msgf("New Event %s:%d at %s Duration %s", scheme, event.Id, at, duration)
}
