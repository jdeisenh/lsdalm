package streamgetter

import (
	"errors"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/jdeisenh/lsdalm/pkg/go-mpd"
	"github.com/rs/zerolog/log"
)

// GetTimeRange gets first and last time from SegmentTimeLine
func GetTimeRange(stl *mpd.SegmentTimeline) (from, to uint64) {
	for _, s := range stl.S {
		var repeat int64
		if s.T != nil {
			to = *s.T
			if from == 0 {
				from = to
			}
		}

		if s.R != nil {
			repeat = *s.R
		}
		to += s.D * uint64(repeat+1)
	}
	return
}

// Iterator walking a SegmentTimeline S chain, returning time and duration in each step
func All(stl *mpd.SegmentTimeline) func(func(t, d uint64) bool) {
	return func(yield func(t, d uint64) bool) {
		var ct uint64
		for _, s := range stl.S {
			var repeat int64
			if s.T != nil {
				ct = *s.T
			}
			if s.R != nil {
				repeat = *s.R
			}
			for r := int64(0); r <= repeat; r++ {
				if !yield(ct, s.D) {
					return
				}
				ct += s.D
			}
		}
	}
}

// Append adds a segemnt to a SegmentTimeline
// We do not check the time for gaps, this would require re-counting on every insert
func Append(st *mpd.SegmentTimeline, t, d uint64) {
	if len(st.S) == 0 {
		st.S = append(st.S, &mpd.SegmentTimelineS{T: &t, D: d})
		return
	}
	last := st.S[len(st.S)-1]
	if last.D == d {
		if last.R == nil {
			r := int64(1)
			last.R = &r
		} else {
			*last.R++
		}
	} else {
		st.S = append(st.S, &mpd.SegmentTimelineS{T: &t, D: d})
	}
}

// AppendR adds a segemnt to a SegmentTimeline
// We do not check the time for gaps, this would require re-counting on every insert
func AppendR(st *mpd.SegmentTimeline, t, d uint64, r int64) {
	var rp *int64
	var tp *uint64
	if r != 0 {
		rp = &r
	}
	if t != 0 {
		tp = &t
	}
	if len(st.S) == 0 {
		st.S = append(st.S, &mpd.SegmentTimelineS{T: tp, D: d, R: rp})
		return
	}
	last := st.S[len(st.S)-1]
	if last.D == d {
		if last.R == nil {
			r := int64(r + 1)
			last.R = &r
		} else {
			*last.R += r + 1
		}
	} else {
		st.S = append(st.S, &mpd.SegmentTimelineS{T: tp, D: d, R: rp})
	}
}

// filterSegmentTemplate walks all Segments inside a Segmenttemplate, calling filter, removing every segment that the callback returns false
func filterSegmentTemplate(st *mpd.SegmentTemplate, periodStart time.Time, filter func(t time.Time, d time.Duration) bool) (total, filtered int) {

	if st == nil || st.SegmentTimeline == nil {
		return
	}
	var pto uint64
	if st.PresentationTimeOffset != nil {
		pto = *st.PresentationTimeOffset
	}
	stl := st.SegmentTimeline
	if stl == nil {
		return
	}
	//fmt.Printf("SegmentTemplate: %+v\n", st)
	timescale := uint64(1)
	if st.Timescale != nil {
		timescale = *st.Timescale
	}
	st.SegmentTimeline = filterSegmentTimeline(st.SegmentTimeline, func(t, d uint64) bool {
		total++
		r := filter(
			periodStart.Add(TLP2Duration(int64(t-pto), timescale)),
			TLP2Duration(int64(d), timescale),
		)
		if !r {
			filtered++
		}
		return r
	})
	return
}

func filterSegmentTimeline(in *mpd.SegmentTimeline, filter func(t, d uint64) bool) *mpd.SegmentTimeline {
	out := new(mpd.SegmentTimeline)
	for t, d := range All(in) {
		if filter(t, d) {
			Append(out, t, d)
		}
	}
	return out
}

// walkSegmentTemplate walks a segmentTemplate and calls 'action' on all media Segments with their full URL
func walkSegmentTemplate(st *mpd.SegmentTemplate, segmentPath *url.URL, repId string, action func(*url.URL) error) error {

	pathTemplate := NewPathReplacer(*st.Media)
	if st.Initialization != nil {
		init := strings.Replace(*st.Initialization, "$RepresentationID$", repId, 1)
		action(segmentPath.JoinPath(init))
	}
	// Walk the Segment
	if st.SegmentTimeline == nil {
		return errors.New("SegmentTemplate without Timeline not supported")
	}
	stl := st.SegmentTimeline
	number := 0
	if st.StartNumber != nil {
		number = int(*st.StartNumber)
	}

	for t := range All(stl) {
		ppa := pathTemplate.ToPath(int(t), number, repId)
		//fmt.Printf("Path %s:%s\n", media, ppa)
		fullUrl := segmentPath.JoinPath(ppa)
		action(fullUrl)
		number++
	}
	return nil
}

// sumSegmentTemplate returns first and last presentationTime of a SegmentTemplate with Timeline
func sumSegmentTemplate(st *mpd.SegmentTemplate, periodStart time.Time) (from, to time.Time) {

	if st == nil {
		return
	}
	var pto uint64
	if st.PresentationTimeOffset != nil {
		pto = *st.PresentationTimeOffset
	}
	stl := st.SegmentTimeline
	if stl == nil {
		return
	}
	//fmt.Printf("SegmentTemplate: %+v\n", st)
	timescale := uint64(1)
	if st.Timescale != nil {
		timescale = *st.Timescale
	}
	ft, lt := GetTimeRange(stl)
	from = periodStart.Add(TLP2Duration(int64(ft-pto), timescale))
	to = periodStart.Add(TLP2Duration(int64(lt-pto), timescale))
	return
}

// Build a fetch base URL from manifest URL, and basepath in period
func segmentPathFromPeriod(period *mpd.Period, mpdUrl *url.URL) *url.URL {
	var segmentPath, baseurl *url.URL
	var err error
	if len(period.BaseURL) > 0 {
		base := period.BaseURL[0].Value
		baseurl, err = url.Parse(base)
		if err != nil {
			log.Warn().Err(err).Msg("Parse URL")
		}
	}
	if baseurl.IsAbs() {
		segmentPath = baseurl
	} else {
		// Combine mpd URL and base
		segmentPath = new(url.URL)
		*segmentPath = *mpdUrl
		// Cut to directory, extend by base path
		joined, err := url.JoinPath(path.Dir(segmentPath.Path), baseurl.Path)
		if err != nil {
			log.Fatal().Err(err).Msg("Path extension")
		}
		segmentPath.Path = joined
	}
	return segmentPath
}

// shiftPto adds 'shiftValue' to the presentationTimeOffset
func shiftPto(st *mpd.SegmentTemplate, shiftValue time.Duration) {

	timescale := uint64(1)
	if st.Timescale != nil {
		timescale = *st.Timescale
	}
	pto := uint64(0)
	if lpto := st.PresentationTimeOffset; lpto != nil {
		pto = *lpto
	}
	pto = uint64(int64(pto) + Duration2TLP(shiftValue, timescale))
	// Write back
	st.PresentationTimeOffset = &pto
}

// reframePeriods moves start of period to newStart and sets ID
// The shift is countered by a change to presentationTimeOffset, so
// the position on the timeline does not change
func reframePeriods(mpde *mpd.MPD, id string, newStart time.Time) {
	// Walk all Periods, AdaptationSets and Representations
	// Calculate period start
	var ast time.Time
	if mpde.AvailabilityStartTime != nil {
		ast = time.Time(*mpde.AvailabilityStartTime)
	}
	var shiftValue time.Duration
	for _, period := range mpde.Period {

		var start time.Duration
		if period.Start != nil {
			startmed, _ := (*period.Start).ToNanoseconds()
			start = time.Duration(startmed)
			//log.Warn().Msgf("Start: %s", start)
			shiftValue = newStart.Sub(ast) - start
			//log.Warn().Msgf("Newstart: %s", newStart)
			//log.Warn().Msgf("Shift: %s", shiftValue)
			*period.Start = DurationToXsdDuration(newStart.Sub(ast))
		}
		if period.ID != nil {
			*period.ID = id
		}

		// Shift EventStreams
		resultAs := period.AdaptationSets[:0]
		for _, as := range period.AdaptationSets {
			if !(as.MimeType == "audio/mp4" && EmptyIfNil(as.Codecs) == "mp4a.40.2") &&
				!(as.MimeType == "video/mp4") {
				// Hack: only keep important AdaptationSets, that also are aligned
				continue
			}

			if st := as.SegmentTemplate; st != nil {
				shiftPto(st, shiftValue)
			} else {
				for _, pres := range as.Representations {
					if st := pres.SegmentTemplate; st != nil {
						shiftPto(st, shiftValue)
					}
				}
			}
			resultAs = append(resultAs, as)
		}
		period.AdaptationSets = resultAs
		// Shift Eventstreams
		for _, es := range period.EventStream {
			timescale := uint64(1)
			if es.Timescale != nil {
				timescale = *es.Timescale
			}
			pto := uint64(0)
			if lpto := es.PresentationTimeOffset; lpto != nil {
				pto = *lpto
			}
			pto = uint64(int64(pto) + Duration2TLP(shiftValue, timescale))
			// Write back
			es.PresentationTimeOffset = &pto

		}
	}

}

// Iterate through all periods, representation, segmentTimeline and
// call 'action' with the URL
func onAllSegmentUrls(mpd *mpd.MPD, mpdUrl *url.URL, action func(*url.URL) error) error {
	// Walk all Periods, AdaptationSets and Representations
	for _, period := range mpd.Period {
		segmentPath := segmentPathFromPeriod(period, mpdUrl)
		for _, as := range period.AdaptationSets {
			for _, pres := range as.Representations {
				if pres.ID == nil {
					continue
				}
				repId := *pres.ID
				if as.SegmentTemplate != nil {
					if err := walkSegmentTemplate(as.SegmentTemplate, segmentPath, repId, action); err != nil {
						break
					}
				} else if pres.SegmentTemplate != nil {
					if err := walkSegmentTemplate(pres.SegmentTemplate, segmentPath, repId, action); err != nil {
						break
					}
				}
			}
		}
	}
	return nil
}
