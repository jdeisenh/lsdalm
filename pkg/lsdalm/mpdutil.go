package lsdalm

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

// Append adds a segment to a SegmentTimeline
// We do not check the time for gaps, this would require re-counting on every insert
func Append(st *mpd.SegmentTimeline, t, d uint64, r int64) {
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

// walkSegmentTemplate walks a segmentTemplate and calls 'action' on all media Segments with their full URL
func WalkSegmentTemplate(st *mpd.SegmentTemplate, segmentPath *url.URL, repId string, start time.Duration, action func(*url.URL, time.Duration, time.Duration, time.Duration) error) error {

	pathTemplate := NewPathReplacer(*st.Media)
	if st.Initialization != nil {
		init := strings.Replace(*st.Initialization, "$RepresentationID$", repId, 1)
		action(segmentPath.JoinPath(init), 0, 0, 0)
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
	timescale := ZeroIfNil(st.Timescale)
	pto := ZeroIfNil(st.PresentationTimeOffset)

	offset := start + TLP2Duration(int64(pto), timescale)

	for t, d := range All(stl) {
		ppa := pathTemplate.ToPath(int(t), number, repId)
		//fmt.Printf("Path %s:%s\n", media, ppa)
		fullUrl := segmentPath.JoinPath(ppa)
		action(fullUrl, TLP2Duration(int64(t), timescale), TLP2Duration(int64(d), timescale), offset)
		number++
	}
	return nil
}

// SumSegmentTemplate returns first and last presentationTime of a SegmentTemplate with Timeline
func SumSegmentTemplate(st *mpd.SegmentTemplate, periodStart time.Time) (from, to time.Time) {

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

// walkegmentTemplate walks the template and calls action
func WalkSegmentTemplateTimings(st *mpd.SegmentTemplate, periodStart time.Time, action func(time.Time, time.Duration)) {

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
	for t, d := range All(stl) {
		action(
			periodStart.Add(TLP2Duration(int64(t-pto), timescale)),
			TLP2Duration(int64(d), timescale),
		)
	}
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
func ShiftPto(st *mpd.SegmentTemplate, shiftValue time.Duration) {

	timescale := uint64(1)
	if st.Timescale != nil {
		timescale = *st.Timescale
	}
	pto := uint64(0)
	if lpto := st.PresentationTimeOffset; lpto != nil {
		pto = *lpto
	}
	ptn := uint64(int64(pto) + Duration2TLP(shiftValue, timescale))
	//log.Debug().Msgf("PTO %d shift %s %d  new %d",pto,shiftValue,Duration2TLP(shiftValue, timescale),ptn)
	// Write back
	if ptn == 0 {
		st.PresentationTimeOffset = nil
	} else {
		st.PresentationTimeOffset = &ptn
	}
	startNumber := ZeroIfNil(st.StartNumber)
	duration := ZeroIfNil(st.Duration)
	if duration == 0 {
		return
	}
	//Number increase with every segment, that is duration/timescale
	stn := uint64(int64(startNumber) + Duration2TLP(shiftValue, timescale)/int64(duration))

	if stn == 0 {
		st.StartNumber = nil
	} else {
		st.StartNumber = &stn
	}

}

// Iterate through all periods, representation, segmentTimeline and
// call 'action' with the URL
func OnAllSegmentUrls(mpd *mpd.MPD, mpdUrl *url.URL, action func(*url.URL, time.Duration, time.Duration, time.Duration) error) error {
	// Walk all Periods, AdaptationSets and Representations
	for _, period := range mpd.Period {
		start := GetStart(period)
		segmentPath := segmentPathFromPeriod(period, mpdUrl)
		for _, as := range period.AdaptationSets {
			for _, pres := range as.Representations {
				if pres.ID == nil {
					continue
				}
				repId := *pres.ID
				if as.SegmentTemplate != nil {
					if err := WalkSegmentTemplate(as.SegmentTemplate, segmentPath, repId, start, action); err != nil {
						break
					}
				} else if pres.SegmentTemplate != nil {
					if err := WalkSegmentTemplate(pres.SegmentTemplate, segmentPath, repId, start, action); err != nil {
						break
					}
				}
			}
		}
	}
	return nil
}

// ReBaseMpd wil change BaseURL, append path of originalBase and then, if localMedia is true, dropping the protocol/host
func ReBaseMpd(mpde *mpd.MPD, originalBase *url.URL, localMedia bool) *mpd.MPD {
	if len(mpde.Period) == 0 {
		return nil
	}
	outMpd := new(mpd.MPD)
	*outMpd = *mpde
	outMpd.Period = make([]*mpd.Period, 0, 1)

	for _, period := range mpde.Period {
		np := new(mpd.Period)
		*np = *period

		// Expand to full URL first
		if originalBase != nil {
			baseurl := ""
			if len(np.BaseURL) > 0 {
				baseurl = np.BaseURL[0].Value
			}
			baseurlUrl := ConcatURL(originalBase, baseurl)
			np.BaseURL = make([]*mpd.BaseURL, 0, 1)
			nburl := new(mpd.BaseURL)
			np.BaseURL = append(np.BaseURL, nburl)
			if localMedia {
				np.BaseURL[0].Value = baseurlUrl.Path[1:]
			} else {
				np.BaseURL[0].Value = baseurlUrl.String()
			}
		}
		outMpd.Period = append(outMpd.Period, np)

	}
	return outMpd
}

// mergeMpd appends the periods from mpd2 into mpd1,
func mergeMpd(mpd1, mpd2 *mpd.MPD) *mpd.MPD {
	if mpd1 == nil {
		return mpd2
	} else if mpd2 == nil {
		return mpd1
	} else {
		mpd1.Period = append(mpd1.Period, mpd2.Period...)
		return mpd1
	}
}

// Concatenat URLs
// If b is absolute, just use this
// if not, append
func ConcatURL(a *url.URL, br string) *url.URL {
	if br == "" {
		return a
	}
	b, err := url.Parse(br)
	if err != nil {
		log.Error().Err(err).Msg("Path extension")
		return nil
	}
	if b.IsAbs() {
		return b
	} else {
		// Cut to directory, extend by base path
		joined := a.JoinPath(b.Path)
		return joined
	}
}

// baseToPath converts a Base URL to a absolute local path
func BaseToPath(base, prefix string) string {
	if prefix == "" {
		// No change
		return base
	}
	var baseurl *url.URL
	var err error
	baseurl, err = url.Parse(base)
	if err != nil {
		log.Warn().Err(err).Msg("Parse URL")
		return base
	}
	if strings.HasPrefix(baseurl.Path, "/") {
		// Path only
		return baseurl.Path
	} else {
		return path.Join(prefix, baseurl.Path)
	}
}

// Returns a Period with mannipulated start and overwrite ID
func PeriodWithStart(in *mpd.Period, start time.Duration, id string) *mpd.Period {
	p := new(mpd.Period)
	*p = *in
	newStart := DurationToXsdDuration(start)
	p.Start = &newStart
	if id != "" {
		newid := id
		p.ID = &newid
	}
	return p
}

// GetAst finds the AvailabilityStartTime in an MPD
// Returns empty time if not found
func GetAst(in *mpd.MPD) time.Time {
	var ast time.Time
	if in != nil && in.AvailabilityStartTime != nil {
		ast = time.Time(*in.AvailabilityStartTime)
	}
	return ast
}

// GetStart get the start value of a period as time.Duration.
// returns 0 if not given or invalid
func GetStart(period *mpd.Period) time.Duration {
	if period.Start == nil {
		return 0
	}
	startmed, _ := (*period.Start).ToNanoseconds()
	return time.Duration(startmed)
}

// Copy is a generic that returns a shallow copy of the original element
func Copy[T mpd.MPD | mpd.Period | mpd.AdaptationSet | mpd.SegmentTemplate | mpd.SegmentTimeline | mpd.EventStream | mpd.Event](org *T) *T {
	if org == nil {
		return nil
	}
	r := new(T)
	*r = *org
	return r
}

// ZeroIfNil is a short hand to evaluate a *uint64
func ZeroIfNil[T int64 | uint64](in *T) T {
	if in == nil {
		return 0
	}
	return *in
}

// EmptyIfNil is a short hand to evaluate a string pointer
func EmptyIfNil(in *string) string {
	if in == nil {
		return ""
	}
	return *in
}

// PeriodSegmentLimits returns the timerange for which Samples are available for all tracks
func PeriodSegmentLimits(p *mpd.Period, ast time.Time) (from, to time.Time) {
	to = time.Now()
	for _, as := range p.AdaptationSets {
		if as.SegmentTemplate != nil {
			lfrom, lto := SumSegmentTemplate(as.SegmentTemplate, ast.Add(PeriodStart(p)))
			if !lfrom.IsZero() && lfrom.After(from) {
				from = lfrom
			}
			if !lto.IsZero() && lto.Before(to) {
				to = lto
			}

		} else {
			for _, rep := range as.Representations {
				if rep.SegmentTemplate != nil {
					lfrom, lto := SumSegmentTemplate(rep.SegmentTemplate, ast.Add(PeriodStart(p)))
					if !lfrom.IsZero() && lfrom.After(from) {
						from = lfrom
					}
					if !lto.IsZero() && lto.Before(to) {
						to = lto
					}
				}
			}
		}
	}
	return
}

// ExactDuration finds the shortes Track by summing the samples for all Representations
func ExactDuration(m *mpd.MPD) time.Duration {

	dur := time.Hour * 10000 // Upper limit
	if m == nil {
		return 0
	}
	ast := GetAst(m)
	for _, p := range m.Period {
		for _, as := range p.AdaptationSets {
			if as.SegmentTemplate != nil {
				from, to := SumSegmentTemplate(as.SegmentTemplate, ast.Add(PeriodStart(p)))
				du := to.Sub(from)
				if du < dur {
					dur = du
				}
			} else {
				for _, rep := range as.Representations {
					if rep.SegmentTemplate != nil {
						from, to := SumSegmentTemplate(rep.SegmentTemplate, ast.Add(PeriodStart(p)))
						du := to.Sub(from)
						if du < dur {
							dur = du
						}
					}
				}
			}
		}
	}
	return dur
}
