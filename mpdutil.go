package main

import (
	"fmt"
	"log"
	"net/url"
	"path"
	"strings"
	"time"

	//"gitlab.com/nowtilus/eventinjector/pkg/go-mpd"
	"github.com/unki2aut/go-mpd"
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

// walkSegmentTemplate walks a segmentTemplate and calls 'action' on all media Segments with their full URL
func walkSegmentTemplate(st *mpd.SegmentTemplate, segmentPath *url.URL, repId string, action func(*url.URL) error) {

	//fmt.Printf("Timescale: %+v\n", timescale)
	//fmt.Printf("Media: %+v\n", *st.Media)
	pathTemplate := NewPathReplacer(*st.Media)
	if st.Initialization != nil {
		init := strings.Replace(*st.Initialization, "$RepresentationID$", repId, 1)
		//fmt.Printf("Init: %s\n", init)
		action(segmentPath.JoinPath(init))
	}
	// Walk the Segment
	if st.SegmentTimeline == nil {
		fmt.Println("SegmentTemplate without Timeline not supported")
		return
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
	//fmt.Printf("SegmentTemplate: %+v\n", st)
	timescale := uint64(1)
	if st.Timescale != nil {
		timescale = *st.Timescale
	}
	ft, lt := GetTimeRange(stl)
	from = periodStart.Add(TLP2Duration(ft-pto, timescale))
	to = periodStart.Add(TLP2Duration(lt-pto, timescale))
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
			log.Println(err)
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
			log.Fatal(err)
		}
		segmentPath.Path = joined
	}
	return segmentPath
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
					walkSegmentTemplate(as.SegmentTemplate, segmentPath, repId, action)
				} else if pres.SegmentTemplate != nil {
					walkSegmentTemplate(pres.SegmentTemplate, segmentPath, repId, action)
				}
			}
		}
	}
	return nil
}
