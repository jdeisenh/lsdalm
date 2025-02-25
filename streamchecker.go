package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	//"gitlab.com/nowtilus/eventinjector/pkg/go-mpd"
	"github.com/unki2aut/go-mpd"
)

const (
	ManifestPath   = "manifests"
	FetchQueueSize = 2000 // Max number of outstanding requests in queue
)

// fetchAndStoreUrl queues an URL for fetching
func (sc *StreamChecker) fetchAndStoreUrl(fetchthis *url.URL) error {

	localpath := path.Join(sc.dumpdir, fetchthis.Path)
	_, err := os.Stat(localpath)
	if err == nil {
		// Assume file exists
		return nil
	}
	// Queue requesta
	select {
	case sc.fetchqueue <- fetchthis:
		return nil
	default:
		fmt.Println("Queue full")
		return errors.New("Queue full")
	}
}

// executeFetchAndStore gets a segment and stores it
func (sc *StreamChecker) executeFetchAndStore(fetchme *url.URL) error {

	localpath := path.Join(sc.dumpdir, fetchme.Path)
	os.MkdirAll(path.Dir(localpath), 0777)
	_, err := os.Stat(localpath)
	if err == nil {
		// Assume file exists
		return nil
	}
	resp, err := http.Get(fetchme.String())
	if err != nil {
		log.Println(err)
	} else {
		if resp.Body != nil {
			defer resp.Body.Close()
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
		}
		log.Printf("Got: %s", fetchme.String())
		err = os.WriteFile(localpath, body, 0644)
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}

// Get first and last time from SegmentTimeLine
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

func walkSegmentTemplate(st *mpd.SegmentTemplate, segmentPath *url.URL, repId string, fetch func(*url.URL) error) {

	//fmt.Printf("Timescale: %+v\n", timescale)
	//fmt.Printf("Media: %+v\n", *st.Media)
	pathTemplate := NewPathReplacer(*st.Media)
	if st.Initialization != nil {
		init := strings.Replace(*st.Initialization, "$RepresentationID$", repId, 1)
		//fmt.Printf("Init: %s\n", init)
		fetch(segmentPath.JoinPath(init))
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
		fetch(fullUrl)
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

// fetchAndStore gets a manifest from URL, decode the manifest, dump stats, and calls back the action
// callback on all Segments
func (sc *StreamChecker) fetchAndStore() error {
	mpd := new(mpd.MPD)

	resp, err := http.Get(sc.sourceUrl.String())
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()
	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	if sc.dumpdir != "" {
		// Store the manifest
		filename := path.Join(outManifestPath, "manifest-"+time.Now().Format(time.TimeOnly)+".mpd")
		err = os.WriteFile(filename, contents, 0644)
		if err != nil {
			log.Println(err)
		}
	}
	err = mpd.Decode(contents)
	if err != nil {
		log.Fatalln(err)
	}
	err = walkMpd(mpd)
	if sc.dumpdir != "" {
		err = onAllSegmentUrls(mpd, sc.sourceUrl, sc.fetchAndStoreUrl)
	}
	return err
}

// Iterate through all periods, representation, segmentTimeline and
// call back with the URL
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

const dateShortFmt = "15:04:05.00"

// Iterate through all periods, representation, segmentTimeline and
// call back with the URL
func walkMpd(mpd *mpd.MPD) error {

	now := time.Now()

	if len(mpd.Period) == 0 {
		return errors.New("No periods")
	}
	var ast time.Time
	if mpd.AvailabilityStartTime != nil {
		ast = time.Time(*mpd.AvailabilityStartTime)
	}
	//fmt.Println(ast)
	// TODO: Choose best period for adaptationSet Refernce
	referencePeriod := mpd.Period[0]
	// Choose one with Period-AdaptationSet->SegmentTemplate, which in our case is
	// usually the live period
	for _, period := range mpd.Period {
		if len(period.AdaptationSets) == 0 {
			continue
		}

		if period.AdaptationSets[0].SegmentTemplate != nil {
			referencePeriod = period
			break
		}
	}
	// Walk all Periods, AdaptationSets and Representations
	var theGap time.Time
	for _, as := range referencePeriod.AdaptationSets {
		for periodIdx, period := range mpd.Period {
			// Find the adaptationset matching the reference adaptationset
			if len(period.AdaptationSets) == 0 {
				continue
			}
			var asr = period.AdaptationSets[0]
			for _, as := range period.AdaptationSets {
				if asr.MimeType == as.MimeType {
					asr = as
					break
				}
			}

			// Calculate period start
			var start time.Duration
			if period.Start != nil {
				startmed, _ := (*period.Start).ToNanoseconds()
				start = time.Duration(startmed)
			}
			periodStart := ast.Add(start)
			from, to := sumSegmentTemplate(asr.SegmentTemplate, periodStart)
			if from.IsZero() {
				for _, pres := range asr.Representations {
					if pres.SegmentTemplate != nil {
						from, to = sumSegmentTemplate(pres.SegmentTemplate, periodStart)
						break
					}
				}
			}
			if periodIdx == 0 {
				// Line start: mimetype, timeshiftBufferDepth
				fmt.Printf("%10s: %8s", as.MimeType, RoundTo(now.Sub(from), time.Second))
			} else if gap := from.Sub(theGap); gap > time.Millisecond || gap < -time.Millisecond {
				fmt.Printf("GAP: %s", Round(gap))
			}

			fmt.Printf(" [%s-%s[", from.Format(dateShortFmt), to.Format(dateShortFmt))

			if periodIdx == len(mpd.Period)-1 {
				fmt.Printf(" %s\n", RoundTo(now.Sub(to), time.Second)) // Live edge distance
			}
			theGap = to
		}
	}
	return nil
}

type StreamChecker struct {
	sourceUrl   *url.URL
	dumpdir     string
	manifestDir string
	// Duration for manifests
	updateFreq time.Duration
	fetchqueue chan *url.URL

	// statistics
}

func NewStreamChecker(source, dumpdir string, updateFreq time.Duration) (*StreamChecker, error) {

	st := &StreamChecker{
		dumpdir:     dumpdir,
		updateFreq:  updateFreq,
		manifestDir: path.Join(dumpdir, ManifestPath),
		fetchqueue:  make(chan *url.URL, fetchQueueSize),
	}
	var err error
	st.sourceUrl, err = url.Parse(source)
	if err != nil {
		return nil, err
	}
	go st.fetcher()
	if err := os.MkdirAll(st.manifestDir, 0777); err != nil {
		return nil, errors.New("Cannot create directory")
	}
	return st, nil
}

// Do fetches and analyzes forever
func (sc *StreamChecker) Do() {

	ticker := time.NewTicker(sc.updateFreq)
	for {
		_ = <-ticker.C
		log.Println("Tick")
		sc.fetchAndStore()
	}

}

// Goroutine
func (sc *StreamChecker) fetcher() {

	for i := range sc.fetchqueue {
		sc.executeFetchAndStore(i)
	}

}
