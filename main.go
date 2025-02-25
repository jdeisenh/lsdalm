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

const doStore = false

const outPath = "dump/"
const outManifestPath = outPath + "manifests/"

const backbuffer = 10000 // Max number of outstanding requests

var fetchme chan *url.URL

// Goroutine
func fetcher() {

	for i := range fetchme {
		executeFetchAndStore(i)
	}

}

func fetchAndStoreUrl(fetchthis *url.URL) error {
	if !doStore {
		return nil
	}

	localpath := path.Join(outPath, fetchthis.Path)
	_, err := os.Stat(localpath)
	if err == nil {
		// Assume file exists
		return nil
	}
	// Queue requesta
	select {
	case fetchme <- fetchthis:
		return nil
	default:
		fmt.Println("Queue full")
		return errors.New("Queue full")
	}
}

func executeFetchAndStore(fetchme *url.URL) error {

	localpath := path.Join(outPath, fetchme.Path)
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

// Get first and last
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

type PathReplacer struct {
	fmt string
}

func NewPathReplacer(template string) *PathReplacer {

	fmt := template
	// Replace with go format parameters
	fmt = strings.Replace(fmt, "$Time$", "%[1]d", 1)
	fmt = strings.Replace(fmt, "$Number$", "%[2]d", 1)
	fmt = strings.Replace(fmt, "$RepresentationID$", "%[3]s", 1)
	return &PathReplacer{fmt}
}

func (r *PathReplacer) ToPath(time, number int, representationId string) string {
	return fmt.Sprintf(r.fmt, time, number, representationId)
}

// TLP2Duration Converts timestamp withtimescale to Duration
func TLP2Duration(pts uint64, timescale uint64) time.Duration {
	secs := pts / timescale
	nsecs := (pts * timescale) % 1000000000
	return time.Duration(secs)*time.Second + time.Duration(nsecs)*time.Nanosecond
}

// Round duration to 100s of seconds
func RoundTo(in time.Duration, to time.Duration) time.Duration {
	return in / to * to
}

func Round(in time.Duration) time.Duration {
	return RoundTo(in, time.Millisecond*10)
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

// Return daterange of SegmentTemplate
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

func fetchAndStore(from string, fetch func(*url.URL) error) error {
	mpd := new(mpd.MPD)

	mpdUrl, err := url.Parse(from)
	if err != nil {
		log.Fatal(err)
	}
	resp, err := http.Get(mpdUrl.String())
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()
	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	if doStore {
		// Store the manifest
		filename := outManifestPath + "manifest-" + time.Now().Format(time.TimeOnly) + ".mpd"
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
	if doStore {
		err = onAllSegmentUrls(mpd, mpdUrl, fetch)
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

func main() {

	if len(os.Args) < 2 {
		log.Printf("%s <URL>\n", os.Args[0])
		return
	}
	firstArg := os.Args[1]
	ticker := time.NewTicker(1920 * time.Millisecond)
	if err := os.MkdirAll(outManifestPath, 0777); err != nil {
		log.Fatal("Cannot create directory")
	}

	fetchme = make(chan *url.URL, backbuffer)

	go fetcher()

	for {
		_ = <-ticker.C
		log.Println("Tick")
		fetchAndStore(firstArg, fetchAndStoreUrl) //"https://svc45.cdn-t0.tv.telekom.net/bpk-tv/vox_hd/DASH/manifest.mpd")
	}
}
