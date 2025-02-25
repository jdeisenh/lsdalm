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

// TLP2Time Converts timestamp withtimescale to Time
func TLP2Time(pts uint64, timescale uint64) time.Time {
	secs := pts / timescale
	usecs := (pts * timescale) % 1000000000
	return time.Unix(int64(secs), int64(usecs))
}

// Round duration to 100s of seconds
func Round(in time.Duration) time.Duration {
	unit := time.Millisecond * 10
	return in / unit * unit
}

func walkSegmentTemplate(st *mpd.SegmentTemplate, segmentPath *url.URL, repId string, periodIdx, presIdx int, fetch func(*url.URL) error) {

	//fmt.Printf("SegmentTemplate: %+v\n", st)
	timescale := uint64(1)
	if st.Timescale != nil {
		timescale = *st.Timescale
	}
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
	// Only on first representation
	if presIdx != 0 {
		return
	}
	ft, lt := GetTimeRange(stl)
	from := TLP2Time(ft, timescale)
	to := TLP2Time(lt, timescale)
	//fmt.Printf("Mimetype: %d: %s\n", periodIdx, as.MimeType)
	const dateShortFmt = "15:04:05.99"
	/*
		fmt.Printf("Duration: %d: %s %s\n",
			periodIdx,
			Round(to.Sub(from)),
			Round(time.Now().Sub(to))
	*/
	fmt.Printf("%d: %s [%s-%s[ %s\n", periodIdx, Round(to.Sub(from)), from.Format(dateShortFmt), to.Format(dateShortFmt), Round(time.Now().Sub(to)))
	//First time: %s\n", time.Unix(int64(ft), int64(math.Round((ft-math.Trunc(ft))*100)*1e7)))
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
	// Walk all Periods, AdaptationSets and Representations
	for periodIdx, period := range mpd.Period {
		segmentPath := segmentPathFromPeriod(period, mpdUrl)
		for _, as := range period.AdaptationSets {
			for presIdx, pres := range as.Representations {
				if pres.ID == nil {
					continue
				}
				repId := *pres.ID
				if as.SegmentTemplate != nil {
					walkSegmentTemplate(as.SegmentTemplate, segmentPath, repId, periodIdx, presIdx, fetch)
				} else if pres.SegmentTemplate != nil {
					walkSegmentTemplate(pres.SegmentTemplate, segmentPath, repId, periodIdx, presIdx, fetch)
				}
			}
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
