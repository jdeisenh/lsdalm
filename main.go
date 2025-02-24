package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/unki2aut/go-mpd"
)

const doStore = true

const outPath = "dump/"
const outManifestPath = outPath + "manifests/"

func fetchAndStoreUrl(fetchme *url.URL) error {
	if !doStore {
		return nil
	}
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

func walkSegmentTemplate(st *mpd.SegmentTemplate, periodUrl *url.URL, repId string, periodIdx, presIdx int, fetch func(*url.URL) error) {

	//fmt.Printf("SegmentTemplate: %+v\n", st)
	timescale := uint64(1)
	if st.Timescale != nil {
		timescale = *st.Timescale
	}
	//fmt.Printf("Timescale: %+v\n", timescale)
	//fmt.Printf("Media: %+v\n", *st.Media)
	media := *st.Media
	// Replace with go format parameters
	media = strings.Replace(media, "$Time$", "%[1]d", 1)
	media = strings.Replace(media, "$RepresentationID$", "%[2]s", 1)
	media = strings.Replace(media, "$Number$", "%[3]d", 1)
	//fmt.Printf("Media: %s\n", media)
	if st.Initialization != nil {
		init := strings.Replace(*st.Initialization, "$RepresentationID$", repId, 1)
		//fmt.Printf("Init: %s\n", init)
		fetch(periodUrl.JoinPath(init))
	}
	// Walk the Segment
	if st.SegmentTimeline == nil {
		fmt.Println("SegmentTemplate without Timeline not suported")
		return
	}
	stl := st.SegmentTimeline
	var lasttime, firsttime uint64
	number := 0
	if st.StartNumber != nil {
		number = int(*st.StartNumber)
	}
	for _, s := range stl.S {
		var repeat int64
		if s.T != nil {
			if firsttime == 0 {
				firsttime = *s.T
			}
			lasttime = *s.T
		}

		if s.R != nil {
			repeat = *s.R
		}

		for r := int64(0); r <= repeat; r++ {
			ppa := fmt.Sprintf(media, lasttime, repId, number)
			//fmt.Printf("Path %s:%s\n", media, ppa)
			fullUrl := periodUrl.JoinPath(ppa)
			fetch(fullUrl)
			lasttime += s.D
			number++
		}
	}
	if presIdx == 0 {
		// Only on first representation
		ft := float64(lasttime) / float64(timescale)
		to := time.Unix(int64(ft), int64(math.Round((ft-math.Trunc(ft))*100)*1e7))
		ft = float64(firsttime) / float64(timescale)
		from := time.Unix(int64(ft), int64(math.Round((ft-math.Trunc(ft))*100)*1e7))
		//fmt.Printf("Mimetype: %d: %s\n", periodIdx, as.MimeType)
		fmt.Printf("Duration: %d: %s %s\n",
			periodIdx,
			to.Sub(from)/time.Millisecond*time.Millisecond,
			time.Now().Sub(to)/time.Millisecond*time.Millisecond)
		//First time: %s\n", time.Unix(int64(ft), int64(math.Round((ft-math.Trunc(ft))*100)*1e7)))
	}
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
	for periodIdx, period := range mpd.Period {
		//fmt.Printf("BaseURL: %s\n", period.BaseURL[0].Value)
		base := period.BaseURL[0].Value
		baseurl, err := url.Parse(base)
		if err != nil {
			log.Println(err)
		}
		var periodUrl *url.URL
		if baseurl.IsAbs() {
			periodUrl = baseurl
		} else {
			periodUrl = mpdUrl
			joined, err := url.JoinPath(path.Dir(periodUrl.Path), base)
			if err != nil {
				log.Fatal(err)
			}
			periodUrl.Path = joined
		}
		for _, as := range period.AdaptationSets {
			//fmt.Printf(": %+v\n", as)
			for presIdx, pres := range as.Representations {
				if pres.ID != nil {
					repId := *pres.ID
					//fmt.Printf("Representation: %+v", pres.ID)
					if as.SegmentTemplate != nil {
						walkSegmentTemplate(as.SegmentTemplate, periodUrl, repId, periodIdx, presIdx, fetch)
					} else if pres.SegmentTemplate != nil {
						walkSegmentTemplate(pres.SegmentTemplate, periodUrl, repId, periodIdx, presIdx, fetch)
					}
				}
			}
		}
		//}
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

	for {
		_ = <-ticker.C
		log.Println("Tick")
		fetchAndStore(firstArg, fetchAndStoreUrl) //"https://svc45.cdn-t0.tv.telekom.net/bpk-tv/vox_hd/DASH/manifest.mpd")
	}
}
