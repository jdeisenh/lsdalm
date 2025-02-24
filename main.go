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

func fetch(fetchme *url.URL) error {
	return nil
	localpath := path.Join(".", fetchme.Path)
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
		log.Printf("Got: %s %s", fetchme.String(), localpath)
		err = os.WriteFile(localpath, body, 0644)
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}

func walkSegmentTimeline(stl *mpd.SegmentTimeline, periodUrl *url.URL, repId, media string, timescale uint64, periodIdx, presIdx int) {
	var lasttime, firsttime uint64
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
			//fmt.Printf("Time %d\n", time)
			fullUrl := periodUrl.JoinPath(fmt.Sprintf(media, lasttime, repId))
			fetch(fullUrl)
			lasttime += s.D
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

func fetchAndStore(from string) error {
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
	filename := "manifest-" + time.Now().Format(time.TimeOnly) + ".mpd"
	err = os.WriteFile(filename, contents, 0644)
	if err != nil {
		log.Println(err)
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
					if as.SegmentTemplate == nil {
						continue
					}
					st := as.SegmentTemplate
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
					//fmt.Printf("Media: %s\n", media)
					if st.Initialization != nil {
						init := strings.Replace(*st.Initialization, "$RepresentationID$", repId, 1)
						//fmt.Printf("Init: %s\n", init)
						fetch(periodUrl.JoinPath(init))
					}
					if st.SegmentTimeline != nil {
						walkSegmentTimeline(st.SegmentTimeline, periodUrl, repId, media, timescale, periodIdx, presIdx)
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
	for {
		_ = <-ticker.C
		log.Println("Tick")
		fetchAndStore(firstArg) //"https://svc45.cdn-t0.tv.telekom.net/bpk-tv/vox_hd/DASH/manifest.mpd")
	}
}
