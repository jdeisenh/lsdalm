package main

import (
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

func fetch(fetchme *url.URL) error {
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
	for _, period := range mpd.Period {
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
			//fmt.Printf("Mimetype: %s\n", as.MimeType)
			for _, pres := range as.Representations {
				if pres.ID != nil {
					repId := *pres.ID
					//fmt.Printf("Representation: %+v", pres.ID)
					if as.SegmentTemplate != nil {
						st := as.SegmentTemplate
						//fmt.Printf("SegmentTemplate: %+v\n", st)
						//fmt.Printf("Timescale: %+v\n", *st.Timescale)
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
							var time uint64
							for _, s := range st.SegmentTimeline.S {
								var repeat int64
								if s.T != nil {
									time = *s.T
								}

								if s.R != nil {
									repeat = *s.R
								}

								for r := int64(0); r <= repeat; r++ {
									//fmt.Printf("Time %d\n", time)
									fullUrl := periodUrl.JoinPath(fmt.Sprintf(media, time, repId))
									fetch(fullUrl)
									time += s.D
								}
							}
						}
					}
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
	for {
		_ = <-ticker.C
		log.Println("Tick")
		fetchAndStore(firstArg) //"https://svc45.cdn-t0.tv.telekom.net/bpk-tv/vox_hd/DASH/manifest.mpd")
	}
}
