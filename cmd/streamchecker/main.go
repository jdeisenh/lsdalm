package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/rs/zerolog"
	"gitlab.com/nowtilus/streamgetter/pkg/streamgetter"
)

var offset time.Duration

var sg *streamgetter.StreamChecker

var logger zerolog.Logger
var dumpdir string

func main() {

	logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	url := flag.String("url", "", "Channel URL")
	name := flag.String("name", "default", "Channel ID")
	debug := flag.Bool("debug", false, "set log level to debug")
	dump := flag.String("dumpdir", "", "Directory to dump segments")
	dumpMedia := flag.Bool("dumpmedia", false, "Copy all Media segments")
	pollTime := flag.Duration("pollInterval", 5*time.Second, "Poll Interval in milliseconds")
	timeLimit := flag.Duration("timelimit", 0, "Time limit")

	flag.Parse()

	dumpdir = *dump

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	if *url == "" {
		flag.Usage()
		return
	}
	var err error
	sg, err = streamgetter.NewStreamChecker(*name, *url, *dump, *pollTime, *dumpMedia, logger)
	if err != nil {
		logger.Fatal().Err(err).Send()
		return
	}
	if *timeLimit == time.Duration(0) {
		sg.Do()
	} else {
		start := time.Now()
		go sg.Do()
		time.Sleep(*timeLimit)
		end := time.Now()
		offset = end.Sub(start)
		sg.Done()
		logger.Info().Msgf("Recorded from %s to %s (%s)", start.Format(time.TimeOnly), end.Format(time.TimeOnly), offset)
		http.HandleFunc("/manifest.mpd", handler)
		logger.Fatal().Err(http.ListenAndServe(":8080", nil)).Send()
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	wantAt := time.Now().Add(-offset).Add(-30 * time.Second)
	found := sg.FindHistory(wantAt)
	if found == nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "it's 404")
		return
	}
	path := path.Join(dumpdir, "manifests", found.Name)
	logger.Info().Str("path", path).Msg("Deliver")
	http.ServeFile(w, r, path)
}
