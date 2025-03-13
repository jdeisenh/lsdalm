package main

import (
	"flag"
	"net/http"
	"os"
	"time"

	"github.com/jdeisenh/lsdalm/pkg/lsdalm"
	"github.com/rs/zerolog"
)

func main() {

	logger := zerolog.New(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.TimeOnly,
	}).With().Timestamp().Logger()

	url := flag.String("url", "", "Channel URL")
	name := flag.String("name", "default", "Channel ID")
	debug := flag.Bool("debug", false, "set log level to debug")
	dir := flag.String("dumpdir", "", "Directory to store manifests and segments")
	accessMedia := flag.Bool("accessmedia", false, "Access all Media segments")
	//verifyMedia := flag.Bool("verifymedia", false, "Verify all Media segments")
	storeMedia := flag.Bool("storemedia", false, "Store all Media segments")
	workers := flag.Int("workers", 1, "Number of parallel downloads")
	listen := flag.String("replayport", "", "socket:Port for timeshift replay server (e.g. :8080)")

	pollTime := flag.Duration("pollInterval", 5*time.Second, "Poll Interval in milliseconds")
	timeLimit := flag.Duration("timelimit", 0, "Time limit")

	flag.Parse()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	if *url == "" {
		flag.Usage()
		return
	}
	var err error

	var mode lsdalm.FetchMode
	switch {
	// Order is important for precedence
	case *storeMedia:
		mode = lsdalm.MODE_STORE
	//case verifymedia:
	case *accessMedia:
		mode = lsdalm.MODE_ACCESS
	}
	sg, err := lsdalm.NewStreamChecker(*name, *url, *dir, *pollTime, mode, logger, *workers)
	if err != nil {
		logger.Fatal().Err(err).Send()
		return
	}

	// If a port is given, we handle replay requests
	if *listen != "" && mode>=lsdalm.MODE_STORE && *dir!="" {
		var err error
		sr, err := lsdalm.NewStreamReplay(sg.GetDumpDir(), logger)
		if err != nil {
			logger.Fatal().Err(err).Send()
		} else {
			sg.AddFetchCallback(func(path string, at time.Time) {
				sr.AddManifest(path, at)
			})
			// Paths for segments
			http.HandleFunc("/manifest.mpd", sr.Handler)
			http.HandleFunc("/", sr.FileHandler)
			go func() {
				logger.Fatal().Err(http.ListenAndServe(*listen, nil)).Send()
			}()
			logger.Info().Msgf("Starting server listening on %s",*listen)
		}
	}

	if *timeLimit == time.Duration(0) {
		sg.Do()
	} else {
		go sg.Do()
		time.Sleep(*timeLimit)
	}
}
