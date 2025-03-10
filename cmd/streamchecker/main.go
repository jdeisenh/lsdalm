package main

import (
	"flag"
	"os"
	"time"

	streamgetter "github.com/jdeisenh/lsdalm/pkg/lsdalm"
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

	var mode streamgetter.FetchMode
	switch {
	// Order is important for precedence
	case *storeMedia:
		mode = streamgetter.MODE_STORE
	//case verifymedia:
	case *accessMedia:
		mode = streamgetter.MODE_ACCESS
	}
	sg, err := streamgetter.NewStreamChecker(*name, *url, *dir, *pollTime, mode, logger)
	if err != nil {
		logger.Fatal().Err(err).Send()
		return
	}
	if *timeLimit == time.Duration(0) {
		sg.Do()
	} else {
		go sg.Do()
		time.Sleep(*timeLimit)
		sg.Done()
	}
}
