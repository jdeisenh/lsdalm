package main

import (
	"flag"
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
	sessions := flag.Int("sessions", 1, "Number of sesssions in parallel")

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

	sg, err := lsdalm.NewStreamLoader(*name, *url, *pollTime, logger, *sessions)
	if err != nil {
		logger.Fatal().Err(err).Send()
		return
	}

	if *timeLimit == time.Duration(0) {
		sg.Do()
	} else {
		go sg.Do()
		time.Sleep(*timeLimit)
	}
}
