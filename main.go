package main

import (
	"flag"
	"os"
	"time"

	"github.com/rs/zerolog"
	//"gitlab.com/nowtilus/eventinjector/pkg/go-mpd"
)

const (
	defaultPollTime = 1920 * time.Millisecond
)

func main() {

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	url := flag.String("url", "", "Channel URL")
	name := flag.String("name", "default", "Channel ID")
	debug := flag.Bool("debug", false, "set log level to debug")
	dump := flag.String("dumpdir", "", "Directory to dump segments")
	pollTime := flag.Duration("pollInterval", 5*time.Second, "Poll Interval in milliseconds")

	flag.Parse()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	if *url == "" {
		flag.Usage()
		return
	}

	st, err := NewStreamChecker(*name, *url, *dump, *pollTime, logger)
	if err != nil {
		logger.Fatal().Err(err).Send()
		return
	}

	st.Do()

}
