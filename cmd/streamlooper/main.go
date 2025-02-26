package main

import (
	"flag"
	"net/http"
	"os"

	"github.com/rs/zerolog"
	"gitlab.com/nowtilus/streamgetter/pkg/streamgetter"
)

func main() {

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	debug := flag.Bool("debug", false, "set log level to debug")
	dump := flag.String("dumpdir", "", "Directory to dump segments")

	flag.Parse()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	if *dump == "" {
		flag.Usage()
		return
	}
	var err error
	sg, err := streamgetter.NewStreamLooper(*dump, logger)
	if err != nil {
		logger.Fatal().Err(err).Send()
		return
	}
	// Todo: Load data
	// Paths for segments
	http.HandleFunc("/manifest.mpd", sg.Handler)
	http.HandleFunc("/dash/", sg.FileHandler)
	logger.Fatal().Err(http.ListenAndServe(":8080", nil)).Send()
}
