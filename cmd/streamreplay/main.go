package main

import (
	"flag"
	"net/http"
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

	debug := flag.Bool("debug", false, "set log level to debug")
	dump := flag.String("dumpdir", "", "Directory to dump segments")
	listen := flag.String("listen", ":9080", "Adress/port to listen")
	baseurl := flag.String("baseurl", "", "Base URL for media segments")

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
	sg, err := streamgetter.NewStreamReplay(*dump, *baseurl, logger)
	if err != nil {
		logger.Fatal().Err(err).Send()
		return
	}
	// Todo: Load data
	// Paths for segments
	http.HandleFunc("/manifest.mpd", sg.Handler)
	http.HandleFunc("/", sg.FileHandler)
	logger.Fatal().Err(http.ListenAndServe(*listen, nil)).Send()
}
