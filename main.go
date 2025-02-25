package main

import (
	"log"
	"os"
	"time"
	//"gitlab.com/nowtilus/eventinjector/pkg/go-mpd"
)

const outManifestPath = "manifests/"

const fetchQueueSize = 2000 // Max number of outstanding requests in queue

func main() {

	if len(os.Args) < 2 {
		log.Printf("%s <URL>\n", os.Args[0])
		return
	}
	firstArg := os.Args[1]

	st, err := NewStreamChecker(firstArg, "dump", 1920*time.Millisecond)
	if err != nil {
		log.Fatal(err)
	}

	st.Do()

}
