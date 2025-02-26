package streamgetter

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	//"gitlab.com/nowtilus/eventinjector/pkg/go-mpd"
	"github.com/rs/zerolog"
	"github.com/unki2aut/go-mpd"
)

const (
	ManifestPath   = "manifests"
	FetchQueueSize = 2000 // Max number of outstanding requests in queue
)

type StreamChecker struct {
	name        string
	sourceUrl   *url.URL
	dumpdir     string
	manifestDir string
	// Duration for manifests
	updateFreq time.Duration
	fetchqueue chan *url.URL

	logger zerolog.Logger

	// statistics
}

func NewStreamChecker(name, source, dumpdir string, updateFreq time.Duration, logger zerolog.Logger) (*StreamChecker, error) {

	st := &StreamChecker{
		name:        name,
		dumpdir:     dumpdir,
		updateFreq:  updateFreq,
		manifestDir: path.Join(dumpdir, ManifestPath),
		fetchqueue:  make(chan *url.URL, FetchQueueSize),
		logger:      logger.With().Str("channel", name).Logger(),
	}
	var err error
	st.sourceUrl, err = url.Parse(source)
	if err != nil {
		return nil, err
	}
	go st.fetcher()
	if err := os.MkdirAll(st.manifestDir, 0777); err != nil {
		return nil, errors.New("Cannot create directory")
	}
	return st, nil
}

// fetchAndStoreUrl queues an URL for fetching
func (sc *StreamChecker) fetchAndStoreUrl(fetchthis *url.URL) error {

	localpath := path.Join(sc.dumpdir, fetchthis.Path)
	_, err := os.Stat(localpath)
	if err == nil {
		// Assume file exists
		return nil
	}
	// Queue request
	select {
	case sc.fetchqueue <- fetchthis:
		return nil
	default:
		sc.logger.Error().Msg("Queue full")
		return errors.New("Queue full")
	}
}

// executeFetchAndStore gets a segment and stores it
func (sc *StreamChecker) executeFetchAndStore(fetchme *url.URL) error {

	localpath := path.Join(sc.dumpdir, fetchme.Path)
	os.MkdirAll(path.Dir(localpath), 0777)
	_, err := os.Stat(localpath)
	if err == nil {
		// Assume file exists
		return nil
	}
	resp, err := http.Get(fetchme.String())
	if err != nil {
		sc.logger.Warn().Err(err).Str("url", fetchme.String()).Msg("Fetch Segment")
		return err
	} else {
		if resp.Body != nil {
			defer resp.Body.Close()
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			sc.logger.Error().Err(err).Str("url", fetchme.String()).Msg("Read Segment data")
			return err
		}
		if resp.StatusCode != http.StatusOK {
			sc.logger.Warn().Str("Segment", fetchme.String()).Int("status", resp.StatusCode).Msg("Status")
			return errors.New("Not successful")
		}
		sc.logger.Debug().Str("Segment", fetchme.String()).Msg("Got")
		err = os.WriteFile(localpath, body, 0644)
		if err != nil {
			sc.logger.Error().Err(err).Str("Path", localpath).Msg("Write Segment Data")
			return err
		}
	}
	return nil
}

// fetchAndStore gets a manifest from URL, decode the manifest, dump stats, and calls back the action
// callback on all Segments
func (sc *StreamChecker) fetchAndStore() error {
	mpd := new(mpd.MPD)

	resp, err := http.Get(sc.sourceUrl.String())
	if err != nil {
		sc.logger.Error().Err(err).Str("source", sc.sourceUrl.String()).Msg("Get Manifest")
		return err
	}
	defer resp.Body.Close()
	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		sc.logger.Error().Err(err).Str("source", sc.sourceUrl.String()).Msg("Get Manifest data")
		return err
	}
	if sc.dumpdir != "" {
		// Store the manifest
		filename := path.Join(sc.manifestDir, "manifest-"+time.Now().Format(time.TimeOnly)+".mpd")
		err = os.WriteFile(filename, contents, 0644)
		if err != nil {
			sc.logger.Error().Err(err).Str("path", filename).Msg("Write segment")
			return err
		}
	}
	err = mpd.Decode(contents)
	if err != nil {
		sc.logger.Error().Err(err).Msg("Parse Manifest")
		return err
	}
	err = sc.walkMpd(mpd)
	if sc.dumpdir != "" {
		err = onAllSegmentUrls(mpd, sc.sourceUrl, sc.fetchAndStoreUrl)
	}
	return err
}

const dateShortFmt = "15:04:05.00"

// Iterate through all periods, representation, segmentTimeline and
// write statistics about timing
func (sc *StreamChecker) walkMpd(mpd *mpd.MPD) error {

	now := time.Now()

	if len(mpd.Period) == 0 {
		return errors.New("No periods")
	}
	var ast time.Time
	if mpd.AvailabilityStartTime != nil {
		ast = time.Time(*mpd.AvailabilityStartTime)
	}
	// TODO: Choose best period for adaptationSet Reference
	referencePeriod := mpd.Period[0]
	// Choose one with Period-AdaptationSet->SegmentTemplate, which in our case is
	// usually the live period
	for _, period := range mpd.Period {
		if len(period.AdaptationSets) == 0 {
			continue
		}

		if period.AdaptationSets[0].SegmentTemplate != nil {
			referencePeriod = period
			break
		}
	}
	// Walk all Periods, AdaptationSets and Representations
	var theGap time.Time
	for _, as := range referencePeriod.AdaptationSets {
		msg := ""
		for periodIdx, period := range mpd.Period {
			// Find the adaptationset matching the reference adaptationset
			if len(period.AdaptationSets) == 0 {
				continue
			}
			var asr = period.AdaptationSets[0]
			for _, as := range period.AdaptationSets {
				if asr.MimeType == as.MimeType {
					asr = as
					break
				}
			}

			// Calculate period start
			var start time.Duration
			if period.Start != nil {
				startmed, _ := (*period.Start).ToNanoseconds()
				start = time.Duration(startmed)
			}
			periodStart := ast.Add(start)
			from, to := sumSegmentTemplate(asr.SegmentTemplate, periodStart)
			if from.IsZero() {
				for _, pres := range asr.Representations {
					if pres.SegmentTemplate != nil {
						from, to = sumSegmentTemplate(pres.SegmentTemplate, periodStart)
						break
					}
				}
			}
			if periodIdx == 0 {
				// Line start: mimetype, timeshiftBufferDepth
				msg = fmt.Sprintf("%10s: %8s", as.MimeType, RoundTo(now.Sub(from), time.Second))
			} else if gap := from.Sub(theGap); gap > time.Millisecond || gap < -time.Millisecond {
				msg += fmt.Sprintf("GAP: %s", Round(gap))
			}

			msg += fmt.Sprintf(" [%s-%s[", from.Format(dateShortFmt), to.Format(dateShortFmt))

			if periodIdx == len(mpd.Period)-1 {
				msg += fmt.Sprintf(" %s", RoundTo(now.Sub(to), time.Second)) // Live edge distance
			}
			theGap = to
		}
		sc.logger.Info().Msg(msg)
	}
	return nil
}

// Do fetches and analyzes forever
func (sc *StreamChecker) Do() error {

	// Do once immediately, return on error
	err := sc.fetchAndStore()
	if err != nil {
		return err
	}
	ticker := time.NewTicker(sc.updateFreq)
	for {
		_ = <-ticker.C
		sc.fetchAndStore()

	}

}

// Goroutine
func (sc *StreamChecker) fetcher() {

	for i := range sc.fetchqueue {
		sc.executeFetchAndStore(i)
	}

}
