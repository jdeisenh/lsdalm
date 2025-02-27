package streamgetter

import (
	"encoding/json"
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
	ManifestFormat = "manifest-2006-01-02T15:04:05Z.mpd"
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
	done       chan struct{}
	ticker     *time.Ticker
	dumpMedia  bool

	logger zerolog.Logger

	// statistics
}

func NewStreamChecker(name, source, dumpdir string, updateFreq time.Duration, dumpMedia bool, logger zerolog.Logger) (*StreamChecker, error) {

	st := &StreamChecker{
		name:        name,
		dumpdir:     dumpdir,
		updateFreq:  updateFreq,
		manifestDir: path.Join(dumpdir, ManifestPath),
		fetchqueue:  make(chan *url.URL, FetchQueueSize),
		logger:      logger.With().Str("channel", name).Logger(),
		done:        make(chan struct{}),
		dumpMedia:   dumpMedia,
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

// rewriteBaseUrl will return a URL concatenating upstream with base
func (sc *StreamChecker) rewriteBaseUrl(base string, upstream *url.URL) string {
	// Check for relative URL
	ur, e := url.Parse(base)
	if e != nil {
		sc.logger.Warn().Err(e).Msg("URL parsing")
		return base
	}
	if ur.IsAbs() {
		return base
	}
	return upstream.ResolveReference(ur).String()
}

// fetchAndStoreSegment queues an URL for fetching
func (sc *StreamChecker) fetchAndStoreSegment(fetchthis *url.URL) error {

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
	}
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
	return nil
}

// fetchAndStore gets a manifest from URL, decode the manifest, dump stats, and calls back the action
// callback on all Segments
func (sc *StreamChecker) fetchAndStoreManifest() error {

	resp, err := http.Get(sc.sourceUrl.String())
	if err != nil {
		sc.logger.Error().Err(err).Str("source", sc.sourceUrl.String()).Msg("Get Manifest")
		return err
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		sc.logger.Error().Err(err).Str("source", sc.sourceUrl.String()).Msg("Get Manifest data")
		return err
	}
	if resp.StatusCode != http.StatusOK {
		sc.logger.Warn().Int("status", resp.StatusCode).Msg("Manifest fetch")
		return errors.New("Not successful")
	}
	if sc.dumpdir != "" {
		// Store the manifest
		now := time.Now()
		filename := now.UTC().Format(ManifestFormat)
		filepath := path.Join(sc.manifestDir, filename)
		err = os.WriteFile(filepath, contents, 0644)
		if err != nil {
			sc.logger.Error().Err(err).Str("path", filepath).Msg("Write manifest")
			return err
		}
		//sc.history = append(sc.history, HistoryElement{now, filename})
	}
	if ct := resp.Header.Get("Content-Type"); ct == "application/json" {
		var sessioninfo struct{ MediaUrl string }
		err := json.Unmarshal(contents, &sessioninfo)
		if err != nil {
			sc.logger.Error().Err(err).Msg("parse view route")
			return err
		}
		if sessioninfo.MediaUrl == "" {
			sc.logger.Error().Msg("no MediaURL or empty")
			return fmt.Errorf("No MediaURL in json")
		}
		sessionUrl, err := url.Parse(sessioninfo.MediaUrl)
		if err != nil {
			sc.logger.Error().Err(err).Msg("Session Url not parsable")
			return err
		}
		sc.logger.Info().Str("url", sessioninfo.MediaUrl).Msg("Open session")
		sc.sourceUrl = sessionUrl
		// Call myself
		return sc.fetchAndStoreManifest()

	}

	mpd := new(mpd.MPD)
	err = mpd.Decode(contents)
	if err != nil {
		sc.logger.Error().Err(err).Msgf("Parse Manifest size %d", len(contents))
		sc.logger.Debug().Msg(string(contents))
		return err
	}
	err = sc.walkMpd(mpd)
	if sc.dumpdir != "" && sc.dumpMedia {
		err = onAllSegmentUrls(mpd, sc.sourceUrl, sc.fetchAndStoreSegment)
	}
	return err
}

const dateShortFmt = "15:04:05.00"

// Iterate through all periods, representation, segmentTimeline and
// write statistics about timing
func (sc *StreamChecker) walkMpd(mpde *mpd.MPD) error {

	now := time.Now()

	if len(mpde.Period) == 0 {
		return errors.New("No periods")
	}
	var ast time.Time
	if mpde.AvailabilityStartTime != nil {
		ast = time.Time(*mpde.AvailabilityStartTime)
	}
	// TODO: Choose best period for adaptationSet Reference
	referencePeriod := mpde.Period[0]
	// Choose one with Period-AdaptationSet->SegmentTemplate, which in our case is
	// usually the live period
	for _, period := range mpde.Period {
		if len(period.AdaptationSets) == 0 {
			continue
		}

		if period.AdaptationSets[0].SegmentTemplate != nil {
			referencePeriod = period
			break
		}
	}
	// Walk all AdaptationSets, Periods, and Representations
	// To have one AdaptationSet on one line for all Periods,
	// we use the list of Adaptations from the reference Period
	// and try to match all others to that
	var theGap time.Time
ASloop:
	for _, asRef := range referencePeriod.AdaptationSets {
		msg := ""
		for periodIdx, period := range mpde.Period {
			// Find the adaptationset matching the reference adaptationset
			if len(period.AdaptationSets) == 0 {
				continue
			}
			// Find an AdaptationSet that matches the AS in the reference period
			// Default is first if not found
			var as *mpd.AdaptationSet
			for _, asfinder := range period.AdaptationSets {
				if asRef.MimeType == asfinder.MimeType && (asRef.Codecs == nil || asfinder.Codecs == nil || *asRef.Codecs == *asfinder.Codecs) {
					sc.logger.Trace().Msgf("Mime-Type %s found in asi %d", asfinder.MimeType, asRef)
					as = asfinder
				}
			}
			if as == nil {
				sc.logger.Debug().Msgf("Mime-Type %s not found in asi %d", asRef.MimeType)
				msg += " N/A "
				continue

			}
			// If there is no segmentTimeline, skip it
			if as.SegmentTemplate == nil || as.SegmentTemplate.SegmentTimeline == nil || len(as.SegmentTemplate.SegmentTimeline.S) == 0 {
				continue ASloop
			}

			// Calculate period start
			var start time.Duration
			if period.Start != nil {
				startmed, _ := (*period.Start).ToNanoseconds()
				start = time.Duration(startmed)
			}
			periodStart := ast.Add(start)
			from, to := sumSegmentTemplate(as.SegmentTemplate, periodStart)
			if from.IsZero() {
				for _, pres := range as.Representations {
					if pres.SegmentTemplate != nil {
						from, to = sumSegmentTemplate(pres.SegmentTemplate, periodStart)
						break
					}
				}
			}
			if periodIdx == 0 {
				// Line start: mimetype+codec, timeshiftBufferDepth
				codecs := ""
				if as.Codecs != nil {
					codecs = "/" + *asRef.Codecs
				}
				msg = fmt.Sprintf("%30s: %8s", asRef.MimeType+codecs, RoundTo(now.Sub(from), time.Second))
			} else if gap := from.Sub(theGap); gap > time.Millisecond || gap < -time.Millisecond {
				msg += fmt.Sprintf("GAP: %s", Round(gap))
			}

			msg += fmt.Sprintf(" [%s-(%7s)-%s[", from.Format(dateShortFmt), Round(to.Sub(from)), to.Format(dateShortFmt))

			if periodIdx == len(mpde.Period)-1 {
				msg += fmt.Sprintf(" %.1fs", float64(now.Sub(to)/(time.Second/10))/10.0) // Live edge distance
			}
			theGap = to
		}
		sc.logger.Info().Msg(msg)
	}
	return nil
}

// Do fetches and analyzes until 'done' is signaled
func (sc *StreamChecker) Do() error {

	// Do once immediately, return on error
	err := sc.fetchAndStoreManifest()
	if err != nil {
		sc.logger.Error().Err(err).Msg("Initial fetch")
		return err
	}
	sc.ticker = time.NewTicker(sc.updateFreq)
	defer sc.ticker.Stop()
forloop:
	for {
		select {
		case <-sc.done:
			break forloop
		case <-sc.ticker.C:
			if err := sc.fetchAndStoreManifest(); err != nil {
				sc.logger.Error().Err(err).Msg("Manifest fetch")
			}
		}

	}
	sc.logger.Debug().Msg("Close Ticker")
	return nil
}

func (sc *StreamChecker) Done() {
	close(sc.done)
	sc.fetchqueue <- nil
	// Sync exit (lame)
	time.Sleep(time.Second)
}

// Goroutine
func (sc *StreamChecker) fetcher() {

	for i := range sc.fetchqueue {
		if i == nil {
			// Exit signal
			break
		}
		sc.executeFetchAndStore(i)
	}
	sc.logger.Debug().Msg("Close Fetcher")

}
