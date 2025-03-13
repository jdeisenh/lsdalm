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
	"sync"
	"time"

	"github.com/jdeisenh/lsdalm/pkg/go-mpd"
	"github.com/rs/zerolog"
)

const (
	ManifestPath     = "manifests"
	ManifestFormat   = "manifest-2006-01-02T15:04:05Z.mpd"
	FetchQueueSize   = 5000                           // Max number of outstanding requests in queue
	maxGapLog        = 5 * time.Millisecond           // Warn above this gap length
	dateShortFmt     = "15:04:05.00"                  // Used in logging dates
	schemeScteXml    = "urn:scte:scte35:2014:xml+bin" // The one scte scheme we support right now
	DefaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)

// What to do with the media segments
const (
	MODE_NOFETCH = iota
	MODE_ACCESS  // just access
	//MODE_VERIFY  // get, check timestamps
	MODE_STORE // plus store
)

type FetchMode int

type StreamChecker struct {
	name        string
	sourceUrl   *url.URL
	dumpdir     string
	manifestDir string
	userAgent   string
	// Duration for manifests
	updateFreq time.Duration
	fetchqueue chan *url.URL
	done       chan struct{}
	ticker     *time.Ticker
	fetchMode  FetchMode

	logger zerolog.Logger
	client *http.Client

	haveMutex sync.Mutex
	haveMap   map[string]bool

	// State
	initialPeriod   *mpd.Period
	upcomingSplices SpliceList
}

func NewStreamChecker(name, source, dumpdir string, updateFreq time.Duration, fetchMode FetchMode, logger zerolog.Logger, workers int) (*StreamChecker, error) {

	st := &StreamChecker{
		name:        name,
		dumpdir:     dumpdir,
		updateFreq:  updateFreq,
		manifestDir: path.Join(dumpdir, ManifestPath),
		fetchqueue:  make(chan *url.URL, FetchQueueSize),
		logger:      logger.With().Str("channel", name).Logger(),
		done:        make(chan struct{}),
		fetchMode:   fetchMode,
		client: &http.Client{
			Transport: &http.Transport{},
		},
		haveMap:   make(map[string]bool),
		userAgent: DefaultUserAgent,
	}
	var err error
	st.sourceUrl, err = url.Parse(source)
	if err != nil {
		return nil, err
	}
	// Start workers
	for w := 0; w < workers; w++ {
		go st.fetcher()
	}
	if dumpdir != "" {
		// Create directory
		if err := os.MkdirAll(st.manifestDir, 0777); err != nil {
			return nil, errors.New("Cannot create directory")
		}
		// Store Metadata
		m := StorageMeta{
			ManifestUrl: st.sourceUrl.String(),
			HaveMedia:   fetchMode >= MODE_STORE,
		}
		metaJson, err := json.Marshal(m)
		if err != nil {
			return st, err
		}
		err = os.WriteFile(path.Join(st.dumpdir, StorageMetaFileName), metaJson, 0666)
		if err != nil {
			return st, err
		}

	}
	return st, nil
}

// fetchAndStoreSegment queues an URL for fetching
func (sc *StreamChecker) fetchAndStoreSegment(fetchthis *url.URL) error {

	// Check what we already have.
	// This does not handle errors, retries, everything else
	sc.haveMutex.Lock()
	if _, ok := sc.haveMap[fetchthis.Path]; ok {
		sc.haveMutex.Unlock()
		// Don't fetch again
		sc.logger.Trace().Msgf("Already in queue%s", fetchthis.Path)
		return nil
	}
	sc.haveMutex.Unlock()

	localpath := path.Join(sc.dumpdir, fetchthis.Path)
	_, err := os.Stat(localpath)
	if err == nil {
		// Assume file exists
		sc.logger.Debug().Msgf("Have file %s", fetchthis.Path)
		return nil
	}
	sc.logger.Debug().Int("QL", len(sc.fetchqueue)).Msg("Queue size")
	// Queue request
	select {
	case sc.fetchqueue <- fetchthis:
		sc.haveMutex.Lock()
		sc.haveMap[fetchthis.Path] = true
		sc.haveMutex.Unlock()
		return nil
	default:
		sc.logger.Error().Msg("Queue full")
		return errors.New("Queue full")
	}
}

// executeFetchAndStore gets a segment and stores it
func (sc *StreamChecker) executeFetchAndStore(fetchme *url.URL) error {

	// Create path
	localpath := ""
	if sc.fetchMode >= MODE_STORE {
		localpath = path.Join(sc.dumpdir, fetchme.Path)
		os.MkdirAll(path.Dir(localpath), 0777)
		_, err := os.Stat(localpath)
		if err == nil {
			// Assume file exists
			return nil
		}
	}
	mode := "HEAD"
	if sc.fetchMode > MODE_ACCESS {
		mode = "GET"
	}
	req, err := http.NewRequest(mode, fetchme.String(), nil)
	if err != nil {
		sc.logger.Warn().Err(err).Str("url", fetchme.String()).Msg("Create Request")
		// Handle error
		return err
	}

	req.Header.Set("User-Agent", sc.userAgent)

	resp, err := sc.client.Do(req)
	if err != nil {
		sc.logger.Warn().Err(err).Str("url", fetchme.String()).Msg("Fetch Segment")
		// Handle error
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
	if sc.dumpdir != "" && sc.fetchMode >= MODE_STORE {
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
func (sc *StreamChecker) fetchAndStoreManifest() error {

	req, err := http.NewRequest("GET", sc.sourceUrl.String(), nil)
	if err != nil {
		sc.logger.Warn().Err(err).Str("url", sc.sourceUrl.String()).Msg("Create Request")
		// Handle error
		return err
	}

	req.Header.Set("User-Agent", sc.userAgent)

	resp, err := sc.client.Do(req)
	if err != nil {
		sc.logger.Error().Err(err).Str("source", sc.sourceUrl.String()).Msg("Do Manifest Request")
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

	mpd := new(mpd.MPD)
	err = mpd.Decode(contents)
	if err != nil {
		sc.logger.Error().Err(err).Msgf("Parse Manifest size %d", len(contents))
		sc.logger.Debug().Msg(string(contents))
		return err
	}
	err = sc.walkMpd(mpd)
	if sc.fetchMode > MODE_NOFETCH {
		err = onAllSegmentUrls(mpd, sc.sourceUrl, sc.fetchAndStoreSegment)
	}
	return err
}

// ZeroIfNil is a short hand to evaluate a *uint64
func ZeroIfNil(in *uint64) uint64 {
	if in == nil {
		return 0
	}
	return *in
}

// EmptyIfNil is a short hand to evaluate a string pointer
func EmptyIfNil(in *string) string {
	if in == nil {
		return ""
	}
	return *in
}

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
	// Log events
	for _, period := range mpde.Period {
		// Calculate period start
		var start time.Duration
		if period.Start != nil {
			startmed, _ := (*period.Start).ToNanoseconds()
			start = time.Duration(startmed)
		}
		periodStart := ast.Add(start)
		for _, eventStream := range period.EventStream {
			schemeIdUri := EmptyIfNil(eventStream.SchemeIdUri)
			timescale := ZeroIfNil(eventStream.Timescale)
			pto := ZeroIfNil(eventStream.PresentationTimeOffset)
			if schemeIdUri != schemeScteXml {
				continue
			}

			//sc.logger.Info().Msgf("EventStream: %s %d %d %+v", schemeIdUri, timescale, pto, eventStream.Event)
			for _, event := range eventStream.Event {
				duration := ZeroIfNil(event.Duration)
				pt := ZeroIfNil(event.PresentationTime)
				// signal, content
				wallSpliceStart := periodStart.Add(TLP2Duration(int64(pt-pto), timescale))
				wallSpliceDuration := TLP2Duration(int64(duration), timescale)
				sc.logger.Info().Msgf("SCTE35 Id: %d Duration: %s Time %s", event.Id, wallSpliceDuration, shortT(wallSpliceStart))
				// store
				sc.upcomingSplices.AddIfNew(wallSpliceStart)
				sc.upcomingSplices.AddIfNew(wallSpliceStart.Add(wallSpliceDuration))
			}
		}
		_ = periodStart
	}

	// Safe the first period (or should it be last) as a reference
	if sc.initialPeriod == nil {
		for _, period := range mpde.Period {
			if len(period.AdaptationSets) == 0 {
				continue
			}

			if period.AdaptationSets[0].SegmentTemplate != nil {
				sc.initialPeriod = period
				break
			}
		}
	}
	// Walk all AdaptationSets, Periods, and Representations
	// To have one AdaptationSet on one line for all Periods,
	// we use the list of Adaptations from the reference Period
	// and try to match all others to that
	var theGap time.Time
ASloop:
	for asRefId, asRef := range sc.initialPeriod.AdaptationSets {
		msg := ""
		for periodIdx, period := range mpde.Period {
			// Find the adaptationset matching the reference adaptationset
			if len(period.AdaptationSets) == 0 {
				continue
			}
			sc.logger.Trace().Msgf("Searching %s/%s in period %d ", asRef.MimeType, EmptyIfNil(asRef.Codecs), periodIdx)
			// Find an AdaptationSet that matches the AS in the reference period
			// Default is first if not found
			var as *mpd.AdaptationSet
			for asfi, asfinder := range period.AdaptationSets {
				// This logic is imcomplete. If the codec is in the representation, it should match it instead of mismatching to the wrong track
				if asRef.MimeType == asfinder.MimeType && (asRef.Codecs == nil || asfinder.Codecs == nil || *asRef.Codecs == *asfinder.Codecs) {
					sc.logger.Trace().Msgf("Mime-Type %s/%s found in p %d asi %d", asfinder.MimeType, EmptyIfNil(asfinder.Codecs), periodIdx, asfi)
					as = asfinder
					break
				}
			}
			if as == nil {
				sc.logger.Debug().Msgf("Mime-Type %s not found in asi %d", asRef.MimeType, asRefId)
				msg += " [missing] "

			} else {
				segTemp := as.SegmentTemplate
				if segTemp == nil && len(as.Representations) > 0 {
					// Use the first SegTemplate of the Representation
					segTemp = as.Representations[0].SegmentTemplate
				}

				// If there is no segmentTimeline, skip it
				if segTemp == nil || segTemp.SegmentTimeline == nil || len(segTemp.SegmentTimeline.S) == 0 {
					continue ASloop
				}
				// Calculate period start
				var start time.Duration
				if period.Start != nil {
					startmed, _ := (*period.Start).ToNanoseconds()
					start = time.Duration(startmed)
				}
				periodStart := ast.Add(start)
				from, to := sumSegmentTemplate(segTemp, periodStart)
				if from.IsZero() {
					for _, pres := range as.Representations {
						if pres.SegmentTemplate != nil {
							from, to = sumSegmentTemplate(pres.SegmentTemplate, periodStart)
							break
						}
					}
				}
				for _, sp := range sc.upcomingSplices.InRange(from, to) {
					//sc.logger.Info().Msgf("Found splice at %s", shortT(sp))
					walkSegmentTemplateTimings(segTemp, periodStart, func(t time.Time, d time.Duration) {
						if !sp.Before(t) && sp.Before(t.Add(d)) {
							offset := sp.Sub(t)
							if offset > d/2 {
								sc.logger.Info().Msgf("Early %s to %s Len %s", RoundTo(d-offset, time.Millisecond), shortT(t.Add(d)), d)
							} else if offset != 0 {
								sc.logger.Info().Msgf("Late  %s to %s Len %s", RoundTo(offset, time.Millisecond), shortT(t), d)
							} else {
								sc.logger.Info().Msgf("Exactly at %s Len %s", shortT(t), d)
							}
						}
					})
				}
				if periodIdx == 0 {
					// Line start: mimetype+codec, timeshiftBufferDepth
					codecs := ""
					if as.Codecs != nil {
						codecs = "/" + *asRef.Codecs
					}
					msg = fmt.Sprintf("%30s: %8s", asRef.MimeType+codecs, RoundTo(now.Sub(from), time.Second))
				} else if gap := from.Sub(theGap); gap > maxGapLog || gap < -maxGapLog {
					msg += fmt.Sprintf("GAP: %s", Round(gap))
				}

				msg += fmt.Sprintf(" [%s-(%7s)-%s[", from.Format(dateShortFmt), Round(to.Sub(from)), to.Format(dateShortFmt))

				if periodIdx == len(mpde.Period)-1 {
					msg += fmt.Sprintf(" %.1fs", float64(now.Sub(to)/(time.Second/10))/10.0) // Live edge distance
				}
				theGap = to
			}
		}
		sc.logger.Info().Msg(msg) // Write the assembled status line
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

// Done terminates the Streamchecker gracefully
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
		sc.haveMutex.Lock()
		delete(sc.haveMap, i.Path)
		sc.haveMutex.Unlock()

		sc.executeFetchAndStore(i)
	}
	sc.logger.Debug().Msg("Close Fetcher")

}
