package lsdalm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Eyevinn/mp4ff/mp4"
	"github.com/jdeisenh/lsdalm/pkg/go-mpd"
	"github.com/rs/zerolog"
)

const (
	ManifestPath     = "manifests"                         // subdirectory name for manifests
	ManifestFormat   = "manifest-2006-01-02T15:04:05Z.mpd" // time format for filenames
	FetchQueueSize   = 50000                               // Max number of outstanding requests in queue
	maxGapLog        = 60 * time.Millisecond               // Warn above this gap length
	dateShortFmt     = "15:04:05.00"                       // Used in logging dates
	SchemeScteXml    = "urn:scte:scte35:2014:xml+bin"      // The one scte scheme we support right now
	DefaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)

// Modes support for checking media segments
const (
	MODE_NOFETCH = iota // Do not fetch media segments
	MODE_ACCESS         // just access by a "HEAD" request, dont get data
	MODE_VERIFY         // get, check timestamps. Not decoded yet
	MODE_STORE          // verification and store plus store
)

// One of above constants
type FetchMode int

// URL and data to verify for a single segment
type SegmentInfo struct {
	Url  *url.URL
	T, D time.Duration
}

type StreamChecker struct {
	name        string           // Name, display only
	sourceUrl   *url.URL         // Manifest source URL
	dumpdir     string           // Directory we write manifests and segments
	manifestDir string           // Subdirectory of above for manifests
	userAgent   string           // Agent used in outgoing http
	updateFreq  time.Duration    // Update freq for manifests
	fetchqueue  chan SegmentInfo // Buffered chan for async media segment requests
	done        chan struct{}    // Chan to stop background goroutines
	ticker      *time.Ticker     // Ticker for timing manifest requests
	fetchMode   FetchMode        // Media segment fetch mode: one of MODE_

	logger zerolog.Logger // Logger instance
	client *http.Client

	haveMap   map[string]bool // Map of requests in queue (to avoid adding them several times)
	haveMutex sync.Mutex      // Mutex protecting the haveMap

	onFetch []func(string, time.Time) // Callbacks to be execute on manifest storage

	// State
	initialPeriod   *mpd.Period // The first period ever fetched, stream format of initial period
	upcomingSplices SpliceList  // SCTE-Markers announced
	lastDate        string

	mpdDiffer *MpdDiffer
}

func NewStreamChecker(name, source, dumpbase string, updateFreq time.Duration, fetchMode FetchMode, logger zerolog.Logger, workers int) (*StreamChecker, error) {

	st := &StreamChecker{
		name:       name,
		updateFreq: updateFreq,
		fetchqueue: make(chan SegmentInfo, FetchQueueSize),
		logger:     logger.With().Str("channel", name).Logger(),
		done:       make(chan struct{}),
		fetchMode:  fetchMode,
		client: &http.Client{
			Transport: &http.Transport{},
		},
		haveMap:   make(map[string]bool),
		userAgent: DefaultUserAgent,
		mpdDiffer: NewMpdDiffer(logger),
	}
	var err error
	st.sourceUrl, err = url.Parse(source)
	if err != nil {
		return nil, err
	}
	// Create a storage directory from dumpdir, name, date and version
	var dumpdir string
	if dumpbase != "" {
		version := ""
		for versioncount := 0; versioncount < 20; versioncount++ {
			dumpdir = path.Join(dumpbase, name+"-"+time.Now().Format("2006-01-02")+version)
			if _, e := os.Stat(dumpdir); e != nil {
				break
			}
			logger.Debug().Msgf("Directory %s exists", dumpdir)
			dumpdir = ""
			version = fmt.Sprintf(".%d", versioncount+1)
		}
	}
	st.dumpdir = dumpdir
	st.manifestDir = path.Join(dumpdir, ManifestPath)

	// Create dump directory if requested
	if dumpdir != "" {
		logger.Info().Msgf("Storing manifests in %s", dumpdir)
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

	// Start workers
	if fetchMode >= MODE_ACCESS {
		for w := 0; w < workers; w++ {
			go st.fetcher()
		}
	}

	st.mpdDiffer.AddOnNewPeriod(func(period *mpd.Period) {
		logger.Info().Msgf("New Period %s starts %s", EmptyIfNil(period.ID), st.mpdDiffer.ast.Add(PeriodStart(period)))
	})

	st.mpdDiffer.AddOnNewEvent(func(event *mpd.Event, scheme string, at time.Time, duration time.Duration) {
		logger.Info().Msgf("New Event %s:%d at %s Duration %s", scheme, event.Id, at, duration)
	})

	return st, nil
}

// GetDumpDir() returns the filesystem path where manifests and segments are stored
func (sc *StreamChecker) GetDumpDir() string {
	return sc.dumpdir
}

// AddFetchCallback adds a callback executed on manifest storage
func (sc *StreamChecker) AddFetchCallback(f func(string, time.Time)) {
	sc.onFetch = append(sc.onFetch, f)
}

func (sc *StreamChecker) fetchAndStoreSegment(url *url.URL, t, d time.Duration) error {
	return sc.fetchAndStoreSegmentS(SegmentInfo{
		Url: url,
		T:   t,
		D:   d,
	})
}

// fetchAndStoreSegment queues an URL for fetching
func (sc *StreamChecker) fetchAndStoreSegmentS(fetchthis SegmentInfo) error {

	// Check what we already have.
	// This does not handle errors, retries, everything else
	sc.haveMutex.Lock()
	if _, ok := sc.haveMap[fetchthis.Url.Path]; ok {
		sc.haveMutex.Unlock()
		// Don't fetch again
		sc.logger.Trace().Msgf("Already in queue%s", fetchthis.Url.Path)
		return nil
	}
	sc.haveMutex.Unlock()

	localpath := path.Join(sc.dumpdir, fetchthis.Url.Path)
	_, err := os.Stat(localpath)
	if err == nil {
		// Assume file exists
		sc.logger.Debug().Msgf("Have file %s", fetchthis.Url.Path)
		return nil
	}
	sc.logger.Debug().Int("QL", len(sc.fetchqueue)).Msg("Queue size")
	// Queue request
	select {
	case sc.fetchqueue <- fetchthis:
		sc.haveMutex.Lock()
		sc.haveMap[fetchthis.Url.Path] = true
		sc.haveMutex.Unlock()
		return nil
	default:
		// That happens in batches, should probably be rate limited
		sc.logger.Error().Msg("Queue full")
		return errors.New("Queue full")
	}
}

// executeFetchAndStore gets a segment and stores it
func (sc *StreamChecker) executeFetchAndStore(fetchme SegmentInfo) error {

	// Create path
	localpath := ""
	if sc.fetchMode >= MODE_STORE {
		localpath = path.Join(sc.dumpdir, fetchme.Url.Path)
		os.MkdirAll(path.Dir(localpath), 0777)
		_, err := os.Stat(localpath)
		if err == nil {
			// Assume file exists
			return nil
		}
	}
	// Decide mode: HEAD for access check, GET for everything else
	mode := "HEAD"
	if sc.fetchMode > MODE_ACCESS {
		mode = "GET"
	}
	req, err := http.NewRequest(mode, fetchme.Url.String(), nil)
	if err != nil {
		sc.logger.Warn().Err(err).Str("url", fetchme.Url.String()).Msg("Create Request")
		// Handle error
		return err
	}

	// Set a (fixed) User Agent, there are sources disciminiating Agents
	req.Header.Set("User-Agent", sc.userAgent)

	resp, err := sc.client.Do(req)
	if err != nil {
		sc.logger.Warn().Err(err).Str("url", fetchme.Url.String()).Msg("Fetch Segment")
		// Handle error
		return err
	}

	if resp.Body != nil {
		defer resp.Body.Close()
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		sc.logger.Error().Err(err).Str("url", fetchme.Url.String()).Msg("Read Segment data")
		return err
	}
	if resp.StatusCode != http.StatusOK {
		sc.logger.Warn().Str("Segment", fetchme.Url.String()).Int("status", resp.StatusCode).Msg("Status")
		return errors.New("Not successful")
	}
	// Check the segment
	if sc.fetchMode >= MODE_VERIFY {
		t, d, err := sc.decodeSegment(body)
		if err != nil {
			sc.logger.Error().Err(err).Msg("Decode media segment")
		} else {
			sc.logger.Trace().Msgf("T:%s D:%s", t, d)
			if fetchme.T != 0 || fetchme.D != 0 {
				if t != fetchme.T || d != fetchme.D {
					sc.logger.Error().Str("url", fetchme.Url.String()).Msg("Mediasegment Offset/Duration Mismatch")
					sc.logger.Error().Str("D", fetchme.D.String()).Str("T", fetchme.T.String()).Msg("Manifest")
					sc.logger.Error().Str("D", d.String()).Str("T", t.String()).Msg("Segment")
				}
			}
		}
	}
	sc.logger.Debug().Str("Segment", fetchme.Url.String()).Msg("Got")
	if sc.dumpdir != "" && sc.fetchMode >= MODE_STORE {
		err = os.WriteFile(localpath, body, 0644)
		if err != nil {
			sc.logger.Error().Err(err).Str("Path", localpath).Msg("Write Segment Data")
			return err
		}
	}
	return nil
}

// decodeSegment will decode the buffer as a mp4, extract the pts and duration metadata and return them
func (sc *StreamChecker) decodeSegment(buf []byte) (offset, duration time.Duration, err error) {
	buffer := bytes.NewReader(buf)
	parsedMp4, lerr := mp4.DecodeFile(buffer, mp4.WithDecodeMode(mp4.DecModeLazyMdat))
	if lerr != nil {
		err = fmt.Errorf("could not parse input file: %w", err)
		return
	}
	for _, box := range parsedMp4.Children {
		switch box.Type() {
		case "sidx":
			//err = box.Info(w, specificBoxLevels, "", "  ")
			sidx := box.(*mp4.SidxBox)
			var ssd uint32
			for _, m := range sidx.SidxRefs {
				ssd += m.SubSegmentDuration
			}
			// Convert to duration. Looks complicated, tries to avoid rounding and overflow
			offset = time.Duration(sidx.EarliestPresentationTime/uint64(sidx.Timescale))*time.Second +
				time.Duration(sidx.EarliestPresentationTime%uint64(sidx.Timescale))*time.Second/time.Duration(sidx.Timescale)
			duration = time.Duration(ssd/sidx.Timescale)*time.Second +
				time.Duration(ssd%sidx.Timescale)*time.Second/time.Duration(sidx.Timescale)
			//sc.logger.Info().Msgf("Start at: %s Duration %s", offset, duration)

		default:
			//sc.logger.Info().Msgf("%s:%d", box.Type(), box.Size())
			//err = box.Info(os.Stderr, "all:1", "", "  ")
		}

	}

	return
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
	if sc.lastDate != "" {
		req.Header.Set("If-Modified-Since", sc.lastDate)
	}

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
	if resp.StatusCode == http.StatusNotModified {
		sc.logger.Debug().Str("url", sc.sourceUrl.String()).Msg("No update")
		return nil

	}
	if resp.StatusCode != http.StatusOK {
		sc.logger.Warn().Int("status", resp.StatusCode).Msg("Manifest fetch")
		return errors.New("Not successful")
	}
	if ct := resp.Header.Get("Content-Type"); strings.HasPrefix(ct, "application/json") || strings.HasPrefix(ct, "text/plain") {
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
	if resp.Header.Get("Date") == sc.lastDate {
		sc.logger.Debug().Str("url", sc.sourceUrl.String()).Msg("No update")
		return nil
	}

	sc.lastDate = resp.Header.Get("Date")

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
		// Call hooks
		for _, e := range sc.onFetch {
			e(filepath, now)
		}
	}

	mpd := new(mpd.MPD)
	err = mpd.Decode(contents)
	if err != nil {
		sc.logger.Error().Err(err).Msgf("Parse Manifest size %d", len(contents))
		sc.logger.Debug().Msg(string(contents))
		return err
	}

	err = sc.OnNewMpd(mpd)
	return err

}

// OnNewMpd is called when a new MPD is published
// (that is different)
func (sc *StreamChecker) OnNewMpd(mpde *mpd.MPD) error {

	if err := sc.mpdDiffer.Update(mpde); err != nil {
		return err
	}
	if err := sc.walkMpd(mpde); err != nil {
		return err
	}
	var err error
	if sc.fetchMode > MODE_NOFETCH {
		err = onAllSegmentUrls(mpde, sc.sourceUrl, sc.fetchAndStoreSegment)
	}
	return err
}

// Iterate through all periods, representation, segmentTimeline and
// write statistics about timing
func (sc *StreamChecker) walkMpd(mpde *mpd.MPD) error {

	now := time.Now()

	if len(mpde.Period) == 0 {
		return errors.New("No periods")
	}
	ast := GetAst(mpde)
	// Log events
	for _, period := range mpde.Period {
		periodStart := ast.Add(PeriodStart(period))
		for _, eventStream := range period.EventStream {
			schemeIdUri := EmptyIfNil(eventStream.SchemeIdUri)
			timescale := ZeroIfNil(eventStream.Timescale)
			pto := ZeroIfNil(eventStream.PresentationTimeOffset)
			if schemeIdUri != SchemeScteXml {
				continue
			}

			//sc.logger.Info().Msgf("EventStream: %s %d %d %+v", schemeIdUri, timescale, pto, eventStream.Event)
			for _, event := range eventStream.Event {
				duration := ZeroIfNil(event.Duration)
				pt := ZeroIfNil(event.PresentationTime)
				// signal, content
				wallSpliceStart := periodStart.Add(TLP2Duration(int64(pt-pto), timescale))
				wallSpliceDuration := TLP2Duration(int64(duration), timescale)
				//sc.logger.Info().Msgf("SCTE35 Id: %d Duration: %s Time %s", event.Id, wallSpliceDuration, shortT(wallSpliceStart))
				// store
				sc.upcomingSplices.AddIfNew(wallSpliceStart, fmt.Sprintf("evid_%d", event.Id))
				sc.upcomingSplices.AddIfNew(wallSpliceStart.Add(wallSpliceDuration), fmt.Sprintf("evid_%d_end", event.Id))
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
				periodStart := ast.Add(PeriodStart(period))
				from, to := SumSegmentTemplate(segTemp, periodStart)
				if from.IsZero() {
					for _, pres := range as.Representations {
						if pres.SegmentTemplate != nil {
							from, to = SumSegmentTemplate(pres.SegmentTemplate, periodStart)
							break
						}
					}
				}
				for _, sp := range sc.upcomingSplices.InRange(from, to) {
					//sc.logger.Info().Msgf("Found splice at %s", shortT(sp))
					walkSegmentTemplateTimings(segTemp, periodStart, func(t time.Time, d time.Duration) {
						if !sp.At.Before(t) && sp.At.Before(t.Add(d)) {
							offset := sp.At.Sub(t)
							if offset > d/2 {
								sc.logger.Debug().Msgf("Early %s to %s Len %s", RoundTo(d-offset, time.Millisecond), shortT(t.Add(d)), d)
							} else if offset != 0 {
								sc.logger.Debug().Msgf("Late  %s to %s Len %s", RoundTo(offset, time.Millisecond), shortT(t), d)
							} else {
								sc.logger.Debug().Msgf("Exactly at %s Len %s", shortT(t), d)
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

				msg += fmt.Sprintf(" %s(%7s)%s", "" /*from.Format(dateShortFmt)*/, Round(to.Sub(from)), "" /*to.Format(dateShortFmt)*/)

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
	sc.fetchqueue <- SegmentInfo{}
	// Sync exit (lame)
	time.Sleep(time.Second)
}

// Goroutine executing media fetches
func (sc *StreamChecker) fetcher() {

	for i := range sc.fetchqueue {
		if i.Url == nil {
			// Exit signal
			break
		}
		sc.haveMutex.Lock()
		delete(sc.haveMap, i.Url.Path)
		sc.haveMutex.Unlock()

		sc.executeFetchAndStore(i)
	}
	sc.logger.Debug().Msg("Close Fetcher")

}
