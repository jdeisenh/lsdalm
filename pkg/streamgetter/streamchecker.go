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
	ManifestFormat = "manifest-2006-01-02T15:04:05Z.mpd"
	FetchQueueSize = 2000 // Max number of outstanding requests in queue
)

type HistoryElement struct {
	At   time.Time
	Name string
}

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

	logger  zerolog.Logger
	history []HistoryElement

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
		history:     make([]HistoryElement, 0, 1000),
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

func findSub(hist []HistoryElement, want time.Time) *HistoryElement {

	if len(hist) == 0 {
		return nil
	}
	/*
		tf := "15:04:05.00"
		fmt.Printf("%d %s-%s-%s\n", len(hist), hist[0].at.Format(tf), hist[len(hist)/2].at.Format(tf), hist[len(hist)-1].at.Format(tf))
	*/
	if len(hist) == 1 {
		if want.Before(hist[0].At) {
			return nil
		} else {
			return &hist[0]
		}
	}
	pivot := len(hist) / 2
	if want.Before(hist[pivot].At) {
		return findSub(hist[:pivot], want)
	} else {
		return findSub(hist[pivot:], want)
	}
}

// FindHistory returns the newest element from history older than 'want'
func (sc *StreamChecker) FindHistory(want time.Time) *HistoryElement {

	ret := findSub(sc.history, want)
	if ret == nil {
		return nil
	}
	acopy := *ret
	acopy.Name = path.Join(sc.manifestDir, ret.Name)
	return &acopy
}

// crude loop
func (sc *StreamChecker) FindLoopedHistory(now time.Time) (*HistoryElement, time.Duration) {
	first := sc.history[0].At
	last := sc.history[len(sc.history)-1].At
	duration := last.Sub(first)

	offset := now.Sub(first) % duration
	base := now.Sub(first) / duration * duration
	return sc.FindHistory(first.Add(offset)), base

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

func (sc *StreamChecker) AdjustMpd(mpde *mpd.MPD, shift time.Duration) {
	if len(mpde.Period) == 0 {
		return
	}
	for _, period := range mpde.Period {
		if len(period.BaseURL) > 0 {
			period.BaseURL[0].Value = sc.rewriteBaseUrl(period.BaseURL[0].Value, sc.sourceUrl)
		}

		// Shift periodds
		if period.Start != nil {
			startmed, _ := (*period.Start).ToNanoseconds()
			start := time.Duration(startmed)
			*period.Start = DurationToXsdDuration(start + shift)
		}
	}
	return
}

func (sc *StreamChecker) GetLooped(now time.Time) ([]byte, error) {

	sourceElement, shift := sc.FindLoopedHistory(now)
	if sourceElement == nil {
		return []byte{}, errors.New("No source found")
	}
	buf, err := os.ReadFile(sourceElement.Name)
	if err != nil {
		return []byte{}, err
	}
	mpde := new(mpd.MPD)
	if err := mpde.Decode(buf); err != nil {
		return buf, err
	}
	sc.AdjustMpd(mpde, shift) // Manipulate
	afterEncode, err := mpde.Encode()
	if err != nil {
		return buf, err
	}
	return afterEncode, nil
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
		now := time.Now()
		filename := now.UTC().Format(ManifestFormat)
		filepath := path.Join(sc.manifestDir, filename)
		err = os.WriteFile(filepath, contents, 0644)
		if err != nil {
			sc.logger.Error().Err(err).Str("path", filepath).Msg("Write manifest")
			return err
		}
		sc.history = append(sc.history, HistoryElement{now, filename})
	}
	err = mpd.Decode(contents)
	if err != nil {
		sc.logger.Error().Err(err).Msg("Parse Manifest")
		return err
	}
	err = sc.walkMpd(mpd)
	if sc.dumpdir != "" && sc.dumpMedia {
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

// Do fetches and analyzes until 'done' is signaled
func (sc *StreamChecker) Do() error {

	// Do once immediately, return on error
	err := sc.fetchAndStore()
	if err != nil {
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
			sc.fetchAndStore()
		}

	}
	sc.logger.Debug().Msg("Close Ticker")
	return nil
}

func (sc *StreamChecker) Done() {
	close(sc.done)
	sc.fetchqueue <- nil
	if len(sc.history) > 0 {
		first := sc.history[0].At
		last := sc.history[len(sc.history)-1].At
		sc.logger.Info().Msgf("Recorded %d manifests from %s to %s (%s)",
			len(sc.history),
			first.Format(time.TimeOnly),
			last.Format(time.TimeOnly),
			last.Sub(first),
		)
	}

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

func (sc *StreamChecker) Handler(w http.ResponseWriter, r *http.Request) {
	buf, err := sc.GetLooped(time.Now())
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, err.Error())
		return
	}
	w.Header().Add("Content-Type", "application/dash+xml")
	w.Write(buf)
}
