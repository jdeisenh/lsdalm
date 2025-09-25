package lsdalm

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jdeisenh/lsdalm/pkg/go-mpd"
	"github.com/rs/zerolog"
)

const (
	requestTimeout = 5 * time.Second
)

type StreamLoader struct {
	name string // Name, display only
	//sourceUrl   *url.URL       // Manifest source URL
	manifestDir     string         // Subdirectory of above for manifests
	userAgent       string         // Agent used in outgoing http
	updateFreq      time.Duration  // Update freq for manifests
	fetchqueue      chan *Session  // Buffered chan for fetch
	restartsPerHour float64        // Percentage (0-1) of sessions that restart per hour
	done            chan struct{}  // Chan to stop background goroutines
	ticker          *time.Ticker   // Ticker for timing manifest requests
	fetchMode       FetchMode      // Media segment fetch mode: one of MODE_
	logger          zerolog.Logger // Logger instance
	client          *http.Client   // http Client
	lastDate        string         // last Manifest fetch for comparison
	numSessions     int            // number of sessions to start
	sessionMutex    sync.RWMutex   // protects session
	sessions        []*Session     // All active Sessions
	singleconn      bool
	sourceUrl       *url.URL
}

type Session struct {
	sourceUrl  *url.URL     // Manifest Source
	sessionUrl *url.URL     // Session, if one was opened
	startDate  time.Time    // start of session
	lastDate   string       // last update
	client     *http.Client // for maxconn tests
}

// NewStreamLoader starts a new StreamLoader
// with 'name' used for logging
// on 'source' url
// polling the stream every 'updateFreq'
// keeping 'sessions' sessions open
// restart 'restartsPerHour' percentage per Hour.
func NewStreamLoader(name, source string, updateFreq time.Duration, logger zerolog.Logger, numSessions int, restartsPerHour float64, singleconn bool) (*StreamLoader, error) {

	st := &StreamLoader{
		name:            name,
		updateFreq:      updateFreq,
		restartsPerHour: restartsPerHour,
		fetchqueue:      make(chan *Session, 2*numSessions),
		logger:          logger.With().Str("channel", name).Logger(),
		done:            make(chan struct{}),
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10000,
				Timeout:             requestTimeout,
				Transport: &http.Transport{
					Dial: func(network, addr string) (net.Conn, error) {
						return net.DialTimeout(network, addr, dialTimeout)
					},
				},
			},
		},
		userAgent:   DefaultUserAgent,
		sessions:    make([]*Session, 0, numSessions),
		singleconn:  singleconn,
		numSessions: numSessions,
	}
	var err error
	st.sourceUrl, err = url.Parse(source)
	if err != nil {
		return nil, err
	}
	for w := 0; w < max(numSessions/10, 1); w++ {
		go st.fetcher()
	}
	go st.AddLoad()
	return st, nil
}

func (sl *StreamLoader) AddLoad() {
	for i := 0; i < sl.numSessions; i++ {
		sl.sessionMutex.Lock()
		sl.sessions = append(sl.sessions, NewSession(sl.sourceUrl, sl.singleconn))
		sl.sessionMutex.Unlock()
		time.Sleep(10 * time.Millisecond) // 100 Hz
	}
}

func NewSession(url *url.URL, singleconnection bool) *Session {
	ses := &Session{
		sourceUrl: url,
	}
	// Give each session its own transport if selected
	if singleconnection {
		ses.client = &http.Client{Transport: &http.Transport{}}
	}
	return ses
}

// fetchManifest gets a manifest from URL, decode the manifest, dump stats, and calls back the action
// callback on all Segments
func (sc *StreamLoader) fetchManifest(ses *Session) error {

	surl := ses.sourceUrl
	// Use SessionUrl if one exists
	if ses.sessionUrl != nil {
		surl = ses.sessionUrl
	}

	req, err := http.NewRequest("GET", surl.String(), nil)
	if err != nil {
		sc.logger.Warn().Err(err).Str("url", surl.String()).Msg("Create Request")
		// Handle error
		return err
	}

	req.Header.Set("User-Agent", sc.userAgent)
	// We won't be able to decode them, but try to keep transfer sizes low
	//req.Header.Set("Accept-Encoding", "zstd,gzip,deflate,compress,br")
	if sc.lastDate != "" {
		req.Header.Set("If-Modified-Since", ses.lastDate)
	}

	client := sc.client
	if ses.client != nil {
		client = ses.client
	}

	resp, err := client.Do(req)
	if err != nil {
		sc.logger.Error().Err(err).Str("source", surl.String()).Msg("Do Manifest Request")
		return err
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		sc.logger.Error().Err(err).Str("source", surl.String()).Msg("Get Manifest data")
		return err
	}
	if resp.StatusCode == http.StatusNotModified {
		sc.logger.Debug().Str("url", surl.String()).Msg("No update")
		return nil

	}
	if resp.StatusCode != http.StatusOK {
		sc.logger.Warn().Int("status", resp.StatusCode).Str("url", surl.String()).Msg("Manifest fetch")
		return errors.New("Not successful")
	}
	// If our sourced returns json, we assume it wants to open a session
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
		ses.sessionUrl = sessionUrl
		ses.startDate = time.Now()
		// Call myself
		//return sc.fetchManifest(ses)
		return nil

	}
	if resp.Header.Get("Date") == ses.lastDate {
		sc.logger.Debug().Str("url", ses.sourceUrl.String()).Msg("No update")
		return nil
	}

	sc.lastDate = resp.Header.Get("Date")

	return err

}

// OnNewMpd is called when a new MPD is published
// (that is different)
func (sc *StreamLoader) OnNewMpd(ses *Session, mpde *mpd.MPD) error {

	sc.logger.Debug().Msgf("New MPD %s", ses.sourceUrl.String())
	// Todo: Parse,
	return nil
}

// closeSessions will determine how many sessions to close to make the rate, and execute it
func (sc *StreamLoader) closeSessions() {

	// Calculat the rate

	killQuota := sc.restartsPerHour / 3600 * float64(sc.updateFreq) / float64(time.Second)
	toKill := int(math.Round(float64(len(sc.sessions))*killQuota + rand.Float64() - 0.5)) // Add dithering
	sc.logger.Debug().Msgf("Rate %f per sec, %f per iteration, %d sessions -> close %d",
		sc.restartsPerHour/3600,
		killQuota,
		len(sc.sessions),
		toKill,
	)
	if toKill == 0 {
		return
	}

	// Randomize order
	sc.sessionMutex.RLock()
	perm := rand.Perm(len(sc.sessions))
	defer sc.sessionMutex.RUnlock()
	for _, i := range perm {
		if toKill == 0 {
			break
		}
		toKill--
		sc.CloseSession(sc.sessions[i])
	}

}

func (sc *StreamLoader) fetchAllManifest() error {
	//now := time.Now()
	sc.logger.Info().Msgf("Queue size %d", len(sc.fetchqueue))

	sc.closeSessions()

	sc.sessionMutex.RLock()
	defer sc.sessionMutex.RUnlock()
	for _, ses := range sc.sessions {
		select {
		case sc.fetchqueue <- ses:
		default:
			// That happens in batches, should probably be rate limited
			sc.logger.Error().Msg("Queue full")
			return errors.New("Queue full")
		}

	}
	return nil
}

// Do fetches and analyzes until 'done' is signaled
func (sc *StreamLoader) Do() error {

	// Do once immediately, return on error
	err := sc.fetchAllManifest()
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
			if err := sc.fetchAllManifest(); err != nil {
				sc.logger.Error().Err(err).Msg("Manifest fetch")
			}
		}

	}
	sc.logger.Debug().Msg("Close Ticker")
	return nil
}

// Goroutine executing media fetches
func (sc *StreamLoader) fetcher() {

	for ses := range sc.fetchqueue {
		if ses == nil {
			// Exit signal
			break
		}
		sc.fetchManifest(ses)
	}
	sc.logger.Debug().Msg("Close Fetcher")

}

func (sc *StreamLoader) CloseSession(ses *Session) {
	if ses.sessionUrl == nil {
		return
	}
	sc.logger.Info().Msgf("Closing session %s after %s", ses.sessionUrl, time.Since(ses.startDate))
	ses.sessionUrl = nil
}
