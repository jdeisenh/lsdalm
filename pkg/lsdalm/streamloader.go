package lsdalm

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jdeisenh/lsdalm/pkg/go-mpd"
	"github.com/rs/zerolog"
)

type StreamLoader struct {
	name string // Name, display only
	//sourceUrl   *url.URL       // Manifest source URL
	manifestDir string         // Subdirectory of above for manifests
	userAgent   string         // Agent used in outgoing http
	updateFreq  time.Duration  // Update freq for manifests
	fetchqueue  chan *Session  // Buffered chan for fetch
	done        chan struct{}  // Chan to stop background goroutines
	ticker      *time.Ticker   // Ticker for timing manifest requests
	fetchMode   FetchMode      // Media segment fetch mode: one of MODE_
	logger      zerolog.Logger // Logger instance
	client      *http.Client   // http Client
	lastDate    string         // last Manifest fetch for comparison
	sessions    []*Session     // All active Sessions
}

type Session struct {
	sourceUrl *url.URL // Manifest Source, might change on redirect
	lastDate  string   // last update
}

func NewStreamLoader(name, source string, updateFreq time.Duration, logger zerolog.Logger, sessions int) (*StreamLoader, error) {

	st := &StreamLoader{
		name:       name,
		updateFreq: updateFreq,
		fetchqueue: make(chan *Session, 2*sessions),
		logger:     logger.With().Str("channel", name).Logger(),
		done:       make(chan struct{}),
		client: &http.Client{
			Transport: &http.Transport{},
		},
		userAgent: DefaultUserAgent,
		sessions:  make([]*Session, 0, sessions),
	}
	var err error
	sourceUrl, err := url.Parse(source)
	if err != nil {
		return nil, err
	}
	for i := 0; i < sessions; i++ {
		st.sessions = append(st.sessions, NewSession(sourceUrl))
	}
	for w := 0; w < sessions/10; w++ {
		go st.fetcher()
	}

	return st, nil
}

func NewSession(url *url.URL) *Session {
	ses := &Session{
		sourceUrl: url,
	}
	return ses
}

// fetchManifest gets a manifest from URL, decode the manifest, dump stats, and calls back the action
// callback on all Segments
func (sc *StreamLoader) fetchManifest(ses *Session) error {

	req, err := http.NewRequest("GET", ses.sourceUrl.String(), nil)
	if err != nil {
		sc.logger.Warn().Err(err).Str("url", ses.sourceUrl.String()).Msg("Create Request")
		// Handle error
		return err
	}

	req.Header.Set("User-Agent", sc.userAgent)
	if sc.lastDate != "" {
		req.Header.Set("If-Modified-Since", ses.lastDate)
	}

	resp, err := sc.client.Do(req)
	if err != nil {
		sc.logger.Error().Err(err).Str("source", ses.sourceUrl.String()).Msg("Do Manifest Request")
		return err
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		sc.logger.Error().Err(err).Str("source", ses.sourceUrl.String()).Msg("Get Manifest data")
		return err
	}
	if resp.StatusCode == http.StatusNotModified {
		sc.logger.Debug().Str("url", ses.sourceUrl.String()).Msg("No update")
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
		ses.sourceUrl = sessionUrl
		// Call myself
		return sc.fetchManifest(ses)

	}
	if resp.Header.Get("Date") == ses.lastDate {
		sc.logger.Debug().Str("url", ses.sourceUrl.String()).Msg("No update")
		return nil
	}

	sc.lastDate = resp.Header.Get("Date")

	mpd := new(mpd.MPD)
	err = mpd.Decode(contents)
	if err != nil {
		sc.logger.Error().Err(err).Msgf("Parse Manifest size %d", len(contents))
		sc.logger.Debug().Msg(string(contents))
		return err
	}

	err = sc.OnNewMpd(ses, mpd)
	return err

}

// OnNewMpd is called when a new MPD is published
// (that is different)
func (sc *StreamLoader) OnNewMpd(ses *Session, mpde *mpd.MPD) error {

	sc.logger.Debug().Msgf("New MPD %s", ses.sourceUrl.String())
	// Todo: Parse,
	return nil
}

func (sc *StreamLoader) fetchAllManifest() error {
	//now := time.Now()
	sc.logger.Info().Msgf("Queue size %d", len(sc.fetchqueue))
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
