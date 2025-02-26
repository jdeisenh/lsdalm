package streamgetter

import (
	"errors"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	//"gitlab.com/nowtilus/eventinjector/pkg/go-mpd"
	"github.com/rs/zerolog"
	"github.com/unki2aut/go-mpd"
)

type HistoryElement struct {
	At   time.Time
	Name string
}

type StreamLooper struct {
	dumpdir     string
	manifestDir string

	logger  zerolog.Logger
	history []HistoryElement

	// statistics
}

func NewStreamLooper(dumpdir string, logger zerolog.Logger) (*StreamLooper, error) {

	st := &StreamLooper{
		dumpdir:     dumpdir,
		manifestDir: path.Join(dumpdir, ManifestPath),
		logger:      logger,
		history:     make([]HistoryElement, 0, 1000),
	}
	st.fillData()
	st.ShowStats()
	return st, nil
}

func (sc *StreamLooper) fillData() error {
	files, err := os.ReadDir(sc.manifestDir)
	if err != nil {
		sc.logger.Error().Err(err).Msg("Scan directories")
		return err
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		sc.logger.Trace().Msg(f.Name())
		time, err := time.Parse(ManifestFormat, f.Name())
		if err != nil {
			sc.logger.Warn().Err(err).Msg("Parse String")
			continue
		}
		sc.history = append(sc.history, HistoryElement{At: time, Name: f.Name()})
	}
	return nil
}

// findSub finds a HistoryElement recursively
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
func (sc *StreamLooper) FindHistory(want time.Time) *HistoryElement {

	ret := findSub(sc.history, want)
	if ret == nil {
		return nil
	}
	acopy := *ret
	acopy.Name = path.Join(sc.manifestDir, ret.Name)
	return &acopy
}

// crude loop
func (sc *StreamLooper) FindLoopedHistory(now time.Time) (*HistoryElement, time.Duration) {
	first := sc.history[0].At
	last := sc.history[len(sc.history)-1].At
	duration := last.Sub(first)

	offset := now.Sub(first) % duration
	base := now.Sub(first) / duration * duration
	return sc.FindHistory(first.Add(offset)), base

}

func (sc *StreamLooper) AdjustMpd(mpde *mpd.MPD, shift time.Duration) {
	if len(mpde.Period) == 0 {
		return
	}
	for _, period := range mpde.Period {
		/*
			Delete?
			if len(period.BaseURL) > 0 {
				period.BaseURL[0].Value = sc.rewriteBaseUrl(period.BaseURL[0].Value, sc.sourceUrl)
			}
		*/

		// Shift periodds
		if period.Start != nil {
			startmed, _ := (*period.Start).ToNanoseconds()
			start := time.Duration(startmed)
			*period.Start = DurationToXsdDuration(start + shift)
		}
	}
	return
}

func (sc *StreamLooper) GetLooped(now time.Time) ([]byte, error) {

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

// Handler serves manifests
func (sc *StreamLooper) Handler(w http.ResponseWriter, r *http.Request) {
	buf, err := sc.GetLooped(time.Now())
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Add("Content-Type", "application/dash+xml")
	w.Write(buf)
}

// FileHanlder serves data
func (sc *StreamLooper) FileHandler(w http.ResponseWriter, r *http.Request) {
	//urlpath := strings.TrimPrefix(r.URL.Path, "/dash/")
	filepath := path.Join(sc.dumpdir, r.URL.Path)
	sc.logger.Trace().Str("path", filepath).Msg("Access")
	http.ServeFile(w, r, filepath)
}

func (sc *StreamLooper) ShowStats() {
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

}

// rewriteBaseUrl will return a URL concatenating upstream with base
func (sc *StreamLooper) rewriteBaseUrl(base string, upstream *url.URL) string {
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
