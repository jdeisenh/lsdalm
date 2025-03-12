package streamgetter

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/jdeisenh/lsdalm/pkg/go-mpd"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type StreamReplay struct {
	dumpdir     string
	manifestDir string
	baseurl     *url.URL

	logger                   zerolog.Logger
	history                  []HistoryElement
	historyStart, historyEnd time.Time
}

func NewStreamReplay(dumpdir, baseurl string, logger zerolog.Logger) (*StreamReplay, error) {

	st := &StreamReplay{
		dumpdir:     dumpdir,
		manifestDir: path.Join(dumpdir, ManifestPath),
		logger:      logger,
		history:     make([]HistoryElement, 0, 1000),
	}
	var err error
	st.baseurl, err = url.Parse(baseurl)
	if err != nil {
		return nil, err
	}
	st.fillData()
	if len(st.history) < 10 {
		return nil, fmt.Errorf("Not enough manifests")
	}
	st.ShowStats()
	return st, nil
}

// fillData reads add the manifests
func (sc *StreamReplay) fillData() error {
	files, err := os.ReadDir(sc.manifestDir)
	if err != nil {
		sc.logger.Error().Err(err).Msg("Scan directories")
		return err
	}
	var lasttime time.Time
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		sc.logger.Trace().Msg(f.Name())
		ctime, err := time.Parse(ManifestFormat, f.Name())
		if err != nil {
			sc.logger.Warn().Err(err).Msg("Parse String")
			continue
		}
		if !lasttime.IsZero() && (ctime.Sub(lasttime) > time.Second*30) {
			sc.logger.Error().Msgf("Too large a gap between %s and %s, dropping",
				lasttime.Format(time.TimeOnly), ctime.Format(time.TimeOnly))
			sc.history = sc.history[:0]
		}

		sc.history = append(sc.history, HistoryElement{At: ctime, Filename: f.Name()})
	}
	// Find last pts in both first and last manifest
	fs := sc.history[0].At
	first, err := sc.loadHistoricMpd(fs)
	if err != nil {
		sc.logger.Warn().Err(err).Msg("Cannot load")
	}
	ff, fl, _ := sc.getPtsRange(first, "video/mp4")

	ls := sc.history[len(sc.history)-1].At
	last, err := sc.loadHistoricMpd(ls)
	if err != nil {
		sc.logger.Warn().Err(err).Msg("Cannot load")
	}
	lf, ll, _ := sc.getPtsRange(last, "video/mp4")

	sc.logger.Debug().Msgf("Start %s %s-%s", shortT(fs), Round(fs.Sub(ff)), Round(fs.Sub(fl)))
	sc.logger.Debug().Msgf("End %s %s-%s", shortT(ls), Round(ls.Sub(lf)), Round(ls.Sub(ll)))
	sc.historyStart = fl
	sc.historyEnd = ll

	return nil
}

// FindHistory returns the newest element from history older than 'want'
func (sc *StreamReplay) FindHistory(want time.Time) *HistoryElement {

	ret := findSub(sc.history, want)
	if ret == nil {
		return nil
	}
	acopy := *ret
	acopy.Filename = path.Join(sc.manifestDir, ret.Filename)
	return &acopy
}

func (sc *StreamReplay) getRecordingRange() (from, to time.Time) {
	from = sc.history[0].At
	to = sc.history[len(sc.history)-1].At

	return
}

// Return loop metadata
// for the position at, return offset (to recording), timeshift und loop duration
func (sc *StreamReplay) getLoopMeta(at, now time.Time, requestDuration time.Duration) (offset, shift, duration time.Duration, start time.Time) {

	// Calculate the offset in the recording buffer and the timeshift (added to timestamps)
	// Invariants:
	// start+offset+shift=now => shift=now-offset-start
	// We play at at%duration => (start+offset)%duration == at%duration => offset=(at-start)%duration

	// Data from history buffer
	// This is inexact, the last might not have all Segments downloaded
	start, _ = sc.getRecordingRange()
	duration = sc.historyEnd.Sub(sc.historyStart)

	offset = at.Sub(start) % duration
	shift = now.Add(-offset).Sub(start)
	//sc.logger.Info().Msgf("%s %s", at, start)
	return
}

// baseToPath converts a Base URL to a absolute local path
func baseToPath(base, prefix string) string {
	if prefix == "" {
		// No change
		return base
	}
	var baseurl *url.URL
	var err error
	baseurl, err = url.Parse(base)
	if err != nil {
		log.Warn().Err(err).Msg("Parse URL")
		return base
	}
	if strings.HasPrefix(baseurl.Path, "/") {
		// Path only
		return baseurl.Path
	} else {
		return path.Join(prefix, baseurl.Path)
	}
}

// AdjustMpd adds a time offset to each Period in the Manifest, shifting the PresentationTime
func (sc *StreamReplay) AdjustMpd(mpde *mpd.MPD, shift time.Duration, replaceBase string) {
	if len(mpde.Period) == 0 {
		return
	}
	for _, period := range mpde.Period {

		// Shift periods
		if period.Start != nil {
			startmed, _ := (*period.Start).ToNanoseconds()
			start := time.Duration(startmed)
			*period.Start = DurationToXsdDuration(start + shift)
		}
		if sc.baseurl.String() != "" {
			period.BaseURL[0].Value = ConcatURL(sc.baseurl, period.BaseURL[0].Value).String()
		} else {
			if replaceBase != "" && len(period.BaseURL) > 0 {
				period.BaseURL[0].Value = baseToPath(period.BaseURL[0].Value, replaceBase)

			}
		}
	}
	return
}

// Concatenat URLs
// If b is absolute, just use this
// if not, append
func ConcatURL(a *url.URL, br string) *url.URL {
	b, err := url.Parse(br)
	if err != nil {
		log.Error().Err(err).Msg("Path extension")
		return nil
	}
	if b.IsAbs() {
		return b
	} else {
		// Cut to directory, extend by base path
		joined := a.JoinPath(b.Path)
		return joined
	}
}

// Find a manifest at time 'at'
func (sc *StreamReplay) loadHistoricMpd(at time.Time) (*mpd.MPD, error) {

	sourceElement := sc.FindHistory(at)

	if sourceElement == nil {
		return nil, errors.New("No source found")
	}
	buf, err := os.ReadFile(sourceElement.Filename)
	if err != nil {
		return nil, err
	}
	mpde := new(mpd.MPD)
	if err := mpde.Decode(buf); err != nil {
		return nil, err
	}
	return mpde, nil
}

// GetLooped generates a Manifest by finding the manifest before now%duration
func (sc *StreamReplay) GetLooped(at, now time.Time, requestDuration time.Duration) ([]byte, error) {

	offset, shift, duration, startOfRecording := sc.getLoopMeta(at, now, requestDuration)
	sc.logger.Info().Msgf("Offset: %s TimeShift: %s LoopDuration: %s LoopStart:%s Original At %s",
		RoundToS(offset), RoundToS(shift), RoundToS(duration), shortT(startOfRecording), shortT(startOfRecording.Add(offset)))
	mpdCurrent, err := sc.loadHistoricMpd(startOfRecording.Add(offset))
	if err != nil {
		return []byte{}, err
	}

	sc.AdjustMpd(mpdCurrent, shift, "/") // Manipulate

	//sc.logger.Info().Msgf("Move period: %s", startOfRecording.Add(shift))

	// re-encode
	afterEncode, err := mpdCurrent.Encode()
	if err != nil {
		return nil, err
	}
	return afterEncode, nil
}

// Iterate through all periods, representation, segmentTimeline and
func (sc *StreamReplay) getPtsRange(mpde *mpd.MPD, mimetype string) (time.Time, time.Time, error) {

	if len(mpde.Period) == 0 {
		return time.Time{}, time.Time{}, errors.New("No periods")
	}
	var ast time.Time
	if mpde.AvailabilityStartTime != nil {
		ast = time.Time(*mpde.AvailabilityStartTime)
	}

	var earliest, latest time.Time

	for _, period := range mpde.Period {
		for _, as := range period.AdaptationSets {

			// If there is no segmentTimeline, skip it
			if as.SegmentTemplate == nil || as.SegmentTemplate.SegmentTimeline == nil || len(as.SegmentTemplate.SegmentTimeline.S) == 0 {
				continue
			}
			if as.MimeType != mimetype {
				continue
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
			if earliest.IsZero() || from.Before(earliest) {
				earliest = from
			}
			if to.After(latest) {
				latest = to
			}
		}
	}
	return earliest, latest, nil
}

// Handler serves manifests
func (sc *StreamReplay) Handler(w http.ResponseWriter, r *http.Request) {

	now := time.Now()
	startat := now
	var duration time.Duration

	// Parse time from query Args
	qm := r.URL.Query()

	// If only "at" is given: Calculte to as difference to Now() and redirect
	if len(qm["to"]) == 0 && len(qm["at"]) > 0 {
		at := qm["at"][0]
		t, err := strconv.Atoi(at)
		if err != nil {
			sc.logger.Warn().Err(err).Msg("Parse time")
		} else if t < 1e6 && t > 3e6 {
			sc.logger.Warn().Msg("Implausable time , ignoring")
		} else {
			atime := time.Unix(int64(t), 0)
			sc.logger.Info().Msgf("Redirecting to time %s", atime)
			http.Redirect(w, r, fmt.Sprintf("/manifest.mpd?to=%d", time.Since(atime)/time.Second), http.StatusSeeOther)
			return
		}
	}
	// to timeoffset
	ts := qm["to"]
	if len(ts) > 0 {
		t, err := strconv.Atoi(ts[0])
		if err != nil {
			sc.logger.Warn().Err(err).Msg("Parse time")
		} else if t < 0 && t > 1e6 {
			sc.logger.Warn().Msg("Implausable time offset, ignoring")
		} else {
			startat = startat.Add(-time.Duration(t) * time.Second)
		}
	}
	/*
		// ld loop duration
		ld := qm["ld"]
		if len(ld) > 0 {
			t, err := strconv.Atoi(ld[0])
			if err != nil {
				sc.logger.Warn().Err(err).Msg("Parse time")
			} else if t <= 0 && t > 1e5 {
				sc.logger.Warn().Msg("Implausable duration, ignoring")
			} else {
				duration = time.Duration(t) * time.Second
			}
		}
	*/
	buf, err := sc.GetLooped(startat, now, duration)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Add("Content-Type", "application/dash+xml")
	w.Write(buf)
}

// FileHanlder serves data
func (sc *StreamReplay) FileHandler(w http.ResponseWriter, r *http.Request) {
	filepath := path.Join(sc.dumpdir, r.URL.Path)
	sc.logger.Trace().Str("path", filepath).Msg("Access")
	http.ServeFile(w, r, filepath)
}

func (sc *StreamReplay) ShowStats() {
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
