package lsdalm

import (
	"encoding/json"
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
)

// StreamReplay can replay a recording as is, time-shifted, but otherwise not manipulated
// It will manipulate presentationTimeOffsets and BaseURLs
type StreamReplay struct {
	dumpdir         string
	manifestDir     string
	originalBaseUrl *url.URL
	storageMeta     StorageMeta
	isPast          bool // Flag: recording end is past, loop mode

	logger                   zerolog.Logger
	history                  []HistoryElement
	historyStart, historyEnd time.Time
}

func NewStreamReplay(dumpdir string, logger zerolog.Logger) (*StreamReplay, error) {

	st := &StreamReplay{
		dumpdir:     dumpdir,
		manifestDir: path.Join(dumpdir, ManifestPath),
		logger:      logger,
		history:     make([]HistoryElement, 0, 1000),
	}

	metapath := path.Join(dumpdir, StorageMetaFileName)
	mf, err := os.ReadFile(metapath)
	if err != nil {
		logger.Warn().Err(err).Str("filename", metapath).Msg("Read Metadata")
	} else {
		err = json.Unmarshal(mf, &st.storageMeta)
		if err != nil {
			logger.Warn().Err(err).Str("filename", metapath).Msg("Decode Metadata")
		}

		st.originalBaseUrl, err = url.Parse(st.storageMeta.ManifestUrl)
		if err != nil {
			return nil, err
		}
		st.originalBaseUrl.Path = path.Dir(st.originalBaseUrl.Path)
	}
	return st, nil
}

// LoadArchive Load the full recording
// This is used for already stored, finished Recordings
func (sc *StreamReplay) LoadArchive() error {
	sc.fillData()
	if len(sc.history) < 10 {
		return fmt.Errorf("Not enough manifests")
	}
	sc.isPast = true
	sc.ShowStats()
	return nil
}

// AddManifest adds a manifest to the archive
// This is called for on-the-fly timeshift
func (sc *StreamReplay) AddManifest(filepath string, ctime time.Time) {
	sc.history = append(sc.history, HistoryElement{At: ctime, Filename: path.Base(filepath)})

}

// fillData scans manifestDir and fills history with timestamp->filename
// it will also find first and last TimeLine date in history
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
		sc.logger.Warn().Err(err).Msg("Cannot load first mpd")
		return err
	}
	ff, fl, _ := sc.getPtsRange(first, "video/mp4")

	ls := sc.history[len(sc.history)-1].At
	last, err := sc.loadHistoricMpd(ls)
	if err != nil {
		sc.logger.Warn().Err(err).Msg("Cannot load last mpd")
		return err
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

// AdjustMpd adds a time offset to each Period in the Manifest, shifting the PresentationTime
// Note that this will change the mpd, which only is not problem if its freshly reloaded
func (sc *StreamReplay) AdjustMpd(mpde *mpd.MPD, shift time.Duration, localMedia bool) {
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
		// Expand to full URL first
		if sc.originalBaseUrl != nil && len(period.BaseURL) > 0 && period.BaseURL[0].Value != "" {
			baseurl := period.BaseURL[0].Value
			baseurlUrl := ConcatURL(sc.originalBaseUrl, baseurl)
			if localMedia && !strings.HasPrefix(baseurlUrl.String(), "https://svc49.cdn-t0.tv.telekom.net/") {

				period.BaseURL[0].Value = baseurlUrl.Path[1:]
			} else {
				period.BaseURL[0].Value = baseurlUrl.String()
			}
		}
	}
	return
}

// Find a manifest at time 'at'
func (sc *StreamReplay) loadHistoricMpd(at time.Time) (*mpd.MPD, error) {

	sourceElement := sc.FindHistory(at)

	if sourceElement == nil {
		return nil, errors.New("No source found")
	}
	buf, err := os.ReadFile(sourceElement.Filename)
	if err != nil {
		return nil, fmt.Errorf("Reading %s: %s", sourceElement.Filename, err)
	}
	mpde := new(mpd.MPD)
	if err := mpde.Decode(buf); err != nil {
		return nil, fmt.Errorf("Decoding mpd %s: %s", sourceElement.Filename, err)
	}
	return mpde, nil
}

// GetLooped generates a Manifest by finding the manifest before now%duration
func (sc *StreamReplay) GetLooped(at, now time.Time, requestDuration time.Duration) ([]byte, error) {

	offset, shift, duration, startOfRecording := sc.getLoopMeta(at, at, requestDuration)
	sc.logger.Info().Msgf("Offset: %s TimeShift: %s LoopDuration: %s LoopStart:%s Original At %s",
		RoundToS(offset), RoundToS(shift), RoundToS(duration), shortT(startOfRecording), shortT(startOfRecording.Add(offset)))
	mpdCurrent, err := sc.loadHistoricMpd(startOfRecording.Add(offset))
	if err != nil {
		return []byte{}, err
	}

	sc.AdjustMpd(mpdCurrent, shift, sc.storageMeta.HaveMedia) // Manipulate

	//sc.logger.Info().Msgf("Move period: %s", startOfRecording.Add(shift))

	// re-encode
	afterEncode, err := mpdCurrent.Encode()
	if err != nil {
		return nil, err
	}
	return afterEncode, nil
}

// GetArchived generates a Manifest by finding the manifest closest
func (sc *StreamReplay) GetArchived(timeShift time.Duration, at time.Time) ([]byte, error) {

	mpdCurrent, err := sc.loadHistoricMpd(at.Add(-timeShift))
	if err != nil {
		return []byte{}, err
	}
	// This must be constant for all updates of this session
	sc.AdjustMpd(mpdCurrent, timeShift, sc.storageMeta.HaveMedia) // Manipulate

	sc.logger.Debug().Msgf("Move period: %s", timeShift)

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
	ast := GetAst(mpde)

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
			from, to := SumSegmentTemplate(as.SegmentTemplate, periodStart)
			if from.IsZero() {
				for _, pres := range as.Representations {
					if pres.SegmentTemplate != nil {
						from, to = SumSegmentTemplate(pres.SegmentTemplate, periodStart)
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

func GetArg(qm map[string][]string, name string) string {

	sm, ok := qm[name]
	if !ok {
		return ""
	}
	// Thats probably redundant
	if len(sm) < 0 {
		return ""
	}
	// Only look at the first value
	return sm[0]
}

var NotFound = errors.New("Argument not found")

func GetIntArg(qm map[string][]string, name string) (int, error) {
	v := GetArg(qm, name)
	if v == "" {
		return 0, NotFound
	}
	r, err := strconv.Atoi(v)
	if err != nil {
		return 0, err
	}
	return r, nil
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
	var timeShift time.Duration
	if len(ts) > 0 {
		t, err := strconv.Atoi(ts[0])
		if err != nil {
			sc.logger.Warn().Err(err).Msg("Parse time")
		} else if t < 0 && t > 1e6 {
			sc.logger.Warn().Msg("Implausable time offset, ignoring")
		} else {
			timeShift = time.Duration(t) * time.Second
			startat = startat.Add(-timeShift)
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
	var buf []byte
	var err error
	if sc.isPast {
		buf, err = sc.GetLooped(startat, now, duration)
	} else {
		buf, err = sc.GetArchived(timeShift, now)
	}
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Add("Content-Type", "application/dash+xml")
	w.Write(buf)
}

// FileHandler serves data
func (sc *StreamReplay) FileHandler(w http.ResponseWriter, r *http.Request) {
	filepath := path.Join(sc.dumpdir, r.URL.Path)
	sc.logger.Trace().Str("path", filepath).Msg("Access")
	http.ServeFile(w, r, filepath)
}

func (sc *StreamReplay) ShowStats() {
	if len(sc.history) == 0 {
		return
	}
	first := sc.history[0].At
	last := sc.history[len(sc.history)-1].At
	sc.logger.Info().Msgf("Original source: %s", sc.storageMeta.ManifestUrl)
	sc.logger.Info().Msgf("Recorded %d manifests from %s to %s (%s)",
		len(sc.history),
		first.Format(time.TimeOnly),
		last.Format(time.TimeOnly),
		last.Sub(first),
	)

}
