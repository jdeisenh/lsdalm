package lsdalm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/jdeisenh/lsdalm/pkg/go-mpd"
	"github.com/rs/zerolog"
)

// Data about our stream. Hardcoded from testing, must be dynamic
const (
	timeShiftWindowSize = 25 * time.Second        // timeshift buffer size. Should be taken from manifest or from samples
	LoopPointOffset     = 10 * time.Second        // move splicepoint back in time to be outside the live Delay
	maxMpdGap           = 30 * time.Second        // maximum gap between mpd updates
	segmentSize         = 1920 * time.Millisecond // must be got from stream
)

type StreamLooper struct {
	dumpdir string

	logger zerolog.Logger

	recording *Recording
	// statistics

	originalBaseUrl *url.URL
	storageMeta     StorageMeta
}

func NewStreamLooper(dumpdir string, logger zerolog.Logger) (*StreamLooper, error) {

	st := &StreamLooper{
		dumpdir:   dumpdir,
		logger:    logger,
		recording: NewRecording(path.Join(dumpdir, ManifestPath)),
	}
	st.recording.fillData(st.logger)
	if len(st.recording.history) < 10 {
		return nil, fmt.Errorf("Not enough manifests")
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

	st.recording.ShowStats(st.logger)
	return st, nil
}

// BuildMpb takes the recordings original mpd and adds Segments for the indicated timestamps range
// it also shifts the Timeline by 'shift' and assigns a new id
// ptsShift: shift presentationTime
// periodStart: Beginning of Period
// from, to: Segments to include (in shifted absolute time)
func (sc *StreamLooper) BuildMpd(ptsShift time.Duration, id string, periodStart, from, to time.Time) *mpd.MPD {
	mpde := sc.recording.originalMpd
	outMpd := Copy(mpde)

	period := mpde.Period[0] // There is only one

	// OUput period
	outMpd.Period = make([]*mpd.Period, 0, 1)
	// Loop over output periods in timeShiftWindow

	if len(period.AdaptationSets) == 0 {
		return outMpd
	}

	// Copy period
	np := Copy(period)

	ast := GetAst(mpde)

	// Calculate period start
	var effectivePtsShift time.Duration
	if period.Start != nil {
		startmed, _ := (*period.Start).ToNanoseconds()
		currentPeriodStart := time.Duration(startmed)
		// Time we have to move PTS to offset period start adjustment and ptsOffset
		effectivePtsShift = periodStart.Sub(ast) - currentPeriodStart - ptsShift
		sc.logger.Debug().Msgf("Org and Effective PTS shift: %d %d", ptsShift, effectivePtsShift)
		ns := DurationToXsdDuration(periodStart.Sub(ast))
		sc.logger.Debug().Msgf("Period start: %s", periodStart.Sub(ast))
		np.Start = &ns
	}
	if period.ID != nil {
		np.ID = &id
	}

	np.AdaptationSets = make([]*mpd.AdaptationSet, 0, 5)
	for asi, as := range period.AdaptationSets {
		if as.SegmentTemplate == nil || as.SegmentTemplate.SegmentTimeline == nil {
			continue
		}
		nas := Copy(as)
		nas.SegmentTemplate = Copy(as.SegmentTemplate)
		nas.SegmentTemplate.SegmentTimeline = new(mpd.SegmentTimeline)

		nst := nas.SegmentTemplate
		nstl := nas.SegmentTemplate.SegmentTimeline

		elements := sc.recording.Segments[asi]
		ShiftPto(nst, effectivePtsShift)
		start := elements.start
		timescale := ZeroIfNil(nst.Timescale)
		pto := ZeroIfNil(nst.PresentationTimeOffset)
		first := true
		for _, s := range elements.elements {
			for ri := int64(0); ri <= s.r; ri++ {
				ts := periodStart.Add(TLP2Duration(int64(uint64(start)-pto), timescale))
				d := TLP2Duration(s.d, timescale)
				if !ts.Add(d).Before(from) && ts.Add(d).Before(to) {
					t := uint64(0)
					if first {
						t = uint64(start)
						first = false
					}
					Append(nstl, t, uint64(s.d), 0)
				}
				start += s.d
			}

		}
		if len(nstl.S) == 0 {
			nst.SegmentTimeline = nil
		}
		np.AdaptationSets = append(np.AdaptationSets, nas)

	}

	// Add Events
	np.EventStream = np.EventStream[:0]
	for _, ev := range sc.recording.EventStreamMap {
		// Append all for all ranges: Todo: map offset, duration
		evs := Copy(ev)
		pto := uint64(ZeroIfNil(ev.PresentationTimeOffset))
		timescale := ZeroIfNil(ev.Timescale)
		pto = uint64(int64(pto) + Duration2TLP(effectivePtsShift, timescale))
		evs.PresentationTimeOffset = &pto
		fel := evs.Event[:0]
		for _, e := range evs.Event {
			ts := periodStart.Add(TLP2Duration(int64(*e.PresentationTime-pto), timescale))
			d := TLP2Duration(int64(ZeroIfNil(e.Duration)), timescale)
			// Still in the future
			if ts.After(to) {
				sc.logger.Debug().Msgf("Skip Event %s %d at %s in the future of %s", EmptyIfNil(evs.SchemeIdUri), e.Id, shortT(ts), shortT(to))
				continue
			}
			// End of event in the past end > to
			if from.After(ts.Add(d)) {
				sc.logger.Debug().Msgf("Skip Event %s %d at ends %s before %s", EmptyIfNil(evs.SchemeIdUri), e.Id, shortT(ts.Add(d)), shortT(from))
				continue
			}
			sc.logger.Debug().Msgf("Add Event %s %d at %s-%s", EmptyIfNil(evs.SchemeIdUri), e.Id, shortT(ts), shortT(ts.Add(d)))
			fel = append(fel, e)

		}
		evs.Event = fel
		if len(evs.Event) > 0 {
			np.EventStream = append(np.EventStream, evs)
		}
	}
	//sc.logger.Info().Msgf("Period %d start: %s", periodIdx, periodStart)
	outMpd.Period = append(outMpd.Period, np)

	return outMpd
}

// GetLooped generates a Manifest by finding the manifest before now%duration
func (sc *StreamLooper) GetLooped(at, now time.Time, requestDuration time.Duration) ([]byte, error) {

	offset, shift, duration, startOfRecording := sc.recording.getLoopMeta(at, now, requestDuration)
	sc.logger.Info().Msgf("Offset: %s TimeShift: %s LoopDuration: %s LoopStart:%s At %s",
		RoundToS(offset), RoundToS(shift), RoundToS(duration), shortT(startOfRecording), shortT(at))

	// Check if we are around the loop point
	var mpdCurrent *mpd.MPD
	if offset < timeShiftWindowSize {
		// We are just after the loop point and have to add date from the previous period
		sc.logger.Debug().Msgf("Loop point: %s", shortT(startOfRecording.Add(shift)))
		mpdPrevious := sc.BuildMpd(
			shift-duration,
			fmt.Sprintf("Id-%d", shift/duration-1),
			startOfRecording.Add(shift).Add(-duration),
			now.Add(-timeShiftWindowSize),
			startOfRecording.Add(shift),
		)
		if offset > segmentSize {
			// Ensure period not empty
			mpdCurrent = sc.BuildMpd(
				shift,
				fmt.Sprintf("Id-%d", shift/duration),
				startOfRecording.Add(shift),
				startOfRecording.Add(shift),
				now,
			)
		}
		mpdCurrent = mergeMpd(mpdPrevious, mpdCurrent)
	} else {
		// No loop point
		mpdCurrent = sc.BuildMpd(
			shift,
			fmt.Sprintf("Id-%d", shift/duration),
			startOfRecording.Add(shift),
			now.Add(-timeShiftWindowSize),
			now,
		)
	}
	mpdCurrent = ReBaseMpd(mpdCurrent, sc.originalBaseUrl, sc.storageMeta.HaveMedia)
	// re-encode
	afterEncode, err := mpdCurrent.Encode()
	if err != nil {
		return nil, err
	}
	return afterEncode, nil
}

// GetStatic generates a Manifest by finding the manifest before now%duration
func (sc *StreamLooper) GetStatic() ([]byte, error) {

	start, end := sc.recording.getTimelineRange()
	//start, end = sc.recording.getRecordingRange()
	duration := end.Sub(start)
	ast := GetAst(sc.recording.originalMpd)
	sc.logger.Debug().Msgf("Start %s End %s Duration %s Shift %s",
		shortT(start), shortT(end), RoundToS(duration), start.Sub(ast))

	now := time.Now()
	var mpdCurrent *mpd.MPD
	mpdCurrent = sc.BuildMpd(
		-start.Sub(ast),
		"ID-0",
		ast, // Period starts at 0
		ast,
		now,
	)
	mpdCurrent.AvailabilityStartTime = nil
	mpdtype := "static"
	mpdCurrent.Type = &mpdtype
	mpdCurrent.TimeShiftBufferDepth = nil
	mpdCurrent.SuggestedPresentationDelay = nil
	mpdCurrent.MinimumUpdatePeriod = nil
	dur := DurationToXsdDuration(duration)
	mpdCurrent.MediaPresentationDuration = &dur
	mpdCurrent.Period[0].Duration = &dur
	mpdCurrent = ReBaseMpd(mpdCurrent, sc.originalBaseUrl, sc.storageMeta.HaveMedia)
	// re-encode
	afterEncode, err := mpdCurrent.Encode()
	if err != nil {
		return nil, err
	}
	return afterEncode, nil
}

// Handler serves manifests
func (sc *StreamLooper) DynamicHandler(w http.ResponseWriter, r *http.Request) {
	/*
		loopstart, _ := time.Parse(time.RFC3339, "2025-02-27T09:48:00Z")
		startat= loopstart.Add(time.Now().Sub(sc.start))

	*/
	now := time.Now()
	startat := now
	var duration time.Duration

	// Parse time from query Args

	// to timeoffset
	ts := r.URL.Query()["to"]
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

	// ld loop duration
	ld := r.URL.Query()["ld"]
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

	buf, err := sc.GetLooped(startat, now, duration)
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

// Static Handler serves the whole buffer as a static mpd
func (sc *StreamLooper) StaticHandler(w http.ResponseWriter, r *http.Request) {

	buf, err := sc.GetStatic()
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Add("Content-Type", "application/dash+xml")
	w.Write(buf)
}
