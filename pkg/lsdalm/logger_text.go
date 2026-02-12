package lsdalm

import (
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

// textCheckerLogger logs in human-readable text format
type textCheckerLogger struct {
	logger zerolog.Logger
}

func NewTextCheckerLogger(logger zerolog.Logger) CheckerLogger {
	return &textCheckerLogger{logger: logger}
}

func (o *textCheckerLogger) LogNewPeriod(periodId string, starts time.Time) {
	o.logger.Info().Msgf("New Period %s starts %s", periodId, starts)
}

func (o *textCheckerLogger) LogNewEvent(scheme string, eventId uint64, at time.Time, duration time.Duration) {
	o.logger.Info().Msgf("New Event %s:%d at %s Duration %s", scheme, eventId, at, duration)
}

func (o *textCheckerLogger) LogPeriodGap(periodId string, gapFromPrevious, gapToNext time.Duration) {
	if gapFromPrevious > 10*time.Millisecond || gapToNext > 10*time.Millisecond {
		o.logger.Warn().Msgf("Period %s gap from old %s to new %s", periodId, gapFromPrevious, gapToNext)
	} else if gapFromPrevious > 1*time.Millisecond || gapToNext > 1*time.Millisecond {
		o.logger.Info().Msgf("Period %s gap from old %s to new %s", periodId, gapFromPrevious, gapToNext)
	}
}

func (o *textCheckerLogger) LogTrackAlignmentOffset(offsetDiff float64, adaptationSet, periodId string) {
	o.logger.Warn().Msgf("Offset difference of %g s found in AS %s of period %s", offsetDiff, adaptationSet, periodId)
}

func (o *textCheckerLogger) LogNoUpdate(since time.Duration) {
	o.logger.Warn().Msgf("No update since %s", since)
}

// LogManifest renders the ManifestLog as one text line per track
func (o *textCheckerLogger) LogManifest(m *ManifestLog) {
	for _, track := range m.Tracks {
		codecs := ""
		if track.Codecs != "" {
			codecs = "/" + track.Codecs
		}
		msg := fmt.Sprintf("%30s: %8s", track.MimeType+codecs, track.BufferDepth)

		for i, p := range track.Periods {
			if p.Missing {
				msg += " [missing] "
				continue
			}
			if i > 0 && p.Gap > 0 {
				msg += fmt.Sprintf("GAP: %s", p.Gap)
			}
			msg += fmt.Sprintf(" (%8s)", p.Duration)

			for _, sp := range p.Splices {
				switch sp.Direction {
				case "early":
					o.logger.Debug().Msgf("Early %s to %s Len %s", sp.Offset, sp.SegBoundary, sp.SegDuration)
				case "late":
					o.logger.Debug().Msgf("Late  %s to %s Len %s", sp.Offset, sp.SegBoundary, sp.SegDuration)
				case "exact":
					o.logger.Debug().Msgf("Exactly at %s Len %s", sp.SegBoundary, sp.SegDuration)
				}
			}
		}

		if track.LiveEdge > 0 {
			le := time.Duration(track.LiveEdge)
			msg += fmt.Sprintf(" %.1fs", float64(le/(time.Second/10))/10.0)
		}

		o.logger.Info().Msg(msg)
	}
}
