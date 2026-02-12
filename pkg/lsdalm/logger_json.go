package lsdalm

import (
	"time"

	"github.com/rs/zerolog"
)

// jsonCheckerLogger logs in structured JSON format
type jsonCheckerLogger struct {
	logger zerolog.Logger
}

func NewJsonCheckerLogger(logger zerolog.Logger) CheckerLogger {
	return &jsonCheckerLogger{logger: logger}
}

func (o *jsonCheckerLogger) LogNewPeriod(periodId string, starts time.Time) {
	o.logger.Info().Str("periodId", periodId).Time("starts", starts).Msg("new period")
}

func (o *jsonCheckerLogger) LogNewEvent(scheme string, eventId uint64, at time.Time, duration time.Duration) {
	o.logger.Info().Str("scheme", scheme).Uint64("eventId", eventId).Time("at", at).Dur("duration", duration).Msg("new event")
}

func (o *jsonCheckerLogger) LogPeriodGap(periodId string, gapFromPrevious, gapToNext time.Duration) {
	if gapFromPrevious > 1*time.Millisecond || gapToNext > 1*time.Millisecond {
		lvl := o.logger.Info()
		if gapFromPrevious > 10*time.Millisecond || gapToNext > 10*time.Millisecond {
			lvl = o.logger.Warn()
		}
		lvl.Str("periodId", periodId).Dur("gapFromPrevious", gapFromPrevious).Dur("gapToNext", gapToNext).Msg("period gap")
	}
}

func (o *jsonCheckerLogger) LogTrackAlignmentOffset(offsetDiff float64, adaptationSet, periodId string) {
	o.logger.Warn().Float64("offsetDiff", offsetDiff).Str("adaptationSet", adaptationSet).Str("periodId", periodId).Msg("track alignment offset")
}

func (o *jsonCheckerLogger) LogNoUpdate(since time.Duration) {
	o.logger.Warn().Dur("since", since).Msg("no update")
}

// LogManifest serializes the ManifestLog as structured JSON
func (o *jsonCheckerLogger) LogManifest(m *ManifestLog) {
	o.logger.Info().Interface("manifest", m).Msg("manifest")
}
