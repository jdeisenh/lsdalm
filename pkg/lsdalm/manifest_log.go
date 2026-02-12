package lsdalm

import (
	"encoding/json"
	"time"
)

// Duration wraps time.Duration to serialize as a human-readable string in JSON.
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d Duration) String() string {
	return time.Duration(d).String()
}

// ManifestLog holds all manifest walk data. Both text and JSON loggers
// render from this shared structure.
type ManifestLog struct {
	Periods []PeriodInfo `json:"periods"`
	Tracks  []TrackLog   `json:"tracks"`
}

type PeriodInfo struct {
	ID    string `json:"id"`
	Start string `json:"start"`
}

type TrackLog struct {
	MimeType    string           `json:"mimeType"`
	Codecs      string           `json:"codecs,omitempty"`
	BufferDepth Duration         `json:"bufferDepth"`
	LiveEdge    Duration         `json:"liveEdge,omitempty"`
	Periods     []TrackPeriodLog `json:"periods"`
}

type TrackPeriodLog struct {
	Duration Duration    `json:"duration,omitempty"`
	Gap      Duration    `json:"gap,omitempty"`
	Missing  bool        `json:"missing,omitempty"`
	Splices  []SpliceLog `json:"splices,omitempty"`
}

type SpliceLog struct {
	Direction   string   `json:"direction"` // "early", "late", "exact"
	Offset      Duration `json:"offset"`
	SegBoundary string   `json:"segBoundary"`
	SegDuration Duration `json:"segDuration"`
}
