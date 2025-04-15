// Package mpd implements parsing and generating of MPEG-DASH Media Presentation Description (MPD) files.
package mpd

import (
	"bytes"
	"encoding/xml"
	"io"
	"regexp"

	"github.com/jdeisenh/lsdalm/pkg/go-xsd-types"
)

// http://mpeg.chiariglione.org/standards/mpeg-dash
// https://www.brendanlong.com/the-structure-of-an-mpeg-dash-mpd.html
// http://standards.iso.org/ittf/PubliclyAvailableStandards/MPEG-DASH_schema_files/DASH-MPD.xsd

var emptyElementRE = regexp.MustCompile(`></[A-Za-z]+>`)

// MPD represents root XML element.
type MPD struct {
	XMLNS                      *string       `xml:"xmlns,attr"`
	BaseURL                    []*BaseURL    `xml:"BaseURL,omitempty"`
	Type                       *string       `xml:"type,attr"`
	MinimumUpdatePeriod        *xsd.Duration `xml:"minimumUpdatePeriod,attr"`
	AvailabilityStartTime      *xsd.DateTime `xml:"availabilityStartTime,attr"`
	AvailabilityEndTime        *xsd.DateTime `xml:"availabilityEndTime,attr"`
	MediaPresentationDuration  *xsd.Duration `xml:"mediaPresentationDuration,attr"`
	MinBufferTime              *xsd.Duration `xml:"minBufferTime,attr"`
	SuggestedPresentationDelay *xsd.Duration `xml:"suggestedPresentationDelay,attr"`
	TimeShiftBufferDepth       *xsd.Duration `xml:"timeShiftBufferDepth,attr"`
	PublishTime                *xsd.DateTime `xml:"publishTime,attr"`
	Profiles                   string        `xml:"profiles,attr"`
	Period                     []*Period     `xml:"Period,omitempty"`
	UTCTiming                  *Descriptor   `xml:"UTCTiming,omitempty"`
}

// Do not try to use encoding.TextMarshaler and encoding.TextUnmarshaler:
// https://github.com/golang/go/issues/6859#issuecomment-118890463

// Encode generates MPD XML.
func (m *MPD) Encode() ([]byte, error) {
	x := new(bytes.Buffer)
	e := xml.NewEncoder(x)
	e.Indent("", "  ")
	err := e.Encode(m)
	if err != nil {
		return nil, err
	}

	// hacks for self-closing tags
	res := new(bytes.Buffer)
	res.WriteString(`<?xml version="1.0" encoding="utf-8"?>`)
	res.WriteByte('\n')
	for {
		s, err := x.ReadString('\n')
		if s != "" {
			s = emptyElementRE.ReplaceAllString(s, `/>`)
			res.WriteString(s)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	res.WriteByte('\n')
	return res.Bytes(), err
}

// Decode parses MPD XML.
func (m *MPD) Decode(b []byte) error {
	return xml.Unmarshal(b, m)
}

// Period represents XSD's PeriodType.
type Period struct {
	Start          *xsd.Duration    `xml:"start,attr"`
	ID             *string          `xml:"id,attr"`
	Duration       *xsd.Duration    `xml:"duration,attr"`
	BaseURL        []*BaseURL       `xml:"BaseURL,omitempty"`
	EventStream    []*EventStream   `xml:"EventStream,omitempty"`
	AdaptationSets []*AdaptationSet `xml:"AdaptationSet,omitempty"`
}

// BaseURL represents XSD's BaseURLType.
type BaseURL struct {
	Value                    string  `xml:",chardata"`
	ServiceLocation          *string `xml:"serviceLocation,attr"`
	ByteRange                *string `xml:"byteRange,attr"`
	AvailabilityTimeOffset   *uint64 `xml:"availabilityTimeOffset,attr"`
	AvailabilityTimeComplete *bool   `xml:"availabilityTimeComplete,attr"`
}

// AdaptationSet represents XSD's AdaptationSetType.
type AdaptationSet struct {
	MimeType                  string           `xml:"mimeType,attr"`
	ContentType               *string          `xml:"contentType,attr"`
	SegmentAlignment          ConditionalUint  `xml:"segmentAlignment,attr"`
	SubsegmentAlignment       ConditionalUint  `xml:"subsegmentAlignment,attr"`
	StartWithSAP              ConditionalUint  `xml:"startWithSAP,attr"`
	SubsegmentStartsWithSAP   ConditionalUint  `xml:"subsegmentStartsWithSAP,attr"`
	BitstreamSwitching        *bool            `xml:"bitstreamSwitching,attr"`
	Group                     *string          `xml:"group,attr"`
	AudioSamplingRate         *string          `xml:"audioSamplingRate,attr"`
	MinBandwidth              *string          `xml:"minBandwidth,attr"`
	MaxBandwidth              *string          `xml:"maxBandwidth,attr"`
	MaxHeight                 *string          `xml:"maxHeight,attr"`
	MaxWidth                  *string          `xml:"maxWidth,attr"`
	MinFrameRate              *string          `xml:"minFrameRate,attr"`
	MaxFrameRate              *string          `xml:"maxFrameRate,attr"`
	Sar                       *string          `xml:"sar,attr"`
	Lang                      *string          `xml:"lang,attr"`
	Id                        *string          `xml:"id,attr"`
	Par                       *string          `xml:"par,attr"`
	Codecs                    *string          `xml:"codecs,attr"`
	Role                      []*Descriptor    `xml:"Role,omitempty"`
	BaseURL                   []*BaseURL       `xml:"BaseURL,omitempty"`
	SegmentTemplate           *SegmentTemplate `xml:"SegmentTemplate,omitempty"`
	ContentProtections        []Descriptor     `xml:"ContentProtection,omitempty"`
	Representations           []Representation `xml:"Representation,omitempty"`
	InbandEventStream         []Descriptor     `xml:"InbandEventStream,omitempty"`
	AudioChannelConfiguration []Descriptor     `xml:"AudioChannelConfiguration,omitempty"`
}

// Representation represents XSD's RepresentationType.
type Representation struct {
	ID                 *string          `xml:"id,attr"`
	Width              *uint64          `xml:"width,attr"`
	Height             *uint64          `xml:"height,attr"`
	FrameRate          *string          `xml:"frameRate,attr"`
	Bandwidth          *uint64          `xml:"bandwidth,attr"`
	AudioSamplingRate  *string          `xml:"audioSamplingRate,attr"`
	Codecs             *string          `xml:"codecs,attr"`
	SAR                *string          `xml:"sar,attr"`
	ScanType           *string          `xml:"scanType,attr"`
	ContentProtections []Descriptor     `xml:"ContentProtection,omitempty"`
	EssentialProperty  []Descriptor     `xml:"EssentialProperty,omitempty"`
	SegmentTemplate    *SegmentTemplate `xml:"SegmentTemplate,omitempty"`
	BaseURL            []*BaseURL       `xml:"BaseURL,omitempty"`
}

// Descriptor represents XSD's DescriptorType.
type Descriptor struct {
	SchemeIDURI *string `xml:"schemeIdUri,attr"`
	Value       *string `xml:"value,attr"`
}

// SegmentTemplate represents XSD's SegmentTemplateType.
type SegmentTemplate struct {
	Duration               *uint64          `xml:"duration,attr"`
	Timescale              *uint64          `xml:"timescale,attr"`
	Media                  *string          `xml:"media,attr"`
	Initialization         *string          `xml:"initialization,attr"`
	StartNumber            *uint64          `xml:"startNumber,attr"`
	PresentationTimeOffset *uint64          `xml:"presentationTimeOffset,attr"`
	SegmentTimeline        *SegmentTimeline `xml:"SegmentTimeline,omitempty"`
}

// SegmentTimeline represents XSD's SegmentTimelineType.
type SegmentTimeline struct {
	S []*SegmentTimelineS `xml:"S"`
}

type EventStream struct {
	SchemeIdUri            *string `xml:"schemeIdUri,attr"`
	Timescale              *uint64 `xml:"timescale,attr"`
	Value                  *string `xml:"value,attr"`
	PresentationTimeOffset *uint64 `xml:"presentationTimeOffset,attr"`
	Event                  []Event `xml:"Event,omitempty"`
}

type Event struct {
	Duration         *uint64  `xml:"duration,attr"`
	PresentationTime *uint64  `xml:"presentationTime,attr"`
	Id               uint64   `xml:"id,attr"`
	ContentEncoding  *string  `xml:"contentEncoding,attr"`
	Signal           []Signal `xml:"Signal,omitempty"`
	Content          string   `xml:",chardata"`
}

type Signal struct {
	Xmlns  *string `xml:"xmlns,attr"`
	Binary *Binary `xml:"Binary,omitempty"`
}

type Binary struct {
	Value string `xml:",chardata"`
}

// SegmentTimelineS represents XSD's SegmentTimelineType's inner S elements.
type SegmentTimelineS struct {
	T *uint64 `xml:"t,attr"`
	D uint64  `xml:"d,attr"`
	R *int64  `xml:"r,attr"`
}
