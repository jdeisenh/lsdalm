package lsdalm

import (
	"fmt"
	"strings"
)

type PathReplacer struct {
	fmt string
}

func NewPathReplacer(template string) *PathReplacer {

	fmt := template
	// Replace with go format parameters
	fmt = strings.Replace(fmt, "$Time$", "%[1]d", 1)
	fmt = strings.Replace(fmt, "$Number$", "%[2]d", 1)
	fmt = strings.Replace(fmt, "$RepresentationID$", "%[3]s", 1)
	return &PathReplacer{fmt}
}

func (r *PathReplacer) ToPath(time, number int, representationId string) string {
	return fmt.Sprintf(r.fmt, time, number, representationId)
}
