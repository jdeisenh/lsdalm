package lsdalm

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace = "lsdalm"
)

var (
	Processed prometheus.Counter
)

func init() {
	Processed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "processed",
		Help:      "Processed Manifests",
	})
	prometheus.MustRegister(Processed)
}
