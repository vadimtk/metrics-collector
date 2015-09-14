package mm

import (
	"time"
)

var MetricTypes map[string]bool = map[string]bool{
	"gauge":   true,
	"counter": true,
}

type Metric struct {
	Name   string // mysql/status/Threads_running
	Type   string // gauge, counter, string
	Number float64
	String string
}

type Collection struct {
	Ts      int64 // UTC Unix timestamp
	Metrics []Metric
}

type InstanceStats struct {
        Stats map[string]*Stats // keyed on metric name
}

type Report struct {
	Ts       time.Time // start, UTC
	Duration uint      // seconds
	Stats    []*InstanceStats
}
