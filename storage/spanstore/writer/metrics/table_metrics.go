package metrics

import (
	"time"

	"github.com/uber/jaeger-lib/metrics"
)

type TableMetrics struct {
	Attempts   metrics.Counter `metric:"attempts"`
	LatencyOk  metrics.Timer   `metric:"latency" tags:"status=ok"`
	LatencyErr metrics.Timer   `metric:"latency" tags:"status=err"`
	RecordsOk  metrics.Counter `metric:"records" tags:"status=ok"`
	RecordsErr metrics.Counter `metric:"records" tags:"status=err"`
}

func NewTableMetrics(factory metrics.Factory, tableName string) *TableMetrics {
	t := &TableMetrics{}
	ns := factory.Namespace(metrics.NSOptions{
		Name: "table",
		Tags: map[string]string{
			"table": tableName,
		},
	})
	metrics.MustInit(t, ns, nil)
	return t
}

func (t *TableMetrics) Emit(err error, latency time.Duration, count int) {
	t.Attempts.Inc(1)
	if err != nil {
		t.LatencyErr.Record(latency)
		t.RecordsErr.Inc(int64(count))
	} else {
		t.LatencyOk.Record(latency)
		t.RecordsOk.Inc(int64(count))
	}
}
