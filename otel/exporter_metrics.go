package otel

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var (
	sizer = ptrace.NewProtoMarshaler().(ptrace.Sizer)
)

var (
	mSpanBytes = stats.Int64(
		"ydb_exporter_processed_spans_bytes",
		"span bytes accepted for write",
		stats.UnitBytes,
	)
	mSpanCount = stats.Int64("ydb_exporter_processed_spans", "", stats.UnitDimensionless)

	mConvertFailed = stats.Int64("ydb_exporter_convert_failed", "otlp to jaeger convert failed spans", stats.UnitDimensionless)
)

func init() {
	_ = view.Register(MetricViews()...)
}

func MetricViews() []*view.View {
	return []*view.View{
		{
			Name:        "ydb_exporter_accepted_span_bytes",
			Measure:     mSpanBytes,
			Description: mSpanBytes.Description(),
			Aggregation: view.Sum(),
		},
		{
			Name:        "ydb_exporter_accepted_span_total",
			Measure:     mSpanCount,
			Description: "",
			Aggregation: view.Sum(),
		},
		{
			Name:        "ydb_exporter_convert_failed_total",
			Measure:     mConvertFailed,
			Description: "",
			Aggregation: view.Sum(),
		},
		{
			Name:        "ydb_exporter_convert_failed_count",
			Measure:     mConvertFailed,
			Description: "",
			Aggregation: view.Count(),
		},
	}
}
