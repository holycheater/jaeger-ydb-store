package otel

import (
	"context"

	"github.com/jaegertracing/jaeger/model"
	jt "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"go.opencensus.io/stats"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/indexer"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/writer"
)

type traceExporter struct {
	spanWriter  *writer.BatchSpanWriter
	indexWriter *indexer.BatchIndexWriter
}

func newTraceExporter(pool table.Client, mf metrics.Factory, logger *zap.Logger, cfg *Config) *traceExporter {
	batchOpts := writer.BatchWriterOptions{
		DbPath:       cfg.DBPath(),
		WriteTimeout: cfg.WriteTimeout,
	}
	return &traceExporter{
		spanWriter:  writer.NewBatchWriter(pool, mf, logger, batchOpts),
		indexWriter: indexer.NewBatchIndexer(pool, mf, logger, cfg.DBPath(), cfg.WriteTimeout),
	}
}

func (e *traceExporter) push(ctx context.Context, td ptrace.Traces) error {
	batches, err := jt.ProtoFromTraces(td)
	if err != nil {
		stats.Record(context.Background(), mConvertFailed.M(int64(td.SpanCount())))
		return err
	}
	spans := make([]*model.Span, 0, td.SpanCount())
	for _, batch := range batches {
		for _, span := range batch.GetSpans() {
			if span.Process == nil {
				span.Process = batch.Process
			}
			spans = append(spans, span)
		}
	}
	e.spanWriter.WriteSpans(spans)
	e.indexWriter.ProcessAndWriteBatch(spans)

	stats.Record(context.Background(),
		mSpanBytes.M(int64(sizer.TracesSize(td))),
		mSpanCount.M(int64(td.SpanCount())),
	)

	return nil
}
