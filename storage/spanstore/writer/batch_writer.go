package writer

import (
	"context"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"

	"github.com/yandex-cloud/jaeger-ydb-store/schema"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/dbmodel"
	wmetrics "github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/writer/metrics"
)

const (
	tblTraces = "traces"
)

type BatchSpanWriter struct {
	pool   table.Client
	logger *zap.Logger
	opts   BatchWriterOptions

	metrics *wmetrics.TableMetrics
}

func NewBatchWriter(pool table.Client, mf metrics.Factory, logger *zap.Logger, opts BatchWriterOptions) *BatchSpanWriter {
	return &BatchSpanWriter{
		pool:    pool,
		logger:  logger,
		opts:    opts,
		metrics: wmetrics.NewTableMetrics(mf, tblTraces),
	}
}

func (w *BatchSpanWriter) WriteSpans(spans []*model.Span) {
	items := make([]interface{}, len(spans))
	for i, s := range spans {
		items[i] = s
	}
	w.WriteItems(items)
}

func (w *BatchSpanWriter) WriteItems(items []interface{}) {
	parts := map[schema.PartitionKey][]*model.Span{}
	for _, item := range items {
		span := item.(*model.Span)
		k := schema.PartitionFromTime(span.StartTime)
		parts[k] = append(parts[k], span)
	}
	for k, partial := range parts {
		w.writeItemsToPartition(k, partial)
	}
}

func (w *BatchSpanWriter) writeItemsToPartition(part schema.PartitionKey, items []*model.Span) {
	spanRecords := make([]types.Value, 0, len(items))
	for _, span := range items {
		dbSpan, _ := dbmodel.FromDomain(span)
		spanRecords = append(spanRecords, dbSpan.StructValue())
	}

	ctx, ctxCancel := context.WithTimeout(context.Background(), w.opts.WriteTimeout)
	defer ctxCancel()
	tableName := func(table string) string {
		return part.BuildFullTableName(w.opts.DbPath.String(), table)
	}
	var err error

	if err = w.uploadRows(ctx, tableName(tblTraces), spanRecords); err != nil {
		w.logger.Error("insertSpan error", zap.Error(err))
		return
	}
}

func (w *BatchSpanWriter) uploadRows(ctx context.Context, tableName string, rows []types.Value) error {
	ts := time.Now()
	data := types.ListValue(rows...)
	err := w.pool.Do(ctx, func(ctx context.Context, session table.Session) (err error) {
		return session.BulkUpsert(ctx, tableName, data)
	})
	w.metrics.Emit(err, time.Since(ts), len(rows))
	return err
}
