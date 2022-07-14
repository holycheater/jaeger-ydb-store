package indexer

import (
	"context"
	"math/rand"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"

	"github.com/yandex-cloud/jaeger-ydb-store/schema"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/dbmodel"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/indexer/index"
	wmetrics "github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/writer/metrics"
)

type BatchIndexWriter struct {
	pool    table.Client
	logger  *zap.Logger
	metrics map[string]indexerMetrics

	idxRand      *rand.Rand
	path         schema.DbPath
	writeTimeout time.Duration
}

func NewBatchIndexer(pool table.Client, mf metrics.Factory, logger *zap.Logger, path schema.DbPath, writeTimeout time.Duration) *BatchIndexWriter {
	return &BatchIndexWriter{
		pool: pool,
		metrics: map[string]indexerMetrics{
			tblTagIndex:              wmetrics.NewTableMetrics(mf, tblTagIndex),
			tblDurationIndex:         wmetrics.NewTableMetrics(mf, tblDurationIndex),
			tblServiceNameIndex:      wmetrics.NewTableMetrics(mf, tblServiceNameIndex),
			tblServiceOperationIndex: wmetrics.NewTableMetrics(mf, tblServiceOperationIndex),
		},
		logger:  logger,
		idxRand: newLockedRand(time.Now().UnixNano()),

		path:         path,
		writeTimeout: writeTimeout,
	}
}

func (w *BatchIndexWriter) ProcessAndWriteBatch(spans []*model.Span) {
	tagMap := newIndexMapWrapper()
	svcNameMap := newIndexMapWrapper()
	svcNameOpMap := newIndexMapWrapper()
	svcDurationMap := newIndexMapWrapper()
	for _, span := range spans {
		for _, tag := range span.GetTags() {
			if shouldIndexTag(tag) {
				tagMap.Add(index.NewTagIndex(span, tag), span.TraceID)
			}
		}
		for _, tag := range span.GetProcess().GetTags() {
			if shouldIndexTag(tag) {
				tagMap.Add(index.NewTagIndex(span, tag), span.TraceID)
			}
		}
		svcNameMap.Add(index.NewServiceNameIndex(span), span.TraceID)
		svcNameOpMap.Add(index.NewServiceOperationIndex(span), span.TraceID)
		svcDurationMap.Add(index.NewDurationIndex(span, ""), span.TraceID)
		if span.OperationName != "" {
			svcDurationMap.Add(index.NewDurationIndex(span, span.OperationName), span.TraceID)
		}
	}
	w.writeItemsTable(tblTagIndex, tagMap.IndexData())
	w.writeItemsTable(tblServiceNameIndex, svcNameMap.IndexData())
	w.writeItemsTable(tblServiceOperationIndex, svcNameOpMap.IndexData())
	w.writeItemsTable(tblDurationIndex, svcDurationMap.IndexData())
}

func (w *BatchIndexWriter) writeItemsTable(tblName string, items []interface{}) {
	parts := map[schema.PartitionKey][]indexData{}
	for _, item := range items {
		data := item.(indexData)
		k := schema.PartitionFromTime(data.idx.Timestamp())
		parts[k] = append(parts[k], data)
	}

	for k, partial := range parts {
		fullName := tableName(w.path, k, tblName)
		w.writePartitionTable(fullName, tblName, partial)
	}
}

func (w *BatchIndexWriter) writePartitionTable(fullTableName string, tblName string, items []indexData) {
	brr := newBucketRR(dbmodel.NumIndexBuckets)
	rows := make([]types.Value, 0, len(items))
	for _, item := range items {
		brr.Next()
		buf := item.traceIds.ToBytes()
		fields := item.idx.StructFields(brr.Next())
		fields = append(fields,
			types.StructFieldValue("uniq", types.Uint32Value(w.idxRand.Uint32())),
			types.StructFieldValue("trace_ids", types.StringValue(buf)),
		)
		rows = append(rows, types.StructValue(fields...))
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.writeTimeout)
	defer cancel()
	ts := time.Now()
	err := w.pool.Do(ctx, func(ctx context.Context, session table.Session) (err error) {
		return session.BulkUpsert(ctx, fullTableName, types.ListValue(rows...))
	})
	if m, ok := w.metrics[tblName]; ok {
		m.Emit(err, time.Since(ts), len(rows))
	}
	if err != nil {
		w.logger.Error("indexer write fail", zap.String("table", tblName), zap.Error(err))
	}
}
