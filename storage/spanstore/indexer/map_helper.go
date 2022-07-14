package indexer

import (
	"time"

	"github.com/jaegertracing/jaeger/model"

	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/indexer/index"
)

func newIndexMapWrapper() *indexMapWrapper {
	return &indexMapWrapper{
		m: make(map[indexMapKey]*ttlMapValue),
	}
}

type indexMapWrapper struct {
	m map[indexMapKey]*ttlMapValue
}

func (m *indexMapWrapper) Add(idx index.Indexable, traceID model.TraceID) {
	key := indexMapKey{
		hash: idx.Hash(),
		ts:   idx.Timestamp().Truncate(time.Second * 5).Unix(),
	}
	if v, exists := m.m[key]; exists {
		v.traceIds = append(v.traceIds, traceID)
	} else {
		v = &ttlMapValue{
			idx:      idx,
			traceIds: []model.TraceID{traceID},
		}
		m.m[key] = v
	}
}

func (m *indexMapWrapper) IndexData() []interface{} {
	result := make([]interface{}, 0, len(m.m))
	for _, v := range m.m {
		result = append(result, indexData{
			idx:      v.idx,
			traceIds: v.traceIds,
		})
	}
	return result
}
