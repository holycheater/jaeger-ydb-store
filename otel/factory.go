package otel

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uber/jaeger-lib/metrics"
	jgrProm "github.com/uber/jaeger-lib/metrics/prometheus"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/zap"

	"github.com/yandex-cloud/jaeger-ydb-store/otel/configydb"
)

const (
	typeStr = "ydb"
)

func NewFactory() component.ExporterFactory {
	return component.NewExporterFactory(
		typeStr,
		createDefaultConfig,
		component.WithTracesExporter(createTracesExporter),
	)
}

func createDefaultConfig() config.Exporter {
	return &Config{
		ExporterSettings: config.NewExporterSettings(config.NewComponentID(typeStr)),
		TimeoutSettings:  exporterhelper.NewDefaultTimeoutSettings(),
		QueueSettings:    exporterhelper.QueueSettings{Enabled: false},
		RetrySettings:    exporterhelper.NewDefaultRetrySettings(),
		ClientConfig: configydb.ClientConfig{
			Session: configydb.SessionSettings{
				PoolSize: 10,
			},
			WriteTimeout: time.Second * 10,
		},
	}
}

func createTracesExporter(_ context.Context, set component.ExporterCreateSettings, cfg config.Exporter) (component.TracesExporter, error) {
	typedCfg := cfg.(*Config)

	// TODO: make metrics init better, this looks like a hack now
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	go func() {
		err := http.ListenAndServe(":9091", mux)
		if err != nil && err != http.ErrServerClosed {
			set.Logger.Error("failed to serve ydb metrics", zap.Error(err))
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	conn, err := ydb.Open(ctx, sugar.DSN(typedCfg.Endpoint, typedCfg.Database, false),
		ydb.WithAccessTokenCredentials(typedCfg.AuthToken),
		ydb.WithSessionPoolSizeLimit(typedCfg.Session.PoolSize),
	)
	if err != nil {
		return nil, err
	}

	metricsFactory := jgrProm.New(jgrProm.WithRegisterer(registry)).Namespace(metrics.NSOptions{Name: "jaeger_ydb"})
	exp := newTraceExporter(conn.Table(), metricsFactory, set.Logger, typedCfg)

	return exporterhelper.NewTracesExporter(
		cfg,
		set,
		exp.push,
		exporterhelper.WithRetry(typedCfg.RetrySettings),
		exporterhelper.WithQueue(typedCfg.QueueSettings),
		exporterhelper.WithTimeout(typedCfg.TimeoutSettings),
	)
}
