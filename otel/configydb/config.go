package configydb

import (
	"time"

	"github.com/yandex-cloud/jaeger-ydb-store/schema"
)

type ClientConfig struct {
	Endpoint     string          `mapstructure:"endpoint"`
	Database     string          `mapstructure:"database"`
	AuthToken    string          `mapstructure:"token"`
	Folder       string          `mapstructure:"folder"`
	Session      SessionSettings `mapstructure:"session"`
	WriteTimeout time.Duration   `mapstructure:"write_timeout"`
}

func (cfg *ClientConfig) DBPath() schema.DbPath {
	return schema.DbPath{Path: cfg.Database, Folder: cfg.Folder}
}

type SessionSettings struct {
	PoolSize int `mapstructure:"pool_size"`
}

type IndexerConfig struct {
	WriteTimeout time.Duration
}
