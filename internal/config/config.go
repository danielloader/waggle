package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DBPath         string
	Addr           string
	IngestAddr     string
	UIAddr         string
	NoOpenBrowser  bool
	Retention      time.Duration
	LogLevel       string
	Dev            bool
}

func Load() (*Config, error) {
	c := &Config{}

	flag.StringVar(&c.DBPath, "db-path", envOr("WAGGLE_DB", "./waggle.db"), "SQLite file path")
	flag.StringVar(&c.Addr, "addr", envOr("WAGGLE_ADDR", "127.0.0.1:4318"), "Bind address for UI, API, and OTLP ingest")
	flag.StringVar(&c.IngestAddr, "ingest-addr", envOr("WAGGLE_INGEST_ADDR", ""), "Override address for OTLP ingest (default: same as --addr)")
	flag.StringVar(&c.UIAddr, "ui-addr", envOr("WAGGLE_UI_ADDR", ""), "Override address for UI + API (default: same as --addr)")
	flag.BoolVar(&c.NoOpenBrowser, "no-open-browser", envBool("WAGGLE_NO_OPEN", false), "Do not auto-open a browser on startup")
	retentionStr := flag.String("retention", envOr("WAGGLE_RETENTION", "24h"), "Drop data older than this")
	flag.StringVar(&c.LogLevel, "log-level", envOr("WAGGLE_LOG_LEVEL", "info"), "slog level: debug, info, warn, error")
	flag.BoolVar(&c.Dev, "dev", false, "Dev mode: do not serve embedded UI and do not open browser")

	flag.Parse()

	d, err := time.ParseDuration(*retentionStr)
	if err != nil {
		return nil, fmt.Errorf("invalid --retention %q: %w", *retentionStr, err)
	}
	c.Retention = d

	if c.IngestAddr == "" {
		c.IngestAddr = c.Addr
	}
	if c.UIAddr == "" {
		c.UIAddr = c.Addr
	}

	return c, nil
}

func (c *Config) SplitListeners() bool {
	return c.IngestAddr != c.UIAddr
}

func envOr(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func envBool(k string, def bool) bool {
	v, ok := os.LookupEnv(k)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
