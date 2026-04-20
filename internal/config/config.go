package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DBPath        string
	Addr          string
	IngestAddr    string
	UIAddr        string
	NoOpenBrowser bool
	Retention     time.Duration
	LogLevel      string
	Dev           bool

	// Tee: mirror incoming log records to a file or stdout in one of
	// several human-readable formats, so a dev can pipe them to `less`
	// in a shell next to the UI. Tee is strictly a passthrough — what
	// lands on the tee is a subset of what's stored, and the store is
	// still authoritative.
	TeePath     string   // path to mirror file, or "-" for stdout. Empty = disabled.
	TeeServices []string // parsed from --tee-service (comma-separated). Empty = all services.
	TeeMinSev   int32    // minimum severity_number (OTel); 0 = no floor.
	TeeFormat   string   // "console" (default) | "logfmt" | "json"
	TeeColor    string   // "auto" (default, TTY-detect) | "always" | "never"
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

	flag.StringVar(&c.TeePath, "tee", envOr("WAGGLE_TEE", ""), "Mirror log records to this path (use '-' for stdout)")
	teeServicesRaw := flag.String("tee-service", envOr("WAGGLE_TEE_SERVICE", ""), "Comma-separated service.name list to tee (empty = all services)")
	teeSeverity := flag.String("tee-severity", envOr("WAGGLE_TEE_SEVERITY", ""), "Floor severity to tee: trace|debug|info|warn|error|fatal")
	flag.StringVar(&c.TeeFormat, "tee-format", envOr("WAGGLE_TEE_FORMAT", "console"), "Tee output format: console|logfmt|json")
	flag.StringVar(&c.TeeColor, "tee-color", envOr("WAGGLE_TEE_COLOR", "auto"), "ANSI colour: auto|always|never (console format only)")

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

	for s := range strings.SplitSeq(*teeServicesRaw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			c.TeeServices = append(c.TeeServices, s)
		}
	}
	if *teeSeverity != "" {
		n, err := parseTeeSeverity(*teeSeverity)
		if err != nil {
			return nil, fmt.Errorf("invalid --tee-severity %q: %w", *teeSeverity, err)
		}
		c.TeeMinSev = n
	}
	switch c.TeeFormat {
	case "console", "logfmt", "json":
	default:
		return nil, fmt.Errorf("invalid --tee-format %q (want: console, logfmt, json)", c.TeeFormat)
	}
	switch c.TeeColor {
	case "auto", "always", "never":
	default:
		return nil, fmt.Errorf("invalid --tee-color %q (want: auto, always, never)", c.TeeColor)
	}

	return c, nil
}

// parseTeeSeverity maps a short level name to the OTel SeverityNumber
// floor it implies. We use the start-of-band number (TRACE=1, DEBUG=5,
// etc.) so `--tee-severity=warn` admits WARN (13) and everything above.
func parseTeeSeverity(s string) (int32, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace":
		return 1, nil
	case "debug":
		return 5, nil
	case "info":
		return 9, nil
	case "warn", "warning":
		return 13, nil
	case "error", "err":
		return 17, nil
	case "fatal":
		return 21, nil
	}
	return 0, fmt.Errorf("unknown level (want trace|debug|info|warn|error|fatal)")
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
