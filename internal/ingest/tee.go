package ingest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/term"

	"github.com/danielloader/waggle/internal/store"
)

// TeeConfig controls log-mirror output. Zero value = disabled.
type TeeConfig struct {
	Path     string   // file path, "-" for stdout, empty for disabled
	Services []string // allow-list of service.name; empty = all
	MinSev   int32    // SeverityNumber floor (inclusive); 0 = no floor
	Format   string   // "console" | "logfmt" | "json"
	Color    string   // "auto" (default) | "always" | "never"
}

// Tee mirrors incoming log Events to an io.Writer in a human-readable
// format so a user can `tail -f` / `less +F` them from a shell alongside
// the Waggle UI. It is strictly a passthrough view of the ingest stream;
// the Store is still authoritative, and Tee never blocks or errors out
// the ingest path — write failures are logged once and the rest of the
// batch continues.
//
// Not used on spans or metrics. Only log records (signal_type='log')
// flow through here; the handler calls us from handleLogs only.
type Tee struct {
	cfg      TeeConfig
	out      io.Writer
	bw       *bufio.Writer
	mu       sync.Mutex
	closer   io.Closer
	services map[string]struct{}
	format   teeFormat
	color    bool // ANSI colour enabled for console format
	// warnedErr is sticky — we log a write error once and keep quiet
	// afterwards so a broken pipe doesn't spam the server log.
	warnedErr bool
}

type teeFormat int

const (
	teeConsole teeFormat = iota
	teeLogfmt
	teeJSON
)

// NewTee opens the configured sink. When cfg.Path is empty, NewTee
// returns (nil, nil) — a nil *Tee is a no-op and callers should guard.
func NewTee(cfg TeeConfig) (*Tee, error) {
	if cfg.Path == "" {
		return nil, nil
	}
	var (
		w      io.Writer
		closer io.Closer
		isTTY  bool
	)
	if cfg.Path == "-" {
		w = os.Stdout
		isTTY = term.IsTerminal(int(os.Stdout.Fd()))
	} else {
		f, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open tee %q: %w", cfg.Path, err)
		}
		w = f
		closer = f
	}
	f := teeConsole
	switch strings.ToLower(cfg.Format) {
	case "", "console":
		f = teeConsole
	case "logfmt":
		f = teeLogfmt
	case "json":
		f = teeJSON
	default:
		if closer != nil {
			closer.Close()
		}
		return nil, fmt.Errorf("unknown tee format %q", cfg.Format)
	}
	color := false
	if f == teeConsole {
		switch strings.ToLower(cfg.Color) {
		case "always":
			color = true
		case "never":
			color = false
		case "", "auto":
			// auto: colour only when writing to a real terminal, and
			// only for the console format. logfmt/json are machine-
			// readable — no colour there.
			color = isTTY
		default:
			if closer != nil {
				closer.Close()
			}
			return nil, fmt.Errorf("unknown tee color %q (want auto|always|never)", cfg.Color)
		}
	}
	t := &Tee{
		cfg:    cfg,
		out:    w,
		bw:     bufio.NewWriter(w),
		closer: closer,
		format: f,
		color:  color,
	}
	if len(cfg.Services) > 0 {
		t.services = make(map[string]struct{}, len(cfg.Services))
		for _, s := range cfg.Services {
			t.services[s] = struct{}{}
		}
	}
	return t, nil
}

// WriteBatch renders each log Event in the batch that passes the
// service + severity filter to the sink, then flushes the buffer.
// Safe to call from multiple goroutines.
func (t *Tee) WriteBatch(b store.Batch) {
	if t == nil || len(b.Events) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := range b.Events {
		e := &b.Events[i]
		if !t.passesFilter(e) {
			continue
		}
		if err := t.writeOne(e); err != nil && !t.warnedErr {
			// Don't take the ingest path down over a tee failure.
			// Log once, then stay quiet.
			fmt.Fprintf(os.Stderr, "waggle: tee write failed: %v\n", err)
			t.warnedErr = true
			return
		}
	}
	if err := t.bw.Flush(); err != nil && !t.warnedErr {
		fmt.Fprintf(os.Stderr, "waggle: tee flush failed: %v\n", err)
		t.warnedErr = true
	}
}

// Close flushes any buffered output and, for file-backed tees, closes
// the underlying file. Idempotent. Safe to call even if NewTee returned
// a nil *Tee.
func (t *Tee) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var firstErr error
	if err := t.bw.Flush(); err != nil {
		firstErr = err
	}
	if t.closer != nil {
		if err := t.closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (t *Tee) passesFilter(e *store.Event) bool {
	if t.services != nil {
		if _, ok := t.services[e.ServiceName]; !ok {
			return false
		}
	}
	if t.cfg.MinSev > 0 {
		if e.SeverityNumber == nil || *e.SeverityNumber < t.cfg.MinSev {
			return false
		}
	}
	return true
}

func (t *Tee) writeOne(e *store.Event) error {
	switch t.format {
	case teeConsole:
		return writeConsole(t.bw, e, t.color)
	case teeLogfmt:
		return writeLogfmt(t.bw, e)
	case teeJSON:
		return writeJSONLine(t.bw, e)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Formatters. All produce a single line terminated with '\n'.
// ---------------------------------------------------------------------------

func writeConsole(w io.Writer, e *store.Event, color bool) error {
	// Wall-clock HH:MM:SS.mmm in the local timezone — matches the Tail
	// UI's render so the two streams look identical side-by-side.
	t := time.Unix(0, e.TimeNS).Local()
	ts := fmt.Sprintf("%02d:%02d:%02d.%03d",
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond()/1_000_000)
	num := severityNumOf(e)
	sev := abbrevSeverity(e.SeverityText, num)
	svc := e.ServiceName
	body := e.Body
	isError := num >= 17

	// `time SEV service body key=value key=value`. Colourised to match
	// the Tail UI palette: muted-grey timestamp, severity-coloured
	// level pill, dim service name, bold-white body, cyan keys, red
	// values for error fields on error-level rows.
	var sb strings.Builder
	writeColored(&sb, color, ansiTime, ts)
	sb.WriteByte(' ')
	writeColored(&sb, color, ansiSeverity(num)+";1", sev)
	sb.WriteByte(' ')
	if svc != "" {
		writeColored(&sb, color, ansiService, svc)
		sb.WriteByte(' ')
	}
	writeColored(&sb, color, ansiBody, body)
	writeConsoleAttrs(&sb, e.AttributesJSON, color, isError)
	sb.WriteByte('\n')
	_, err := io.WriteString(w, sb.String())
	return err
}

// writeConsoleAttrs is writeLogfmtAttrs with optional colour per segment.
// When `color` is false it behaves identically. When true, keys get cyan,
// values stay default, and on error-level rows `error`/`exception.*` field
// values get bold-red — the same highlight the Tail UI applies.
func writeConsoleAttrs(sb *strings.Builder, attrsJSON string, color, errorRow bool) {
	if attrsJSON == "" || attrsJSON == "{}" {
		return
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(attrsJSON), &raw); err != nil {
		return
	}
	for k, v := range raw {
		if strings.HasPrefix(k, "meta.") {
			continue
		}
		sb.WriteByte(' ')
		writeColored(sb, color, ansiKey, k+"=")
		valStyle := ""
		if color && errorRow && isErrorAttrKey(k) {
			valStyle = ansiErrorValue
		}
		if valStyle != "" {
			sb.WriteString(csiStart(valStyle))
			writeLogfmtScalar(sb, v)
			sb.WriteString(csiReset)
		} else {
			writeLogfmtScalar(sb, v)
		}
	}
}

// isErrorAttrKey mirrors the UI's ERROR_ATTR_KEYS set so the tee's
// error-value highlight picks out the same attributes the Tail tab does.
func isErrorAttrKey(k string) bool {
	switch k {
	case "error", "err",
		"error.message", "error.type",
		"exception.message", "exception.type", "exception.stacktrace":
		return true
	}
	return false
}

// --- ANSI helpers ------------------------------------------------------

const (
	// CSI m codes. Semicolon-joined inside a single ESC[...m for
	// combined attributes (e.g. "31;1" = red + bold).
	ansiTime       = "90"    // bright black (grey)
	ansiService    = "37;2"  // white, dim
	ansiBody       = "1"     // bold (keeps default fg = white)
	ansiKey        = "36"    // cyan
	ansiErrorValue = "31;1"  // red, bold
	csiReset       = "\x1b[0m"
)

func ansiSeverity(num int32) string {
	switch {
	case num >= 21:
		return "31" // FATAL: red
	case num >= 17:
		return "31" // ERROR: red
	case num >= 13:
		return "33" // WARN: yellow
	case num >= 9:
		return "32" // INFO: green
	case num >= 5:
		return "34" // DEBUG: blue
	case num > 0:
		return "90" // TRACE: grey
	}
	return "90"
}

func csiStart(code string) string { return "\x1b[" + code + "m" }

func writeColored(sb *strings.Builder, color bool, code, s string) {
	if !color || code == "" {
		sb.WriteString(s)
		return
	}
	sb.WriteString(csiStart(code))
	sb.WriteString(s)
	sb.WriteString(csiReset)
}

func writeLogfmt(w io.Writer, e *store.Event) error {
	t := time.Unix(0, e.TimeNS).UTC().Format("2006-01-02T15:04:05.000Z")
	level := strings.ToLower(abbrevSeverityFull(e.SeverityText, severityNumOf(e)))
	var sb strings.Builder
	fmt.Fprintf(&sb, "time=%s level=%s", t, level)
	if e.ServiceName != "" {
		sb.WriteString(" service=")
		writeLogfmtValue(&sb, e.ServiceName)
	}
	if e.Body != "" {
		sb.WriteString(" msg=")
		writeLogfmtValue(&sb, e.Body)
	}
	writeLogfmtAttrs(&sb, e.AttributesJSON)
	sb.WriteByte('\n')
	_, err := io.WriteString(w, sb.String())
	return err
}

func writeJSONLine(w io.Writer, e *store.Event) error {
	obj := make(map[string]any, 6)
	obj["time"] = time.Unix(0, e.TimeNS).UTC().Format("2006-01-02T15:04:05.000000000Z")
	obj["severity"] = abbrevSeverityFull(e.SeverityText, severityNumOf(e))
	if e.ServiceName != "" {
		obj["service"] = e.ServiceName
	}
	if e.Body != "" {
		obj["body"] = e.Body
	}
	if e.AttributesJSON != "" && e.AttributesJSON != "{}" {
		// Re-decode just to filter meta.* keys — callers asked for a
		// clean record, not the storage-shape blob.
		var raw map[string]any
		if err := json.Unmarshal([]byte(e.AttributesJSON), &raw); err == nil {
			for k := range raw {
				if strings.HasPrefix(k, "meta.") {
					delete(raw, k)
				}
			}
			if len(raw) > 0 {
				obj["attrs"] = raw
			}
		}
	}
	enc := json.NewEncoder(w)
	return enc.Encode(obj)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func severityNumOf(e *store.Event) int32 {
	if e.SeverityNumber == nil {
		return 0
	}
	return *e.SeverityNumber
}

// abbrevSeverity returns the 3-letter short form used by the Tail UI
// (TRC/DBG/INF/WRN/ERR/FTL). Falls back to the SeverityText if set,
// then the band derived from SeverityNumber, then "---".
func abbrevSeverity(text string, num int32) string {
	full := abbrevSeverityFull(text, num)
	switch full {
	case "TRACE":
		return "TRC"
	case "DEBUG":
		return "DBG"
	case "INFO":
		return "INF"
	case "WARN":
		return "WRN"
	case "ERROR":
		return "ERR"
	case "FATAL":
		return "FTL"
	}
	if len(full) >= 3 {
		return strings.ToUpper(full[:3])
	}
	return "---"
}

func abbrevSeverityFull(text string, num int32) string {
	if text != "" {
		return strings.ToUpper(text)
	}
	switch {
	case num >= 21:
		return "FATAL"
	case num >= 17:
		return "ERROR"
	case num >= 13:
		return "WARN"
	case num >= 9:
		return "INFO"
	case num >= 5:
		return "DEBUG"
	case num > 0:
		return "TRACE"
	}
	return ""
}

// writeLogfmtAttrs renders an OTel attributes JSON blob as space-separated
// `key=value` pairs, skipping meta.* keys (waggle-internal bookkeeping).
// Matches the Tail UI's zerolog-style attribute rendering.
func writeLogfmtAttrs(sb *strings.Builder, attrsJSON string) {
	if attrsJSON == "" || attrsJSON == "{}" {
		return
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(attrsJSON), &raw); err != nil {
		return
	}
	for k, v := range raw {
		if strings.HasPrefix(k, "meta.") {
			continue
		}
		sb.WriteByte(' ')
		sb.WriteString(k)
		sb.WriteByte('=')
		writeLogfmtScalar(sb, v)
	}
}

func writeLogfmtScalar(sb *strings.Builder, v any) {
	switch t := v.(type) {
	case string:
		writeLogfmtValue(sb, t)
	case nil:
		sb.WriteString("null")
	case float64:
		// json.Unmarshal gives all numbers as float64. Render integers
		// without a trailing .0 so `pid=37556` looks right.
		if t == float64(int64(t)) {
			fmt.Fprintf(sb, "%d", int64(t))
		} else {
			fmt.Fprintf(sb, "%v", t)
		}
	case bool:
		if t {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	default:
		// Objects/arrays: inline as JSON so nothing is lost.
		b, _ := json.Marshal(t)
		sb.Write(b)
	}
}

func writeLogfmtValue(sb *strings.Builder, s string) {
	// logfmt quoting rule: wrap in double quotes if the value contains
	// whitespace, `=`, or `"`. Escape embedded quotes and backslashes.
	if s == "" {
		sb.WriteString(`""`)
		return
	}
	needsQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '=' || c == '"' {
			needsQuote = true
			break
		}
	}
	if !needsQuote {
		sb.WriteString(s)
		return
	}
	sb.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"', '\\':
			sb.WriteByte('\\')
			sb.WriteByte(c)
		case '\n':
			sb.WriteString(`\n`)
		default:
			sb.WriteByte(c)
		}
	}
	sb.WriteByte('"')
}
