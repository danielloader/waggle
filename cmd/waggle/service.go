package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// macOS launchd label and Linux systemd unit name. Kept stable so
// service-uninstall can find what service-install wrote.
const (
	launchdLabel = "com.github.danielloader.waggle"
	systemdUnit  = "waggle.service"
)

// runServiceInstall writes a per-user systemd unit (Linux) or LaunchAgent
// plist (macOS) for the currently-running binary and (by default) starts it.
// Any flags after `--` are baked verbatim into the unit's ExecStart /
// ProgramArguments so the service can be tuned without hand-editing.
func runServiceInstall(args []string) error {
	fs := flag.NewFlagSet("service-install", flag.ContinueOnError)
	noStart := fs.Bool("no-start", false, "Write the unit/plist but do not enable or start it")
	if err := fs.Parse(args); err != nil {
		return err
	}
	extra := fs.Args()

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve current binary: %w", err)
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		return fmt.Errorf("resolve symlink for %q: %w", exe, err)
	}

	switch runtime.GOOS {
	case "linux":
		return installSystemd(exe, extra, *noStart)
	case "darwin":
		return installLaunchd(exe, extra, *noStart)
	default:
		return fmt.Errorf("service-install is only supported on Linux (systemd) and macOS (launchd); not %s", runtime.GOOS)
	}
}

func runServiceUninstall(args []string) error {
	fs := flag.NewFlagSet("service-uninstall", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch runtime.GOOS {
	case "linux":
		return uninstallSystemd()
	case "darwin":
		return uninstallLaunchd()
	default:
		return fmt.Errorf("service-uninstall is only supported on Linux (systemd) and macOS (launchd); not %s", runtime.GOOS)
	}
}

// ---- systemd (Linux, per-user) -------------------------------------------------

func systemdUnitPath() (string, error) {
	cfg, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(cfg, "systemd", "user", systemdUnit), nil
}

func installSystemd(exe string, extra []string, noStart bool) error {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return errors.New("systemctl not found on PATH (is systemd available?)")
	}

	unitPath, err := systemdUnitPath()
	if err != nil {
		return err
	}
	if _, err := os.Stat(unitPath); err == nil {
		return fmt.Errorf("unit already exists at %s; run `waggle service-uninstall` first", unitPath)
	}

	execStart := shellJoin(append([]string{exe, "--no-open-browser"}, extra...))
	unit := fmt.Sprintf(`[Unit]
Description=waggle — local OpenTelemetry viewer
Documentation=https://github.com/danielloader/waggle
After=network-online.target

[Service]
Type=simple
ExecStart=%s
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
`, execStart)

	if err := os.MkdirAll(filepath.Dir(unitPath), 0o755); err != nil {
		return fmt.Errorf("create unit dir: %w", err)
	}
	if err := os.WriteFile(unitPath, []byte(unit), 0o644); err != nil {
		return fmt.Errorf("write unit: %w", err)
	}
	fmt.Println("wrote", unitPath)

	if err := run("systemctl", "--user", "daemon-reload"); err != nil {
		return err
	}
	if noStart {
		fmt.Println("skipped enable/start (--no-start); run `systemctl --user enable --now", systemdUnit+"` when ready")
		return nil
	}
	if err := run("systemctl", "--user", "enable", "--now", systemdUnit); err != nil {
		return err
	}
	fmt.Println("waggle is running. `systemctl --user status", systemdUnit+"` for details.")
	fmt.Println("note: to keep the service running across reboots without an active login, run `sudo loginctl enable-linger`", os.Getenv("USER"))
	return nil
}

func uninstallSystemd() error {
	unitPath, err := systemdUnitPath()
	if err != nil {
		return err
	}
	if _, err := exec.LookPath("systemctl"); err == nil {
		// Best-effort stop+disable; ignore "not loaded" errors.
		_ = exec.Command("systemctl", "--user", "disable", "--now", systemdUnit).Run()
	}
	if err := os.Remove(unitPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Println("no unit at", unitPath, "— nothing to do")
			return nil
		}
		return fmt.Errorf("remove unit: %w", err)
	}
	fmt.Println("removed", unitPath)
	if _, err := exec.LookPath("systemctl"); err == nil {
		_ = exec.Command("systemctl", "--user", "daemon-reload").Run()
	}
	return nil
}

// ---- launchd (macOS, per-user LaunchAgent) ------------------------------------

func launchdPlistPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist"), nil
}

func launchdLogPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "Logs", "waggle", "waggle.log"), nil
}

func installLaunchd(exe string, extra []string, noStart bool) error {
	plistPath, err := launchdPlistPath()
	if err != nil {
		return err
	}
	if _, err := os.Stat(plistPath); err == nil {
		return fmt.Errorf("plist already exists at %s; run `waggle service-uninstall` first", plistPath)
	}
	logPath, err := launchdLogPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return fmt.Errorf("create log dir: %w", err)
	}

	var argsXML strings.Builder
	for _, a := range append([]string{exe, "--no-open-browser"}, extra...) {
		fmt.Fprintf(&argsXML, "        <string>%s</string>\n", xmlEscape(a))
	}

	plist := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>%s</string>
    <key>ProgramArguments</key>
    <array>
%s    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>%s</string>
    <key>StandardErrorPath</key>
    <string>%s</string>
</dict>
</plist>
`, launchdLabel, argsXML.String(), xmlEscape(logPath), xmlEscape(logPath))

	if err := os.MkdirAll(filepath.Dir(plistPath), 0o755); err != nil {
		return fmt.Errorf("create LaunchAgents dir: %w", err)
	}
	if err := os.WriteFile(plistPath, []byte(plist), 0o644); err != nil {
		return fmt.Errorf("write plist: %w", err)
	}
	fmt.Println("wrote", plistPath)

	if noStart {
		fmt.Println("skipped launchctl bootstrap (--no-start); load with `launchctl bootstrap gui/$UID", plistPath+"` when ready")
		return nil
	}
	if err := run("launchctl", "bootstrap", "gui/"+strconv.Itoa(os.Getuid()), plistPath); err != nil {
		return err
	}
	fmt.Println("waggle is running. logs:", logPath)
	return nil
}

func uninstallLaunchd() error {
	plistPath, err := launchdPlistPath()
	if err != nil {
		return err
	}
	// Best-effort bootout; ignore "not loaded" errors.
	_ = exec.Command("launchctl", "bootout", "gui/"+strconv.Itoa(os.Getuid())+"/"+launchdLabel).Run()
	if err := os.Remove(plistPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Println("no plist at", plistPath, "— nothing to do")
			return nil
		}
		return fmt.Errorf("remove plist: %w", err)
	}
	fmt.Println("removed", plistPath)
	return nil
}

// ---- helpers ------------------------------------------------------------------

func run(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

// shellJoin formats argv for a systemd ExecStart line. systemd parses
// ExecStart with its own quoting rules; for paths/flags we expect in practice
// (no embedded quotes), wrapping anything containing whitespace in double
// quotes is sufficient and matches what `systemd-escape` would produce.
func shellJoin(argv []string) string {
	parts := make([]string, len(argv))
	for i, a := range argv {
		if strings.ContainsAny(a, " \t\"") {
			parts[i] = strconv.Quote(a)
		} else {
			parts[i] = a
		}
	}
	return strings.Join(parts, " ")
}

func xmlEscape(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
	)
	return r.Replace(s)
}
