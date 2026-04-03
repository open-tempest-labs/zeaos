package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const defaultMountName = "zeadrive"

// volConfig mirrors the structure of ~/.zeaos/volumez.json.
type volConfig struct {
	Mounts []volMount  `json:"mounts"`
	Cache  volCache    `json:"cache"`
	Debug  bool        `json:"debug"`
}

type volMount struct {
	Path    string                 `json:"path"`
	Backend string                 `json:"backend"`
	Config  map[string]interface{} `json:"config"`
}

type volCache struct {
	Enabled     bool  `json:"enabled"`
	MaxSize     int64 `json:"max_size"`
	TTL         int   `json:"ttl"`
	MetadataTTL int   `json:"metadata_ttl"`
}

// DriveManager handles the zeadrive mount lifecycle and zea:// path resolution.
type DriveManager struct {
	MountPath  string // absolute path to FUSE mount, e.g. ~/zeadrive
	ConfigPath string // ~/.zeaos/volumez.json
	LocalPath  string // ~/.zeaos/local — backing dir for zea:// root
	volProc    *os.Process
}

func NewDriveManager(zeaosDir string) *DriveManager {
	home, _ := os.UserHomeDir()
	d := &DriveManager{
		MountPath:  filepath.Join(home, defaultMountName),
		ConfigPath: filepath.Join(zeaosDir, "volumez.json"),
		LocalPath:  filepath.Join(zeaosDir, "local"),
	}
	_ = os.MkdirAll(d.LocalPath, 0o755)
	return d
}

// ExpandPath resolves a zea:// URL to an absolute filesystem path.
//
// Routing rules:
//   zea://              → LocalPath/
//   zea://<backend>/..  → MountPath/<backend>/...   if <backend> is a configured Volumez mount
//   zea://<other>/...   → LocalPath/<other>/...
func (d *DriveManager) ExpandPath(path string) string {
	rest, ok := strings.CutPrefix(path, "zea://")
	if !ok {
		return path
	}
	// Determine the first path segment to check against configured backends.
	seg := strings.SplitN(rest, "/", 2)[0]
	if seg != "" && d.isBackend(seg) {
		return filepath.Join(d.MountPath, rest)
	}
	return filepath.Join(d.LocalPath, rest)
}

// isBackend returns true if name matches a mount path in volumez.json.
func (d *DriveManager) isBackend(name string) bool {
	cfg, err := d.loadConfig()
	if err != nil {
		return false
	}
	for _, m := range cfg.Mounts {
		if strings.TrimPrefix(m.Path, "/") == name {
			return true
		}
	}
	return false
}

// IsMounted returns true if the FUSE mount point has a different device ID
// than its parent — the reliable way to detect an active mount on POSIX.
func (d *DriveManager) IsMounted() bool {
	var st, parentSt syscall.Stat_t
	if err := syscall.Stat(d.MountPath, &st); err != nil {
		return false
	}
	if err := syscall.Stat(filepath.Dir(d.MountPath), &parentSt); err != nil {
		return false
	}
	return st.Dev != parentSt.Dev
}

// isStale returns true if the mount point looks mounted but our child process
// is not running — i.e., the FUSE process was killed externally.
func (d *DriveManager) isStale() bool {
	if !d.IsMounted() {
		return false
	}
	if d.volProc == nil {
		return true
	}
	// Check if the process is still alive by sending signal 0.
	err := d.volProc.Signal(syscall.Signal(0))
	return err != nil
}

// Label returns the display string used in the startup screen and status bar.
func (d *DriveManager) Label() string {
	if d.IsMounted() {
		return "~/zeadrive [mounted]"
	}
	return "~/zeadrive [not mounted]"
}

// EnsureCloudMount lazily starts Volumez the first time a cloud backend path
// is accessed. No-op if already mounted or no config exists.
func (d *DriveManager) EnsureCloudMount() error {
	if _, err := os.Stat(d.ConfigPath); os.IsNotExist(err) {
		return fmt.Errorf("no ZeaDrive cloud config found — run 'zeadrive enable-s3' to configure")
	}
	if d.isStale() {
		if err := d.forceUnmount(); err != nil {
			return fmt.Errorf("zeadrive: stale mount cleanup failed: %w", err)
		}
	}
	if d.IsMounted() {
		return nil
	}
	return d.startVolumez()
}

// Stop kills the Volumez child process and unmounts the drive. Called on
// session close.
func (d *DriveManager) Stop() {
	if d.volProc != nil {
		_ = d.volProc.Kill()
		_, _ = d.volProc.Wait()
		d.volProc = nil
	}
	if d.IsMounted() {
		_ = d.forceUnmount()
	}
}

// startVolumez launches volumez as a child process owned by ZeaOS.
func (d *DriveManager) startVolumez() error {
	bin, err := exec.LookPath("volumez")
	if err != nil {
		return fmt.Errorf("volumez binary not found in PATH — install volumez first")
	}
	if err := os.MkdirAll(d.MountPath, 0o755); err != nil {
		return fmt.Errorf("zeadrive: mkdir %s: %w", d.MountPath, err)
	}
	cmd := exec.Command(bin, "-config", d.ConfigPath, "-mount", d.MountPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("zeadrive: failed to start volumez: %w", err)
	}
	d.volProc = cmd.Process

	// Wait up to 3 seconds for the FUSE mount to become active.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if d.IsMounted() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("zeadrive: volumez started but mount did not appear at %s", d.MountPath)
}

func (d *DriveManager) forceUnmount() error {
	if runtime.GOOS == "darwin" {
		if err := exec.Command("diskutil", "unmount", "force", d.MountPath).Run(); err != nil {
			return exec.Command("umount", "-f", d.MountPath).Run()
		}
		return nil
	}
	if fp, err := exec.LookPath("fusermount"); err == nil {
		return exec.Command(fp, "-uz", d.MountPath).Run()
	}
	return exec.Command("umount", "-l", d.MountPath).Run()
}

func (d *DriveManager) loadConfig() (*volConfig, error) {
	data, err := os.ReadFile(d.ConfigPath)
	if err != nil {
		return &volConfig{}, nil // no config is not an error
	}
	var cfg volConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (d *DriveManager) saveConfig(cfg *volConfig) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(d.ConfigPath, data, 0o600)
}

// Exec dispatches a zeadrive subcommand.
func (d *DriveManager) Exec(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("zeadrive: subcommand required (status, mount, unmount, enable-s3)")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "status":
		return d.execStatus()
	case "mount":
		return d.execMount(rest)
	case "unmount", "umount":
		return d.execUnmount()
	case "enable-s3":
		return d.execEnableS3()
	default:
		return fmt.Errorf("zeadrive: unknown subcommand %q", sub)
	}
}

func (d *DriveManager) execStatus() error {
	cfg, _ := d.loadConfig()
	fmt.Printf("Local:  zea:// → %s\n", d.LocalPath)
	if d.IsMounted() {
		fmt.Printf("Drive:  %s  [mounted]\n", d.MountPath)
	} else {
		fmt.Printf("Drive:  %s  [not mounted]\n", d.MountPath)
	}
	if cfg != nil && len(cfg.Mounts) > 0 {
		fmt.Println("Backends:")
		for _, m := range cfg.Mounts {
			name := strings.TrimPrefix(m.Path, "/")
			bucket, _ := m.Config["bucket"].(string)
			fmt.Printf("  zea://%s/  → %s  (bucket: %s)\n", name, m.Backend, bucket)
		}
	} else {
		fmt.Println("No cloud backends configured. Run 'zeadrive enable-s3' to add one.")
	}
	return nil
}

func (d *DriveManager) execMount(args []string) error {
	if runtime.GOOS == "windows" {
		return fmt.Errorf("zeadrive: FUSE mounts are not supported on Windows")
	}
	if len(args) > 0 {
		d.MountPath = args[0]
	}
	if d.isStale() {
		fmt.Println("zeadrive: stale mount detected, cleaning up...")
		if err := d.forceUnmount(); err != nil {
			return fmt.Errorf("zeadrive: stale mount cleanup: %w", err)
		}
	}
	if d.IsMounted() {
		fmt.Printf("already mounted at %s\n", d.MountPath)
		return nil
	}
	if err := d.startVolumez(); err != nil {
		return err
	}
	fmt.Printf("zeadrive mounted at %s\n", d.MountPath)
	return nil
}

func (d *DriveManager) execUnmount() error {
	if runtime.GOOS == "windows" {
		return fmt.Errorf("zeadrive: FUSE mounts are not supported on Windows")
	}
	if d.volProc != nil {
		_ = d.volProc.Kill()
		_, _ = d.volProc.Wait()
		d.volProc = nil
	}
	if !d.IsMounted() {
		fmt.Println("zeadrive: not mounted")
		return nil
	}
	if err := d.forceUnmount(); err != nil {
		return fmt.Errorf("zeadrive unmount: %w", err)
	}
	fmt.Printf("unmounted %s\n", d.MountPath)
	return nil
}

// execEnableS3 opens a tview form to configure an S3-compatible backend and
// writes the result to ~/.zeaos/volumez.json.
func (d *DriveManager) execEnableS3() error {
	cfg, err := d.loadConfig()
	if err != nil {
		return fmt.Errorf("zeadrive: load config: %w", err)
	}
	if cfg == nil {
		cfg = &volConfig{}
	}
	if cfg.Cache.MaxSize == 0 {
		cfg.Cache = volCache{Enabled: true, MaxSize: 1073741824, TTL: 300, MetadataTTL: 60}
	}

	// Check for existing AWS credentials.
	awsCreds := awsCredentialsExist()

	var (
		mountName string = "s3-data"
		bucket    string
		region    string = "us-east-1"
		prefix    string
		endpoint  string
		saveErr   error
		saved     bool
	)

	app := tview.NewApplication()

	form := tview.NewForm()
	form.SetBorder(true).SetTitle(" ZeaDrive — Configure S3 Backend ")

	form.AddInputField("Mount name", mountName, 20, nil, func(v string) { mountName = v })
	form.AddInputField("Bucket", bucket, 40, nil, func(v string) { bucket = v })
	form.AddInputField("Region", region, 20, nil, func(v string) { region = v })
	form.AddInputField("Prefix (optional)", prefix, 40, nil, func(v string) { prefix = v })
	form.AddInputField("Endpoint URL (optional)", endpoint, 40, nil, func(v string) { endpoint = v })

	credsNote := "~/.aws/credentials will be used"
	if !awsCreds {
		credsNote = "~/.aws/credentials not found — configure AWS credentials before mounting"
	}
	form.AddTextView("Credentials", credsNote, 0, 1, false, false)

	form.AddButton("Save", func() {
		if bucket == "" {
			saveErr = fmt.Errorf("bucket name is required")
			app.Stop()
			return
		}
		name := strings.TrimPrefix(mountName, "/")
		if name == "" {
			name = "s3-data"
		}
		c := map[string]interface{}{
			"bucket": bucket,
			"region": region,
		}
		if prefix != "" {
			c["prefix"] = prefix
		}
		if endpoint != "" {
			c["endpoint"] = endpoint
		}
		// Replace existing mount with same name, or append.
		replaced := false
		for i, m := range cfg.Mounts {
			if strings.TrimPrefix(m.Path, "/") == name {
				cfg.Mounts[i] = volMount{Path: "/" + name, Backend: "s3", Config: c}
				replaced = true
				break
			}
		}
		if !replaced {
			cfg.Mounts = append(cfg.Mounts, volMount{Path: "/" + name, Backend: "s3", Config: c})
		}
		if err := d.saveConfig(cfg); err != nil {
			saveErr = err
		} else {
			saved = true
		}
		app.Stop()
	})

	form.AddButton("Cancel", func() { app.Stop() })

	form.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			app.Stop()
		}
		return event
	})

	frame := tview.NewFrame(form).
		AddText("Use Tab/Shift-Tab to navigate · Enter to activate · Esc to cancel",
			false, tview.AlignCenter, tcell.ColorDarkGray)

	if err := app.SetRoot(frame, true).Run(); err != nil {
		return fmt.Errorf("zeadrive enable-s3: %w", err)
	}
	if saveErr != nil {
		return fmt.Errorf("zeadrive enable-s3: %w", saveErr)
	}
	if saved {
		fmt.Printf("Saved. Run 'zeadrive mount' to activate zea://%s/\n", strings.TrimPrefix(mountName, "/"))
		if !awsCreds {
			fmt.Println("Warning: ~/.aws/credentials not found. Configure AWS credentials before mounting.")
		}
	}
	return nil
}

// awsCredentialsExist returns true if ~/.aws/credentials exists.
func awsCredentialsExist() bool {
	home, _ := os.UserHomeDir()
	_, err := os.Stat(filepath.Join(home, ".aws", "credentials"))
	return err == nil
}
