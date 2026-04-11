package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// ExpandPath resolves a zea:// URL.
//
// Routing rules:
//   zea://              → LocalPath/
//   zea://<backend>/..  → MountPath/<backend>/...   if FUSE is mounted
//   zea://<backend>/..  → s3://bucket/prefix/...    if backend is S3 but FUSE not mounted (SDK mode)
//   zea://<other>/...   → LocalPath/<other>/...
func (d *DriveManager) ExpandPath(path string) string {
	rest, ok := strings.CutPrefix(path, "zea://")
	if !ok {
		return path
	}
	mount, subPath := d.mountAndSubpathForZeaPath(path)
	if mount != nil && mount.Backend == "s3" {
		if d.IsMounted() {
			return filepath.Join(d.MountPath, rest)
		}
		// FUSE not available — return an s3:// URI for DuckDB httpfs / SDK writes.
		return d.s3URL(mount, subPath)
	}
	return filepath.Join(d.LocalPath, rest)
}

// s3URL builds an s3:// URI from a mount config and the sub-path within it.
func (d *DriveManager) s3URL(mount *volMount, subPath string) string {
	bucket, _ := mount.Config["bucket"].(string)
	prefix, _ := mount.Config["prefix"].(string)
	key := strings.TrimPrefix(
		strings.ReplaceAll(prefix+"/"+subPath, "//", "/"), "/")
	if bucket == "" {
		return "s3:///" + key
	}
	if key == "" {
		return "s3://" + bucket + "/"
	}
	return "s3://" + bucket + "/" + key
}

// IsS3Path returns true if the given path is an s3:// URI produced by ExpandPath
// when SDK mode is active. Callers use this to skip FUSE checks and filesystem ops.
func (d *DriveManager) IsS3Path(path string) bool {
	return strings.HasPrefix(path, "s3://")
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
	if d.hasS3Config() {
		if _, err := exec.LookPath("volumez"); err != nil {
			return "~/zeadrive [sdk — no mount]"
		}
	}
	return "~/zeadrive [not mounted]"
}

// hasS3Config returns true if any S3 backend is configured.
func (d *DriveManager) hasS3Config() bool {
	cfg, err := d.loadConfig()
	if err != nil || cfg == nil {
		return false
	}
	for _, m := range cfg.Mounts {
		if m.Backend == "s3" {
			return true
		}
	}
	return false
}

// EnsureCloudMount lazily starts Volumez the first time a cloud backend path
// is accessed. No-op if already mounted or no config exists.
// If Volumez is not installed but an S3 config exists, the SDK path is used
// instead (no error — callers check IsS3Path on the expanded path).
func (d *DriveManager) EnsureCloudMount() error {
	if _, err := os.Stat(d.ConfigPath); os.IsNotExist(err) {
		return fmt.Errorf("no ZeaDrive cloud config found — run 'zeadrive enable-s3' to configure")
	}
	// If Volumez is not installed, fall back to SDK mode silently.
	if _, err := exec.LookPath("volumez"); err != nil {
		return nil // SDK mode: ExpandPath already returned s3://, no mount needed
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
		return fmt.Errorf("zeadrive: subcommand required (status, ls, mount, unmount, enable-s3)")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "status":
		return d.execStatus()
	case "ls":
		return d.execLS(rest)
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
		_, noFUSE := exec.LookPath("volumez")
		fmt.Println("Backends:")
		for _, m := range cfg.Mounts {
			name := strings.TrimPrefix(m.Path, "/")
			bucket, _ := m.Config["bucket"].(string)
			mode := "fuse"
			if m.Backend == "s3" && noFUSE != nil {
				mode = "sdk"
			}
			fmt.Printf("  zea://%s/  → %s  (bucket: %s)  [%s]\n", name, m.Backend, bucket, mode)
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

// WriteFile writes data to a zea:// path. For S3-backed mounts it uses the AWS
// SDK to PUT the object directly, bypassing FUSE entirely. Works in SDK mode
// (no Volumez/FUSE required). For local paths it falls back to os.WriteFile.
func (d *DriveManager) WriteFile(zeaPath string, data []byte) error {
	mount, subPath := d.mountAndSubpathForZeaPath(zeaPath)
	if mount != nil && mount.Backend == "s3" {
		return d.s3PutObject(mount, subPath, data)
	}
	// Local backend: write via filesystem.
	dst := d.ExpandPath(zeaPath)
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0644)
}

// CopyDirToMount copies a local directory tree to a zea:// destination,
// skipping the data/ subdirectory (large Parquet files are streamed
// separately). Uses WriteFile per file so S3-backed mounts go through the
// SDK rather than FUSE.
func (d *DriveManager) CopyDirToMount(srcDir, dstZeaBase string) error {
	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		// Skip the data/ directory — Parquet files are streamed directly by
		// the caller to avoid a redundant copy through the SDK.
		if info.IsDir() && rel == "data" {
			return filepath.SkipDir
		}
		if info.IsDir() {
			if rel == "." {
				return nil
			}
			// S3 directories are implied by key prefixes — no explicit create needed.
			mount, _ := d.mountAndSubpathForZeaPath(dstZeaBase)
			if mount != nil && mount.Backend == "s3" {
				return nil
			}
			return os.MkdirAll(d.ExpandPath(dstZeaBase+"/"+filepath.ToSlash(rel)), 0755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return d.WriteFile(dstZeaBase+"/"+filepath.ToSlash(rel), data)
	})
}

// mountAndSubpathForZeaPath resolves a zea:// path to the matching volMount
// and the path segment after the backend name.
func (d *DriveManager) mountAndSubpathForZeaPath(zeaPath string) (*volMount, string) {
	rest, ok := strings.CutPrefix(zeaPath, "zea://")
	if !ok {
		return nil, ""
	}
	parts := strings.SplitN(rest, "/", 2)
	backendName := parts[0]
	subPath := ""
	if len(parts) > 1 {
		subPath = parts[1]
	}
	cfg, _ := d.loadConfig()
	if cfg == nil {
		return nil, subPath
	}
	for i, m := range cfg.Mounts {
		if strings.TrimPrefix(m.Path, "/") == backendName {
			return &cfg.Mounts[i], subPath
		}
	}
	return nil, subPath
}

// ReadS3Path downloads an s3:// URI using the first mount config whose bucket
// matches. Used by verify and other consumers that receive an expanded s3:// path
// and need to read the bytes without a FUSE mount.
func (d *DriveManager) ReadS3Path(s3URI string) ([]byte, error) {
	// Parse s3://bucket/key
	rest := strings.TrimPrefix(s3URI, "s3://")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid s3 URI: %s", s3URI)
	}
	bucket, key := parts[0], parts[1]

	cfg, err := d.loadConfig()
	if err != nil || cfg == nil {
		return nil, fmt.Errorf("no ZeaDrive config for S3 read")
	}
	// Find the mount whose bucket matches to inherit region/endpoint settings.
	var mount *volMount
	for i := range cfg.Mounts {
		b, _ := cfg.Mounts[i].Config["bucket"].(string)
		if b == bucket {
			mount = &cfg.Mounts[i]
			break
		}
	}
	if mount == nil {
		// No matching mount — try a default AWS config.
		mount = &volMount{Config: map[string]interface{}{"bucket": bucket, "region": "us-east-1"}}
	}
	return d.s3GetObject(mount, key)
}

// s3GetObject downloads an S3 object using the mount's region/endpoint config.
func (d *DriveManager) s3GetObject(mount *volMount, key string) ([]byte, error) {
	bucket, _ := mount.Config["bucket"].(string)
	region, _ := mount.Config["region"].(string)
	endpoint, _ := mount.Config["endpoint"].(string)
	if region == "" {
		region = "us-east-1"
	}

	ctx := context.Background()
	awscfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("s3 get: load config: %w", err)
	}
	client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// s3PutObject writes data directly to S3, bypassing FUSE.
func (d *DriveManager) s3PutObject(mount *volMount, subPath string, data []byte) error {
	bucket, _ := mount.Config["bucket"].(string)
	prefix, _ := mount.Config["prefix"].(string)
	region, _ := mount.Config["region"].(string)
	endpoint, _ := mount.Config["endpoint"].(string)
	if region == "" {
		region = "us-east-1"
	}

	// Build S3 key: join prefix and subpath, collapse double slashes.
	key := strings.TrimPrefix(
		strings.ReplaceAll(prefix+"/"+subPath, "//", "/"), "/")

	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return fmt.Errorf("s3: load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

// ConfigureHTTPFS emits DuckDB SET statements so that s3:// URIs produced by
// ExpandPath resolve correctly via the httpfs extension. Called once at session
// init. Only runs when an S3 backend is configured; no-op otherwise.
// Uses the first configured S3 mount's region and endpoint settings.
func (d *DriveManager) ConfigureHTTPFS(ctx context.Context, exec func(string)) {
	cfg, err := d.loadConfig()
	if err != nil || cfg == nil {
		return
	}
	var s3mount *volMount
	for i := range cfg.Mounts {
		if cfg.Mounts[i].Backend == "s3" {
			s3mount = &cfg.Mounts[i]
			break
		}
	}
	if s3mount == nil {
		return
	}
	region, _ := s3mount.Config["region"].(string)
	endpoint, _ := s3mount.Config["endpoint"].(string)
	if region == "" {
		region = "us-east-1"
	}
	// httpfs is already loaded at session init; just configure S3 settings.
	exec(fmt.Sprintf("SET s3_region='%s'", region))
	if endpoint != "" {
		// Strip scheme — DuckDB httpfs expects host:port only.
		ep := strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
		exec(fmt.Sprintf("SET s3_endpoint='%s'", ep))
		exec("SET s3_url_style='path'")
		if strings.HasPrefix(endpoint, "http://") {
			exec("SET s3_use_ssl=false")
		}
	}
}

// awsCredentialsExist returns true if ~/.aws/credentials exists.
func awsCredentialsExist() bool {
	home, _ := os.UserHomeDir()
	_, err := os.Stat(filepath.Join(home, ".aws", "credentials"))
	return err == nil
}

// execLS lists the contents of a zea:// path (or the zea:// root if no path given).
func (d *DriveManager) execLS(args []string) error {
	zeaPath := "zea://"
	if len(args) > 0 {
		zeaPath = args[0]
		if !strings.HasPrefix(zeaPath, "zea://") {
			zeaPath = "zea://" + zeaPath
		}
	}

	mount, subPath := d.mountAndSubpathForZeaPath(zeaPath)

	// S3 backend — use ListObjectsV2 with a delimiter to get a directory-like view.
	if mount != nil && mount.Backend == "s3" {
		return d.execLSS3(mount, subPath)
	}

	// Local or FUSE-mounted path — use the filesystem.
	fsPath := d.ExpandPath(zeaPath)
	entries, err := os.ReadDir(fsPath)
	if err != nil {
		return fmt.Errorf("zeadrive ls: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			fmt.Printf("  %s/\n", e.Name())
		} else {
			info, _ := e.Info()
			if info != nil {
				fmt.Printf("  %-40s  %d\n", e.Name(), info.Size())
			} else {
				fmt.Printf("  %s\n", e.Name())
			}
		}
	}
	return nil
}

// execLSS3 lists objects under a prefix in an S3 backend using a delimiter so
// it behaves like a directory listing rather than a flat key dump.
func (d *DriveManager) execLSS3(mount *volMount, subPath string) error {
	bucket, _ := mount.Config["bucket"].(string)
	prefix, _ := mount.Config["prefix"].(string)
	region, _ := mount.Config["region"].(string)
	endpoint, _ := mount.Config["endpoint"].(string)
	if region == "" {
		region = "us-east-1"
	}

	// Build the S3 key prefix to list under.
	keyPrefix := strings.TrimPrefix(
		strings.ReplaceAll(prefix+"/"+subPath, "//", "/"), "/")
	if keyPrefix != "" && !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}

	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return fmt.Errorf("zeadrive ls: load AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})

	delim := "/"
	resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(keyPrefix),
		Delimiter: aws.String(delim),
	})
	if err != nil {
		return fmt.Errorf("zeadrive ls: %w", err)
	}

	if len(resp.CommonPrefixes) == 0 && len(resp.Contents) == 0 {
		fmt.Println("  (empty)")
		return nil
	}

	for _, cp := range resp.CommonPrefixes {
		name := strings.TrimPrefix(aws.ToString(cp.Prefix), keyPrefix)
		fmt.Printf("  %s\n", name) // already has trailing slash
	}
	for _, obj := range resp.Contents {
		name := strings.TrimPrefix(aws.ToString(obj.Key), keyPrefix)
		if name == "" {
			continue
		}
		fmt.Printf("  %-40s  %d\n", name, obj.Size)
	}
	return nil
}
