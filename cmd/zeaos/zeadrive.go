package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

const defaultMountName = "zeadrive"

// DriveManager handles the zeadrive mount lifecycle and filesystem commands.
type DriveManager struct {
	MountPath  string // absolute path, e.g. /Users/foo/zeadrive
	ConfigPath string // volumez config, e.g. ~/.zeaos/volumez.json
}

func NewDriveManager(zeaosDir string) *DriveManager {
	home, _ := os.UserHomeDir()
	return &DriveManager{
		MountPath:  filepath.Join(home, defaultMountName),
		ConfigPath: filepath.Join(zeaosDir, "volumez.json"),
	}
}

// IsMounted returns true if the mount path exists and is readable.
func (d *DriveManager) IsMounted() bool {
	_, err := os.ReadDir(d.MountPath)
	return err == nil
}

// Label returns the display string used in the startup screen and status bar.
func (d *DriveManager) Label() string {
	if d.IsMounted() {
		return "~/zeadrive"
	}
	return "~/zeadrive (not mounted)"
}

// Exec dispatches a zeadrive subcommand.
func (d *DriveManager) Exec(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("zeadrive: subcommand required (status, mount, unmount)")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "status":
		return d.execStatus()
	case "mount":
		return d.execMount(rest)
	case "unmount", "umount":
		return d.execUnmount()
	default:
		return fmt.Errorf("zeadrive: unknown subcommand %q", sub)
	}
}

func (d *DriveManager) execStatus() error {
	if d.IsMounted() {
		fmt.Printf("Drive:  %s  [mounted]\n", d.MountPath)
		if _, err := os.Stat(d.ConfigPath); err == nil {
			fmt.Printf("Config: %s\n", d.ConfigPath)
		}
	} else {
		fmt.Printf("Drive:  %s  [not mounted]\n", d.MountPath)
		if runtime.GOOS == "windows" {
			fmt.Println("Note: FUSE mounts are not supported on Windows.")
		} else {
			fmt.Println("Run 'zeadrive mount' to start volumez.")
		}
	}
	return nil
}

func (d *DriveManager) execMount(args []string) error {
	if runtime.GOOS == "windows" {
		return fmt.Errorf("zeadrive: FUSE mounts are not supported on Windows")
	}

	mountPoint := d.MountPath
	if len(args) > 0 {
		mountPoint = args[0]
	}

	if d.IsMounted() {
		fmt.Printf("already mounted at %s\n", mountPoint)
		return nil
	}

	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return fmt.Errorf("zeadrive mount: %w", err)
	}

	bin, err := exec.LookPath("volumez")
	if err != nil {
		return fmt.Errorf("zeadrive: volumez binary not found in PATH — build or install volumez first")
	}

	cmdArgs := []string{"-mount", mountPoint}
	if _, err := os.Stat(d.ConfigPath); err == nil {
		cmdArgs = append(cmdArgs, "-config", d.ConfigPath)
	}

	cmd := exec.Command(bin, cmdArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("zeadrive: failed to start volumez: %w", err)
	}
	// Release the process — volumez runs as a background daemon.
	if err := cmd.Process.Release(); err != nil {
		return fmt.Errorf("zeadrive: %w", err)
	}

	fmt.Printf("volumez started — mounting at %s\n", mountPoint)
	return nil
}

func (d *DriveManager) execUnmount() error {
	if runtime.GOOS == "windows" {
		return fmt.Errorf("zeadrive: FUSE mounts are not supported on Windows")
	}

	// On Linux prefer fusermount (no root required); fall back to umount.
	var cmd *exec.Cmd
	if runtime.GOOS != "darwin" {
		if fp, err := exec.LookPath("fusermount"); err == nil {
			cmd = exec.Command(fp, "-u", d.MountPath)
		}
	}
	if cmd == nil {
		cmd = exec.Command("umount", d.MountPath)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("zeadrive unmount: %w", err)
	}
	fmt.Printf("unmounted %s\n", d.MountPath)
	return nil
}

