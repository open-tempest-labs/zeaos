# ZeaOS Installation Guide

## Homebrew (recommended)

```sh
brew tap open-tempest-labs/zeaos
brew install zeaos
```

This installs the `zeaos` binary. ZeaShell and Volumez are compiled into it — no separate installs needed.

Verify:

```sh
zeaos --version
```

---

## ZeaDrive Setup (macFUSE)

ZeaDrive is ZeaOS's mountable data volume, accessed via the `zea://` URL scheme. It is built on [Volumez](https://github.com/open-tempest-labs/volumez) which uses FUSE. On macOS, FUSE requires **macFUSE** and explicit system-level approval before it will work.

**ZeaDrive is optional but enables key features including cross-machine session portability** — the ability to resume a ZeaOS session with all its tables on a brand new machine by syncing to a mounted ZeaDrive volume.

### Step 1 — Install macFUSE

```sh
brew install --cask macfuse
```

### Step 2 — Approve the System Extension

macFUSE installs a kernel extension that macOS blocks by default. After installation you will see a notification that a system extension was blocked.

1. Open **System Settings → Privacy & Security**
2. Scroll down to the **Security** section
3. You will see: *"System software from developer 'Benjamin Fleischer' was blocked from loading"*
4. Click **Allow**
5. Enter your administrator password when prompted
6. **Restart your Mac** — the extension cannot load until after a reboot

> **Apple Silicon (M1/M2/M3/M4) only:** If you do not see the Allow button, your Mac may be in Full Security mode. You need to reduce the security policy to allow third-party kernel extensions:
>
> 1. Shut down your Mac
> 2. Hold the **power button** until you see "Loading startup options"
> 3. Select **Options → Continue** to enter Recovery Mode
> 4. In the menu bar: **Utilities → Startup Security Utility**
> 5. Select your startup disk and click **Security Policy**
> 6. Choose **Reduced Security** and check **"Allow user management of kernel extensions from identified developers"**
> 7. Click OK, enter your password, and restart
> 8. Repeat the Privacy & Security approval above

### Step 3 — Verify macFUSE is active

After rebooting:

```sh
/Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse --version
```

If this prints a version number, macFUSE is ready.

### Step 4 — Mount ZeaDrive

```sh
zeaos
```

```
ZeaOS> zeadrive mount
ZeaOS> zeadrive status
```

Or mount to a custom path:

```sh
ZeaOS> zeadrive mount ~/mydata
```

Once mounted, reference files with `zea://`:

```
ZeaOS> t = load zea://datasets/sales.parquet
ZeaOS> ls zea://datasets/
```

---

## Cross-Machine Session Portability

ZeaDrive is the foundation for resuming sessions across machines. When your tables are synced to a ZeaDrive volume, you can mount the same volume on any machine with ZeaOS installed and pick up where you left off.

This feature is under active development. The current workflow is:

1. Mount ZeaDrive on machine A and load/transform your data
2. Use `sync <table>` *(coming soon)* to push tables to the drive
3. Mount the same ZeaDrive volume on machine B
4. ZeaOS restores your session automatically on launch

---

## Building from Source

Requires Go 1.21+ and a C compiler (for DuckDB CGO).

```sh
git clone https://github.com/open-tempest-labs/zeaos
cd zeaos
go build -ldflags "-X main.version=$(git describe --tags --always)" -o zeaos ./cmd/zeaos
```

For ZeaDrive support, macFUSE must also be installed (see above).

---

## Troubleshooting

**`zeadrive mount` fails with "operation not permitted" or "dyld: Library not loaded"**
→ macFUSE is not installed or its system extension has not been approved. Follow the ZeaDrive setup steps above.

**"System software blocked" alert keeps reappearing after approval**
→ You need to reboot after approving. The kernel extension does not load until the next restart.

**Apple Silicon: no "Allow" button visible in Privacy & Security**
→ Your Mac is in Full Security mode. Follow the Apple Silicon Recovery Mode steps above to reduce the security policy.

**`zeaos --version` prints `dev`**
→ You built from source without `-ldflags`. Use the build command above to bake in the version.
