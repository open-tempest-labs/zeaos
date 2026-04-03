# ZeaOS Installation Guide

## Homebrew (recommended)

```sh
brew tap open-tempest-labs/zeaos
brew install zeaos
```

Verify:

```sh
zeaos --version
```

That's it. ZeaOS is ready to use. `zea://` local storage works immediately — no additional setup required.

---

## ZeaDrive — Local Storage (no setup required)

ZeaOS includes ZeaDrive out of the box. The `zea://` URL scheme resolves to `~/.zeaos/local/` on your machine — a local directory that ZeaOS creates automatically on first launch. No FUSE, no mounts, no configuration.

```
ZeaOS> t = load zea://datasets/sales.parquet
ZeaOS> ls zea://datasets/
ZeaOS> cp ~/downloads/data.csv zea://datasets/
```

Use `zeadrive status` to see the local path and any configured cloud backends:

```
ZeaOS> zeadrive status
Local:  zea:// → /Users/you/.zeaos/local
Drive:  /Users/you/zeadrive  [not mounted]
No cloud backends configured. Run 'enable-s3' to add one.
```

---

## ZeaDrive — Cloud Backends (S3-compatible)

Cloud backends extend `zea://` to remote storage via [Volumez](https://github.com/open-tempest-labs/volumez) FUSE. This enables cross-machine session portability — load and transform data on one machine, sync tables to S3, resume on any other machine with ZeaOS installed.

Cloud backends require three additional things: **Volumez**, **macFUSE**, and **AWS credentials**.

### Step 1 — Install Volumez

```sh
brew tap open-tempest-labs/volumez
brew install volumez
```

### Step 2 — Install macFUSE

```sh
brew install --cask macfuse
```

### Step 3 — Approve the macFUSE System Extension

macFUSE installs a kernel extension that macOS blocks by default.

1. Open **System Settings → Privacy & Security**
2. Scroll down to the **Security** section
3. You will see: *"System software from developer 'Benjamin Fleischer' was blocked from loading"*
4. Click **Allow**
5. Enter your administrator password
6. **Restart your Mac** — the extension will not load until after a reboot

> **Apple Silicon (M1/M2/M3/M4) — if you do not see the Allow button:**
> Your Mac is in Full Security mode. You need to reduce the startup security policy:
>
> 1. Shut down your Mac
> 2. Hold the **power button** until you see "Loading startup options"
> 3. Select **Options → Continue** to enter Recovery Mode
> 4. In the menu bar: **Utilities → Startup Security Utility**
> 5. Select your startup disk → **Security Policy**
> 6. Choose **Reduced Security** and check **"Allow user management of kernel extensions from identified developers"**
> 7. Click OK, enter your password, and restart
> 8. Repeat the Privacy & Security approval above

### Step 4 — Verify macFUSE is active

```sh
/Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse --version
```

If this prints a version number, macFUSE is ready.

### Step 5 — Configure AWS Credentials

Volumez uses the standard AWS credential chain. If you already have `~/.aws/credentials` configured (e.g. from the AWS CLI), nothing more is needed.

If not, either run `aws configure` or create the file manually:

```
~/.aws/credentials:

[default]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
```

For S3-compatible storage (MinIO, Cloudflare R2, etc.), the endpoint URL is set during `enable-s3` configuration — credentials still go in `~/.aws/credentials`.

### Step 6 — Configure the S3 Backend in ZeaOS

Run ZeaOS and use the `enable-s3` command to open the configuration form:

```
zeaos
ZeaOS> enable-s3
```

The form collects:

| Field | Description |
|-------|-------------|
| Mount name | The `zea://` prefix for this backend (default: `s3-data`) |
| Bucket | Your S3 bucket name |
| Region | AWS region (default: `us-east-1`) |
| Prefix | Optional key prefix within the bucket |
| Endpoint URL | Optional — for S3-compatible endpoints (MinIO, R2, etc.) |

After saving, `~/.zeaos/volumez.json` is written. Cloud paths mount automatically on first access:

```
ZeaOS> t = load zea://s3-data/warehouse/sales.parquet
volumez started — mounting at /Users/you/zeadrive
→ t: 2_847_103 rows × 14 cols
```

You can also mount explicitly:

```
ZeaOS> zeadrive mount
ZeaOS> zeadrive status
```

ZeaOS owns the Volumez process and unmounts cleanly on exit.

---

## Cross-Machine Session Portability

ZeaDrive cloud backends are the foundation for resuming sessions across machines. The workflow:

1. Load and transform data on machine A
2. `sync <table>` *(coming soon)* — push tables to `zea://s3-data/`
3. On machine B: `brew install zeaos`, run `enable-s3` with the same bucket
4. ZeaOS restores your session automatically on launch

---

## Building from Source

Requires Go 1.21+ and a C compiler (for DuckDB CGO).

```sh
git clone https://github.com/open-tempest-labs/zeaos
cd zeaos
go build -ldflags "-X main.version=$(git describe --tags --always)" -o zeaos ./cmd/zeaos
```

For cloud ZeaDrive support, also install Volumez and macFUSE (see above).

---

## Troubleshooting

**`zea://` paths return "no such file or directory"**
→ The path doesn't exist yet under `~/.zeaos/local/`. Create it or copy files there first.

**`enable-s3` saved but `zea://s3-data/` says "no cloud config"**
→ Check `~/.zeaos/volumez.json` exists and contains your mount. Run `zeadrive status`.

**`zeadrive mount` fails with "volumez not found"**
→ Install Volumez: `brew tap open-tempest-labs/volumez && brew install volumez`

**`zeadrive mount` fails with "operation not permitted"**
→ macFUSE is not installed or its system extension has not been approved. Follow the macFUSE steps above.

**"System software blocked" alert reappears after approval**
→ You need to reboot after approving. The kernel extension does not load until the next restart.

**Apple Silicon: no "Allow" button in Privacy & Security**
→ Your Mac is in Full Security mode. Follow the Apple Silicon Recovery Mode steps above.

**Mount point appears mounted but files are inaccessible (stale mount)**
→ The Volumez process was killed without a clean unmount. ZeaOS detects and cleans this up automatically on next launch. Or run `diskutil unmount force ~/zeadrive` manually.

**`zeaos --version` prints `dev`**
→ You built from source without `-ldflags`. Use the build command above to bake in the version.
