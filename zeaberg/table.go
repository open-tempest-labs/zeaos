package zeaberg

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	iceberg "github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// Table represents an Iceberg table at a filesystem location.
type Table struct {
	Location string
	schema   *iceberg.Schema
	meta     *tableMetadata
	version  int
}

// TableOption configures CreateTable.
type TableOption func(*tableConfig)

type tableConfig struct {
	canonicalLocation string // overrides the location field in metadata (e.g. final S3 path)
}

// WithCanonicalLocation sets the location written into the Iceberg metadata,
// overriding the directory where files are physically written. Use this when
// staging locally before copying to a remote path (ZeaDrive, S3) so that
// readers resolve file paths against the final destination.
func WithCanonicalLocation(loc string) TableOption {
	return func(c *tableConfig) { c.canonicalLocation = loc }
}

// CreateTable initializes a new Iceberg table at location using the given Arrow schema.
// Returns an error if a table already exists at that location.
func CreateTable(location string, arrowSchema *arrow.Schema, opts ...TableOption) (*Table, error) {
	cfg := &tableConfig{}
	for _, o := range opts {
		o(cfg)
	}
	metaLocation := location
	if cfg.canonicalLocation != "" {
		metaLocation = cfg.canonicalLocation
	}

	_, err := readVersionHint(location)
	if err == nil {
		return nil, fmt.Errorf("table already exists at %q: use OpenTable", location)
	}

	schema, err := SchemaFromArrow(arrowSchema)
	if err != nil {
		return nil, fmt.Errorf("convert schema: %w", err)
	}

	tableUUID := uuid.New().String()
	meta := newTableMetadata(metaLocation, tableUUID, schema)

	if err := os.MkdirAll(filepath.Join(location, "data"), 0755); err != nil {
		return nil, err
	}
	if _, err := writeMetadata(location, meta, 1); err != nil {
		return nil, err
	}
	return &Table{Location: location, schema: schema, meta: meta, version: 1}, nil
}

// OpenTable opens an existing Iceberg table at location.
func OpenTable(location string) (*Table, error) {
	meta, _, err := readMetadata(location)
	if err != nil {
		return nil, fmt.Errorf("open table at %q: %w", location, err)
	}
	hint, _ := readVersionHint(location)
	var schema *iceberg.Schema
	if len(meta.Schemas) > 0 {
		schema = meta.Schemas[0]
	}
	return &Table{Location: location, schema: schema, meta: meta, version: hint}, nil
}

// SnapshotOption configures an AppendSnapshot call.
type SnapshotOption func(*snapshotConfig)

type snapshotConfig struct {
	lineage      *LineageInfo
	operation    string
	externalPath string // if set, register this path in manifest without copying
}

// WithLineage attaches ZeaOS lineage metadata to the snapshot.
func WithLineage(l *LineageInfo) SnapshotOption {
	return func(c *snapshotConfig) { c.lineage = l }
}

// WithSessionID sets the ZeaOS session ID on the snapshot.
func WithSessionID(id string) SnapshotOption {
	return func(c *snapshotConfig) {
		if c.lineage == nil {
			c.lineage = &LineageInfo{}
		}
		c.lineage.SessionID = id
	}
}

// WithSourceURIs sets the originating source URIs on the snapshot.
func WithSourceURIs(uris ...string) SnapshotOption {
	return func(c *snapshotConfig) {
		if c.lineage == nil {
			c.lineage = &LineageInfo{}
		}
		c.lineage.SourceURIs = append(c.lineage.SourceURIs, uris...)
	}
}

// WithExternalPath registers the given path in the manifest instead of copying
// the source file into the table's data/ directory. The caller is responsible
// for ensuring the file exists at externalPath before readers access the table.
// Use this when streaming large files to a remote destination (e.g. FUSE/S3)
// to avoid a redundant local copy.
func WithExternalPath(path string) SnapshotOption {
	return func(c *snapshotConfig) { c.externalPath = path }
}

// WithPromotedAs records the alias under which this table was promoted in ZeaOS.
func WithPromotedAs(name string) SnapshotOption {
	return func(c *snapshotConfig) {
		if c.lineage == nil {
			c.lineage = &LineageInfo{}
		}
		c.lineage.PromotedAs = name
	}
}

// AppendSnapshot copies srcParquetPath into the table's data/ directory and
// registers it as a new snapshot. srcParquetPath may be any readable path;
// zeaberg owns the destination.
func (t *Table) AppendSnapshot(srcParquetPath string, rowCount int64, opts ...SnapshotOption) error {
	cfg := &snapshotConfig{operation: "append"}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.lineage != nil && cfg.lineage.ExportedAt.IsZero() {
		cfg.lineage.ExportedAt = time.Now().UTC()
	}

	snapshotID := newSnapshotID()
	t.meta.LastSequenceNumber++
	seqNum := t.meta.LastSequenceNumber

	var dataPath string
	if cfg.externalPath != "" {
		// Caller manages the file at externalPath — just register it.
		dataPath = cfg.externalPath
	} else {
		// Copy source file into <location>/data/<snapshotID>.parquet
		dataDir := filepath.Join(t.Location, "data")
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return err
		}
		dataPath = filepath.Join(dataDir, fmt.Sprintf("%d.parquet", snapshotID))
		if err := copyFile(srcParquetPath, dataPath); err != nil {
			return fmt.Errorf("copy parquet to data dir: %w", err)
		}
	}

	stat, err := os.Stat(srcParquetPath) // size from source regardless of mode
	if err != nil {
		return fmt.Errorf("stat parquet file: %w", err)
	}

	listPath, err := writeManifestFiles(t.Location, t.schema, snapshotID, seqNum, rowCount, stat.Size(), dataPath)
	if err != nil {
		return fmt.Errorf("write manifest files: %w", err)
	}

	// Hash the source file — always the local copy regardless of externalPath,
	// since it's the same bytes and the source is guaranteed readable here.
	dataHash, hashErr := hashFile(srcParquetPath)

	summary := map[string]string{
		"operation":        cfg.operation,
		"added-data-files": "1",
		"added-records":    fmt.Sprintf("%d", rowCount),
		"zea.data_file":    dataPath,
	}
	if hashErr == nil {
		summary["zea.data_sha256"] = dataHash
	}
	if cfg.lineage != nil {
		for k, v := range cfg.lineage.snapshotProperties() {
			summary[k] = v
		}
	}

	schemaID := t.meta.CurrentSchemaID
	snap := snapshotMeta{
		SnapshotID:   snapshotID,
		TimestampMs:  time.Now().UnixMilli(),
		ManifestList: listPath,
		Summary:      summary,
		SchemaID:     &schemaID,
	}

	t.meta.Snapshots = append(t.meta.Snapshots, snap)
	t.meta.CurrentSnapshotID = &snapshotID
	t.meta.SnapshotLog = append(t.meta.SnapshotLog, snapshotLogEntry{
		TimestampMs: snap.TimestampMs,
		SnapshotID:  snapshotID,
	})

	prevMeta := filepath.Join(t.Location, "metadata", fmt.Sprintf("v%d.metadata.json", t.version))
	t.meta.MetadataLog = append(t.meta.MetadataLog, metadataLogEntry{
		TimestampMs:  snap.TimestampMs,
		MetadataFile: prevMeta,
	})

	t.version++
	if _, err := writeMetadata(t.Location, t.meta, t.version); err != nil {
		return fmt.Errorf("write metadata: %w", err)
	}
	return nil
}

// CurrentSnapshotID returns the current snapshot ID, or an error if no snapshots exist.
func (t *Table) CurrentSnapshotID() (int64, error) {
	if t.meta.CurrentSnapshotID == nil {
		return 0, errors.New("table has no snapshots")
	}
	return *t.meta.CurrentSnapshotID, nil
}

// SnapshotCount returns the number of snapshots on the table.
func (t *Table) SnapshotCount() int {
	return len(t.meta.Snapshots)
}
