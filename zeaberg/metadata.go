package zeaberg

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	iceberg "github.com/apache/iceberg-go"
)

const icebergFormatVersion = 2

// tableMetadata is the on-disk representation of an Iceberg v2 table metadata file.
type tableMetadata struct {
	FormatVersion   int                `json:"format-version"`
	TableUUID       string             `json:"table-uuid"`
	Location        string             `json:"location"`
	LastUpdatedMs   int64              `json:"last-updated-ms"`
	LastColumnID    int                `json:"last-column-id"`
	Schemas         []*iceberg.Schema  `json:"schemas"`
	CurrentSchemaID int                `json:"current-schema-id"`
	PartitionSpecs []partitionSpec    `json:"partition-specs"`
	DefaultSpecID  int                `json:"default-spec-id"`
	LastPartitionID int               `json:"last-partition-id"`
	SortOrders     []sortOrder        `json:"sort-orders"`
	DefaultSortOrderID int            `json:"default-sort-order-id"`
	Snapshots      []snapshotMeta     `json:"snapshots"`
	CurrentSnapshotID *int64          `json:"current-snapshot-id,omitempty"`
	SnapshotLog    []snapshotLogEntry `json:"snapshot-log"`
	MetadataLog    []metadataLogEntry `json:"metadata-log"`
	Properties     map[string]string  `json:"properties,omitempty"`
}

type partitionSpec struct {
	SpecID int           `json:"spec-id"`
	Fields []interface{} `json:"fields"` // empty for unpartitioned
}

type sortOrder struct {
	OrderID int           `json:"order-id"`
	Fields  []interface{} `json:"fields"` // empty for unsorted
}

type snapshotMeta struct {
	SnapshotID   int64             `json:"snapshot-id"`
	TimestampMs  int64             `json:"timestamp-ms"`
	ManifestList string            `json:"manifest-list"`
	Summary      map[string]string `json:"summary"`
	SchemaID     *int              `json:"schema-id,omitempty"`
}

type snapshotLogEntry struct {
	TimestampMs int64 `json:"timestamp-ms"`
	SnapshotID  int64 `json:"snapshot-id"`
}

type metadataLogEntry struct {
	TimestampMs  int64  `json:"timestamp-ms"`
	MetadataFile string `json:"metadata-file"`
}

// newTableMetadata creates a fresh metadata struct for a new table.
func newTableMetadata(location, tableUUID string, schema *iceberg.Schema) *tableMetadata {
	return &tableMetadata{
		FormatVersion:   icebergFormatVersion,
		TableUUID:       tableUUID,
		Location:        location,
		LastUpdatedMs:   time.Now().UnixMilli(),
		LastColumnID:    schema.NumFields(),
		Schemas:         []*iceberg.Schema{schema},
		CurrentSchemaID: 0,
		PartitionSpecs:  []partitionSpec{{SpecID: 0, Fields: []interface{}{}}},
		DefaultSpecID:   0,
		LastPartitionID: 999,
		SortOrders:      []sortOrder{{OrderID: 0, Fields: []interface{}{}}},
		DefaultSortOrderID: 0,
		Snapshots:       []snapshotMeta{},
		SnapshotLog:     []snapshotLogEntry{},
		MetadataLog:     []metadataLogEntry{},
	}
}

// readMetadata loads the current table metadata from the metadata directory.
func readMetadata(tableLocation string) (*tableMetadata, string, error) {
	hint, err := readVersionHint(tableLocation)
	if err != nil {
		return nil, "", err
	}
	metaPath := filepath.Join(tableLocation, "metadata", fmt.Sprintf("v%d.metadata.json", hint))
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, "", fmt.Errorf("read metadata: %w", err)
	}
	var meta tableMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, "", fmt.Errorf("parse metadata: %w", err)
	}
	return &meta, metaPath, nil
}

// writeMetadata writes a new versioned metadata file and updates version-hint.text.
func writeMetadata(tableLocation string, meta *tableMetadata, version int) (string, error) {
	metaDir := filepath.Join(tableLocation, "metadata")
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return "", err
	}
	meta.LastUpdatedMs = time.Now().UnixMilli()
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return "", err
	}
	metaFile := filepath.Join(metaDir, fmt.Sprintf("v%d.metadata.json", version))
	if err := os.WriteFile(metaFile, data, 0644); err != nil {
		return "", err
	}
	if err := writeVersionHint(tableLocation, version); err != nil {
		return "", err
	}
	return metaFile, nil
}

// readVersionHint reads the current metadata version from version-hint.text.
// The file may be padded with trailing spaces (see writeVersionHint).
func readVersionHint(tableLocation string) (int, error) {
	hintPath := filepath.Join(tableLocation, "metadata", "version-hint.text")
	data, err := os.ReadFile(hintPath)
	if err != nil {
		return 0, fmt.Errorf("version-hint.text not found: %w", err)
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("invalid version-hint.text: %w", err)
	}
	return v, nil
}

// writeVersionHint writes the current metadata version to version-hint.text.
// The value is left-justified and space-padded to 64 bytes so that object
// storage backends with a minimum file size (e.g. Volumez via FUSE) do not
// silently discard the write. All standard Iceberg readers (DuckDB, PyIceberg,
// Java) parse with trim/strip, so the padding is transparent to consumers.
func writeVersionHint(tableLocation string, version int) error {
	hintPath := filepath.Join(tableLocation, "metadata", "version-hint.text")
	const minSize = 64
	content := fmt.Sprintf("%-*d\n", minSize, version)
	return os.WriteFile(hintPath, []byte(content), 0644)
}
