package zeaberg

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	iceberg "github.com/apache/iceberg-go"
	"github.com/google/uuid"
	haavro "github.com/hamba/avro/v2"
)

// writeManifestFiles writes the manifest file and manifest list for a single
// Parquet data file being added as a new snapshot. Returns the manifest list path.
func writeManifestFiles(tableLocation string, schema *iceberg.Schema, snapshotID, seqNum, rowCount, fileSize int64, parquetPath string) (string, error) {
	metaDir := filepath.Join(tableLocation, "metadata")
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return "", err
	}

	// Build DataFile
	df, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData,
		parquetPath,
		iceberg.ParquetFile,
		map[int]any{},                  // no partition data
		map[int]haavro.LogicalType{},   // no logical type overrides
		map[int]int{},                  // no fixed-size fields
		rowCount,
		fileSize,
	)
	if err != nil {
		return "", fmt.Errorf("build data file: %w", err)
	}

	// Write manifest file
	manifestPath := filepath.Join(metaDir, fmt.Sprintf("%s-m0.avro", uuid.New().String()))
	mf, err := os.Create(manifestPath)
	if err != nil {
		return "", err
	}
	defer mf.Close()

	mw, err := iceberg.NewManifestWriter(2, mf, *iceberg.UnpartitionedSpec, schema, snapshotID)
	if err != nil {
		return "", fmt.Errorf("create manifest writer: %w", err)
	}

	snapID := snapshotID
	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, &seqNum, &seqNum, df.Build())
	if err := mw.Add(entry); err != nil {
		return "", fmt.Errorf("add manifest entry: %w", err)
	}
	if err := mw.Close(); err != nil {
		return "", fmt.Errorf("close manifest writer: %w", err)
	}
	mfStat, err := mf.Stat()
	if err != nil {
		return "", err
	}

	manifestFile, err := mw.ToManifestFile(manifestPath, mfStat.Size())
	if err != nil {
		return "", fmt.Errorf("to manifest file: %w", err)
	}

	// Write manifest list
	var listBuf bytes.Buffer
	mlw, err := iceberg.NewManifestListWriterV2(&listBuf, snapshotID, seqNum, nil)
	if err != nil {
		return "", fmt.Errorf("create manifest list writer: %w", err)
	}
	if err := mlw.AddManifests([]iceberg.ManifestFile{manifestFile}); err != nil {
		return "", fmt.Errorf("add manifest to list: %w", err)
	}
	if err := mlw.Close(); err != nil {
		return "", fmt.Errorf("close manifest list writer: %w", err)
	}

	listPath := filepath.Join(metaDir, fmt.Sprintf("snap-%d-%s.avro", snapshotID, uuid.New().String()))
	if err := os.WriteFile(listPath, listBuf.Bytes(), 0644); err != nil {
		return "", err
	}
	return listPath, nil
}

// newSnapshotID generates a snapshot ID from current time in milliseconds.
func newSnapshotID() int64 {
	return time.Now().UnixMilli()
}
