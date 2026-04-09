// Package zeaberg writes lineage-aware Apache Iceberg tables.
//
// Zeaberg extends the Iceberg snapshot model with ZeaOS provenance metadata:
// session IDs, source URIs, and lineage chains are embedded in snapshot
// summary properties and survive into any Iceberg-compatible reader.
//
// Basic usage:
//
//	tbl, err := zeaberg.CreateTable("/path/to/table", arrowSchema)
//	err = tbl.AppendSnapshot("data.parquet", rowCount,
//	    zeaberg.WithSessionID("sess-123"),
//	    zeaberg.WithSourceURIs("https://example.com/data.parquet"),
//	    zeaberg.WithLineage(lineage),
//	)
package zeaberg
