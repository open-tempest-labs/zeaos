package zeaberg

import (
	"fmt"
	"time"
)

// LineageInfo carries ZeaOS provenance metadata that gets embedded into
// Iceberg snapshot summary properties as "zea.*" keys.
type LineageInfo struct {
	SessionID  string       `json:"zea.session_id,omitempty"`
	SourceURIs []string     `json:"zea.source_uris,omitempty"`
	Chain      []ChainEntry `json:"zea.chain,omitempty"`
	PromotedAs string       `json:"zea.promoted_as,omitempty"`
	ExportedAt time.Time    `json:"zea.exported_at,omitempty"`
}

// ChainEntry records one step in the ZeaOS lineage chain.
type ChainEntry struct {
	Name      string `json:"name"`
	Operation string `json:"operation"` // "load" | "sql" | "pipe" | "copy"
	Parent    string `json:"parent,omitempty"`
	SourceURI string `json:"source_uri,omitempty"`
}

// snapshotProperties serializes LineageInfo into the flat string map that
// Iceberg stores in snapshot.summary.
func (l *LineageInfo) snapshotProperties() map[string]string {
	if l == nil {
		return nil
	}
	props := make(map[string]string)
	if l.SessionID != "" {
		props["zea.session_id"] = l.SessionID
	}
	if len(l.SourceURIs) > 0 {
		props["zea.source_uris"] = joinStrings(l.SourceURIs, ",")
	}
	if l.PromotedAs != "" {
		props["zea.promoted_as"] = l.PromotedAs
	}
	if !l.ExportedAt.IsZero() {
		props["zea.exported_at"] = l.ExportedAt.UTC().Format(time.RFC3339)
	}
	for i, entry := range l.Chain {
		prefix := fmt.Sprintf("zea.chain.%d", i)
		props[prefix+".name"] = entry.Name
		props[prefix+".op"] = entry.Operation
		if entry.Parent != "" {
			props[prefix+".parent"] = entry.Parent
		}
		if entry.SourceURI != "" {
			props[prefix+".uri"] = entry.SourceURI
		}
	}
	return props
}
