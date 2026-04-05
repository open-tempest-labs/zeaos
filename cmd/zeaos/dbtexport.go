package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode"
)

// PromotedArtifact records a table marked for export.
type PromotedArtifact struct {
	ExportName   string    `json:"export_name"`
	Kind         string    `json:"kind"`          // "model" | "semantic"
	PromotedFrom string    `json:"promoted_from"` // session table name
	PromotedAt   time.Time `json:"promoted_at"`
}

// parseExportArgs extracts --target=FORMAT, --output=DIR / -o DIR from args.
// Returns the format, output path, and remaining positional args.
func parseExportArgs(args []string) (format, output string, rest []string) {
	for i := 0; i < len(args); i++ {
		switch {
		case strings.HasPrefix(args[i], "--target="):
			format = strings.TrimPrefix(args[i], "--target=")
		case strings.HasPrefix(args[i], "--output="):
			output = strings.TrimPrefix(args[i], "--output=")
		case (args[i] == "-o" || args[i] == "--output") && i+1 < len(args):
			i++
			output = args[i]
		default:
			rest = append(rest, args[i])
		}
	}
	return
}

// --- promote ---

func execPromote(args []string, s *Session) error {
	if len(args) == 0 {
		return fmt.Errorf("promote: table name required")
	}
	tableName := args[0]
	if _, err := s.Get(tableName); err != nil {
		return err
	}

	exportName := tableName
	kind := "model"

	// Parse: [as <name>] [model|semantic]
	rest := args[1:]
	if len(rest) > 0 && rest[0] == "as" {
		if len(rest) < 2 {
			return fmt.Errorf("promote: expected name after 'as'")
		}
		exportName = rest[1]
		rest = rest[2:]
	}
	if len(rest) > 0 {
		switch rest[0] {
		case "model", "semantic":
			kind = rest[0]
		default:
			return fmt.Errorf("promote: unknown kind %q — use 'model' or 'semantic'", rest[0])
		}
	}

	if !isValidDbtName(exportName) {
		return fmt.Errorf("promote: %q is not a valid dbt model name (lowercase, alphanumeric + underscores, start with letter)", exportName)
	}

	s.Promoted[exportName] = &PromotedArtifact{
		ExportName:   exportName,
		Kind:         kind,
		PromotedFrom: tableName,
		PromotedAt:   time.Now(),
	}
	fmt.Printf("promoted %s → %s (%s)\n", tableName, exportName, kind)
	return nil
}

// --- list ---

// execList shows session tables (default) or promoted artifacts (--type=promotions).
func execList(args []string, s *Session) error {
	listType := "tables"
	for _, a := range args {
		if strings.HasPrefix(a, "--type=") {
			listType = strings.TrimPrefix(a, "--type=")
		}
	}
	switch listType {
	case "promotions":
		return execListPromotions(s)
	default:
		return execListTables(s)
	}
}

func execListTables(s *Session) error {
	if len(s.Registry) == 0 {
		fmt.Println("(no tables in session)")
		return nil
	}
	fmt.Printf("%-24s  %10s  %6s  %s\n", "Table", "Rows", "Cols", "Ops")
	fmt.Println(strings.Repeat("─", 70))
	for _, e := range s.Registry {
		ops := strings.Join(e.Ops, " | ")
		fmt.Printf("%-24s  %10d  %6d  %s\n", e.Name, e.RowCount, e.ColCount, ops)
	}
	return nil
}

func execListPromotions(s *Session) error {
	if len(s.Promoted) == 0 {
		fmt.Println("No promoted artifacts. Use 'promote <table> [as <name>]' first.")
		return nil
	}
	fmt.Printf("%-24s  %-10s  %-20s  %s\n", "Export Name", "Kind", "Source Table", "Promoted At")
	fmt.Println(strings.Repeat("─", 72))
	for _, a := range s.Promoted {
		fmt.Printf("%-24s  %-10s  %-20s  %s\n",
			a.ExportName, a.Kind, a.PromotedFrom,
			a.PromotedAt.Format("2006-01-02 15:04:05"))
	}
	return nil
}

// --- validate ---

func execValidate(args []string, s *Session) error {
	format, _, rest := parseExportArgs(args)
	if format != "" && format != "dbt" {
		return fmt.Errorf("validate: unsupported target %q (supported: dbt)", format)
	}
	if len(rest) == 0 {
		if len(s.Promoted) == 0 {
			fmt.Println("No promoted artifacts to validate.")
			return nil
		}
		for name := range s.Promoted {
			if err := validateArtifact(name, s); err != nil {
				fmt.Printf("✗ %s: %v\n", name, err)
			}
		}
		return nil
	}
	return validateArtifact(rest[0], s)
}

func validateArtifact(exportName string, s *Session) error {
	art, ok := s.Promoted[exportName]
	if !ok {
		return fmt.Errorf("validate: %q not found in promoted artifacts", exportName)
	}

	fmt.Printf("Validating %s (from %s)...\n", exportName, art.PromotedFrom)

	chain, err := walkLineage(s, art.PromotedFrom)
	if err != nil {
		return err
	}

	sql, warnings, err := reconstructSQL(chain, s)
	if err != nil {
		fmt.Printf("  ✗ SQL reconstruction failed: %v\n", err)
		return err
	}

	// Parse-check the reconstructed SQL via DuckDB EXPLAIN.
	// Strip dbt Jinja templating for the parse check.
	checkSQL := stripJinja(sql)
	ctx := context.Background()
	_, execErr := s.arrowConn.ExecContext(ctx, "EXPLAIN "+checkSQL)
	if execErr != nil {
		fmt.Printf("  ✗ SQL does not parse: %v\n", execErr)
	} else {
		fmt.Printf("  ✓ SQL parses correctly\n")
	}

	entry, _ := s.Get(art.PromotedFrom)
	if entry != nil {
		fmt.Printf("  ✓ %d rows × %d cols\n", entry.RowCount, entry.ColCount)
	}

	if len(chain.SourceURIs) > 0 {
		fmt.Printf("  ✓ source URIs: %s\n", strings.Join(chain.SourceURIs, ", "))
	} else {
		fmt.Printf("  ⚠  no source URIs found (re-load table to capture)\n")
	}

	portable := true
	for _, w := range warnings {
		fmt.Printf("  ⚠  %s\n", w)
		portable = false
	}
	for _, issue := range chain.Issues {
		fmt.Printf("  ✗ %s\n", issue)
		portable = false
	}
	if portable {
		fmt.Printf("  ✓ portable: duckdb dialect, no non-standard functions detected\n")
	}

	return nil
}

// --- export ---

func execExport(args []string, s *Session) error {
	if len(s.Promoted) == 0 {
		fmt.Println("No promoted artifacts. Use 'promote <table> [as <name>]' first.")
		return nil
	}

	format, outDir, rest := parseExportArgs(args)
	if format == "" {
		return fmt.Errorf("export: --target=<format> required  (e.g. --target=dbt -o ./my-project)")
	}
	if format != "dbt" {
		return fmt.Errorf("export: unsupported target %q (supported: dbt)", format)
	}
	if outDir == "" {
		outDir = "zea-dbt-export"
	}

	var exportName string
	for _, a := range rest {
		if !strings.HasPrefix(a, "--") {
			exportName = a
			break
		}
	}

	var artifacts []*PromotedArtifact
	if exportName != "" {
		art, ok := s.Promoted[exportName]
		if !ok {
			return fmt.Errorf("export: %q not found in promoted artifacts", exportName)
		}
		artifacts = []*PromotedArtifact{art}
	} else {
		for _, a := range s.Promoted {
			artifacts = append(artifacts, a)
		}
	}

	// Create directory structure.
	for _, dir := range []string{
		outDir,
		filepath.Join(outDir, "models"),
		filepath.Join(outDir, "sources"),
		filepath.Join(outDir, "seeds"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("export: %w", err)
		}
	}

	type manifestArtifact struct {
		Name            string            `json:"name"`
		Kind            string            `json:"kind"`
		SourceURIs      []string          `json:"source_uris"`
		Transformations []map[string]any  `json:"transformations"`
		Columns         int               `json:"columns"`
		RowCount        int64             `json:"row_count"`
		Dialect         string            `json:"dialect"`
		Portable        bool              `json:"portable"`
	}

	var manifestArtifacts []manifestArtifact
	var allSources []sourceEntry // collected across all artifacts

	for _, art := range artifacts {
		chain, err := walkLineage(s, art.PromotedFrom)
		if err != nil {
			fmt.Printf("⚠  skipping %s: %v\n", art.ExportName, err)
			continue
		}

		sql, warnings, err := reconstructSQL(chain, s)
		if err != nil {
			fmt.Printf("⚠  skipping %s: SQL reconstruction failed: %v\n", art.ExportName, err)
			continue
		}

		entry, _ := s.Get(art.PromotedFrom)
		portable := len(warnings) == 0 && len(chain.Issues) == 0

		// models/NAME.sql
		sqlPath := filepath.Join(outDir, "models", art.ExportName+".sql")
		if err := os.WriteFile(sqlPath, []byte(sql+"\n"), 0o644); err != nil {
			return fmt.Errorf("export: %w", err)
		}
		fmt.Printf("  created %s\n", sqlPath)

		// models/NAME.yml
		ymlPath := filepath.Join(outDir, "models", art.ExportName+".yml")
		ymlContent := buildModelYAML(art, entry, warnings)
		if err := os.WriteFile(ymlPath, []byte(ymlContent), 0o644); err != nil {
			return fmt.Errorf("export: %w", err)
		}
		fmt.Printf("  created %s\n", ymlPath)

		// Collect sources for this artifact.
		for _, uri := range chain.SourceURIs {
			sn, tbl, desc := mapURIToSource(uri)
			allSources = append(allSources, sourceEntry{SourceName: sn, TableName: tbl, Description: desc, URI: uri})
		}

		// Build manifest entry.
		var rowCount int64
		var colCount int
		if entry != nil {
			rowCount = entry.RowCount
			colCount = entry.ColCount
		}
		transformations := buildTransformations(chain)
		manifestArtifacts = append(manifestArtifacts, manifestArtifact{
			Name:            art.ExportName,
			Kind:            art.Kind,
			SourceURIs:      chain.SourceURIs,
			Transformations: transformations,
			Columns:         colCount,
			RowCount:        rowCount,
			Dialect:         "duckdb",
			Portable:        portable,
		})
	}

	// sources/zea_sources.yml
	if len(allSources) > 0 {
		sourcesPath := filepath.Join(outDir, "sources", "zea_sources.yml")
		sourcesContent := buildSourcesYAML(allSources)
		if err := os.WriteFile(sourcesPath, []byte(sourcesContent), 0o644); err != nil {
			return fmt.Errorf("export: %w", err)
		}
		fmt.Printf("  created %s\n", sourcesPath)
	}

	// dbt_project.yml
	projectPath := filepath.Join(outDir, "dbt_project.yml")
	if err := os.WriteFile(projectPath, []byte(dbtProjectYML()), 0o644); err != nil {
		return fmt.Errorf("export: %w", err)
	}
	fmt.Printf("  created %s\n", projectPath)

	// profiles.yml
	profilesPath := filepath.Join(outDir, "profiles.yml")
	if err := os.WriteFile(profilesPath, []byte(dbtProfilesYML()), 0o644); err != nil {
		return fmt.Errorf("export: %w", err)
	}
	fmt.Printf("  created %s\n", profilesPath)

	// zea_export.json
	manifest := map[string]any{
		"version":    "0.2.0",
		"session_id": fmt.Sprintf("zea-%s", time.Now().Format("2006-01-02-1504")),
		"exported":   time.Now().UTC().Format(time.RFC3339),
		"user":       os.Getenv("USER"),
		"artifacts":  manifestArtifacts,
	}
	manifestData, _ := json.MarshalIndent(manifest, "", "  ")
	manifestPath := filepath.Join(outDir, "zea_export.json")
	if err := os.WriteFile(manifestPath, manifestData, 0o644); err != nil {
		return fmt.Errorf("export: %w", err)
	}
	fmt.Printf("  created %s\n", manifestPath)

	fmt.Printf("\nExported %d artifact(s) to %s/\n", len(manifestArtifacts), outDir)
	fmt.Println("Next steps:")
	fmt.Printf("  cd %s\n", outDir)
	fmt.Println("  pip install dbt-duckdb   # if not already installed")
	fmt.Println("  dbt debug --profiles-dir .")
	fmt.Println("  dbt run")
	return nil
}

// --- Lineage walker ---

// LineageNode represents one table in the ancestry chain.
type LineageNode struct {
	Entry    *TableEntry
	NodeKind string    // "load" | "sql" | "pipe" | "copy" | "zearun" | "unknown"
	PipeOps  []PipeOp  // decoded for "pipe" nodes
}

// LineageChain is the root-first ancestry chain of a promoted table.
type LineageChain struct {
	Nodes      []*LineageNode
	SourceURIs []string
	Issues     []string
}

func walkLineage(s *Session, tableName string) (*LineageChain, error) {
	chain := &LineageChain{}
	visited := map[string]bool{}
	current := tableName

	for current != "" {
		if visited[current] {
			chain.Issues = append(chain.Issues, "cycle detected at table "+current)
			break
		}
		visited[current] = true

		entry, ok := s.Registry[current]
		if !ok {
			chain.Issues = append(chain.Issues, "missing ancestor: "+current+" (was it dropped?)")
			break
		}

		node := classifyNode(entry)
		// Prepend to build root-first order.
		chain.Nodes = append([]*LineageNode{node}, chain.Nodes...)

		if node.NodeKind == "load" {
			uri := entry.SourceURI
			if uri == "" {
				// Legacy: derive from ops label.
				uri = extractLoadLabel(entry.Ops)
			}
			if uri != "" {
				chain.SourceURIs = append([]string{uri}, chain.SourceURIs...)
			}
		}
		if node.NodeKind == "zearun" {
			chain.Issues = append(chain.Issues,
				"table "+current+" was produced by a zeaplugin (non-portable)")
		}

		current = entry.Parent
	}

	return chain, nil
}

func classifyNode(entry *TableEntry) *LineageNode {
	node := &LineageNode{Entry: entry}
	for _, op := range entry.Ops {
		switch {
		case strings.HasPrefix(op, "load("):
			node.NodeKind = "load"
		case op == "sql":
			node.NodeKind = "sql"
		case strings.HasPrefix(op, "zearun("):
			node.NodeKind = "zearun"
		case op == "copy":
			node.NodeKind = "copy"
		default:
			node.NodeKind = "pipe"
			node.PipeOps = decodeOps(entry.Ops)
		}
		return node
	}
	node.NodeKind = "unknown"
	return node
}

// decodeOps parses stored op strings like "where(amount > 100)", "group(col)"
// back into PipeOp structs. The format is kind(args) where args may itself
// contain parentheses — only the outermost closing ) is stripped.
func decodeOps(ops []string) []PipeOp {
	var result []PipeOp
	for _, op := range ops {
		lp := strings.Index(op, "(")
		if lp < 0 {
			continue
		}
		kind := op[:lp]
		args := op[lp+1:]
		args = strings.TrimSuffix(args, ")")
		switch kind {
		case "where", "group", "select", "top", "pivot":
			result = append(result, PipeOp{Kind: kind, Args: args})
		}
	}
	return result
}

func extractLoadLabel(ops []string) string {
	for _, op := range ops {
		if strings.HasPrefix(op, "load(") {
			return strings.TrimSuffix(op[5:], ")")
		}
	}
	return ""
}

// --- SQL reconstruction ---

func reconstructSQL(chain *LineageChain, s *Session) (sql string, warnings []string, err error) {
	if len(chain.Nodes) == 0 {
		return "", nil, fmt.Errorf("empty lineage chain")
	}

	// Find the promoted (leaf) node — last in root-first order.
	leaf := chain.Nodes[len(chain.Nodes)-1]

	switch leaf.NodeKind {
	case "sql":
		sql, warnings, err = reconstructZeaqlSQL(leaf, s)
	case "load", "pipe", "copy":
		sql, warnings, err = reconstructPipeSQL(chain)
	default:
		return "", nil, fmt.Errorf("cannot reconstruct SQL for node kind %q", leaf.NodeKind)
	}
	return
}

// reconstructZeaqlSQL takes a zeaql-sourced node and rewrites session table
// references to dbt source() / ref() Jinja calls.
func reconstructZeaqlSQL(node *LineageNode, s *Session) (string, []string, error) {
	if node.Entry.SourceSQL == "" {
		return "", nil, fmt.Errorf("table %q has no stored SQL (was it created with zeaql?)", node.Entry.Name)
	}

	sql := node.Entry.SourceSQL
	var warnings []string

	// Find session table names referenced in the SQL and substitute them.
	for name, entry := range s.Registry {
		if !sqlReferencesTable(sql, name) {
			continue
		}
		var replacement string
		if entry.SourceURI != "" || isLoadNode(entry) {
			// Root source table.
			sn, tbl, _ := mapURIToSource(entry.SourceURI)
			if entry.SourceURI == "" {
				tbl = sanitizeDbtName(strings.TrimSuffix(extractLoadLabel(entry.Ops), filepath.Ext(extractLoadLabel(entry.Ops))))
				sn = "zea_local"
			}
			replacement = fmt.Sprintf("{{ source('%s', '%s') }}", sn, tbl)
		} else {
			// Derived table — reference as a dbt model.
			replacement = fmt.Sprintf("{{ ref('%s') }}", name)
		}
		sql = replaceTableRef(sql, name, replacement)
	}

	w, _ := checkPortability(sql)
	warnings = append(warnings, w...)

	header := "{{ config(materialized='table') }}\n\n"
	return header + sql, warnings, nil
}

// reconstructPipeSQL rebuilds SQL from a pipe-chain lineage, rooted at a
// dbt source() reference.
func reconstructPipeSQL(chain *LineageChain) (string, []string, error) {
	if len(chain.Nodes) == 0 {
		return "", nil, fmt.Errorf("empty chain")
	}

	root := chain.Nodes[0]
	if root.NodeKind != "load" {
		return "", nil, fmt.Errorf("chain root is %q, not a load — cannot generate source ref", root.NodeKind)
	}

	uri := root.Entry.SourceURI
	sn, tbl, _ := mapURIToSource(uri)
	q := fmt.Sprintf("SELECT * FROM {{ source('%s', '%s') }}", sn, tbl)

	var warnings []string
	for _, node := range chain.Nodes[1:] {
		if node.NodeKind == "copy" {
			continue
		}
		for _, op := range node.PipeOps {
			if op.Kind == "pivot" {
				warnings = append(warnings, "pivot uses DuckDB-specific PIVOT syntax — not portable to all dbt adapters")
			}
			var err error
			q, err = applyOp(q, op)
			if err != nil {
				return "", warnings, err
			}
		}
		if node.NodeKind == "sql" {
			// Mixed: zeaql in the middle of a pipe chain.
			inner, w, err := reconstructZeaqlSQL(node, nil)
			warnings = append(warnings, w...)
			if err != nil {
				return "", warnings, err
			}
			q = "SELECT * FROM (\n" + inner + "\n) _zea"
		}
	}

	w, _ := checkPortability(q)
	warnings = append(warnings, w...)

	header := "{{ config(materialized='table') }}\n\n"
	return header + q, warnings, nil
}

// --- Source mapping ---

type sourceEntry struct {
	SourceName  string
	TableName   string
	Description string
	URI         string
}

func mapURIToSource(uri string) (sourceName, tableName, description string) {
	switch {
	case strings.HasPrefix(uri, "zea://"):
		rest := strings.TrimPrefix(uri, "zea://")
		parts := strings.SplitN(rest, "/", 2)
		sourceName = "zea_" + sanitizeDbtName(parts[0])
		if len(parts) > 1 {
			tableName = sanitizeDbtName(strings.TrimSuffix(filepath.Base(parts[1]), filepath.Ext(parts[1])))
		} else {
			tableName = sanitizeDbtName(parts[0])
		}
		description = "Loaded from " + uri
	case strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://"):
		sourceName = "zea_http"
		base := filepath.Base(strings.TrimRight(uri, "/"))
		tableName = sanitizeDbtName(strings.TrimSuffix(base, filepath.Ext(base)))
		description = "Loaded from " + uri
	case uri == "":
		sourceName = "zea_local"
		tableName = "unknown"
		description = "Source URI not recorded — re-load to capture"
	default:
		sourceName = "zea_local"
		tableName = sanitizeDbtName(strings.TrimSuffix(filepath.Base(uri), filepath.Ext(uri)))
		description = "Loaded from " + uri
	}
	return
}

// --- Portability checker ---

var nonDeterministicPatterns = []string{
	"random(", "rand(", "now()", "current_timestamp", "uuid(", "gen_random_uuid(",
}

var duckdbSpecificPatterns = []string{
	"read_parquet(", "read_csv_auto(", "read_json_auto(",
	"list_aggregate(", "struct_pack(", "regexp_matches(",
	"duckdb_", "pragma_",
}

func checkPortability(sql string) (warnings []string, portable bool) {
	upper := strings.ToUpper(sql)
	for _, p := range nonDeterministicPatterns {
		if strings.Contains(upper, strings.ToUpper(p)) {
			warnings = append(warnings, "non-deterministic function: "+p)
		}
	}
	for _, p := range duckdbSpecificPatterns {
		if strings.Contains(upper, strings.ToUpper(p)) {
			warnings = append(warnings, "DuckDB-specific function: "+p)
		}
	}
	portable = len(warnings) == 0
	return
}

// --- SQL helpers ---

// sqlReferencesTable returns true if the SQL string references the given table name.
// Matches: FROM tablename, JOIN tablename, "tablename" (quoted).
var reWordBoundary = regexp.MustCompile(`(?i)\b(FROM|JOIN)\s+["` + "`" + `]?([a-zA-Z_][a-zA-Z0-9_]*)["` + "`" + `]?`)

func sqlReferencesTable(sql, table string) bool {
	matches := reWordBoundary.FindAllStringSubmatch(sql, -1)
	for _, m := range matches {
		if strings.EqualFold(m[2], table) {
			return true
		}
	}
	return false
}

// replaceTableRef substitutes all FROM/JOIN references to table with replacement.
func replaceTableRef(sql, table, replacement string) string {
	// Replace quoted: "table" or `table`
	sql = strings.ReplaceAll(sql, `"`+table+`"`, replacement)
	sql = strings.ReplaceAll(sql, "`"+table+"`", replacement)
	// Replace unquoted with word boundaries using regex.
	re := regexp.MustCompile(`(?i)((?:FROM|JOIN)\s+)` + regexp.QuoteMeta(table) + `\b`)
	return re.ReplaceAllStringFunc(sql, func(match string) string {
		loc := re.FindStringSubmatchIndex(match)
		if loc == nil {
			return match
		}
		prefix := match[loc[2]:loc[3]]
		return prefix + replacement
	})
}

// stripJinja removes {{ ... }} and {% ... %} Jinja templating for DuckDB parse checks,
// substituting source() and ref() calls with placeholder table names.
var reJinjaSource = regexp.MustCompile(`\{\{\s*source\('[^']+',\s*'([^']+)'\)\s*\}\}`)
var reJinjaRef = regexp.MustCompile(`\{\{\s*ref\('([^']+)'\)\s*\}\}`)
var reJinjaConfig = regexp.MustCompile(`\{\{[^}]+\}\}`)
var reJinjaTags = regexp.MustCompile(`\{%[^%]+%\}`)

func stripJinja(sql string) string {
	sql = reJinjaSource.ReplaceAllString(sql, `"$1"`)
	sql = reJinjaRef.ReplaceAllString(sql, `"$1"`)
	sql = reJinjaConfig.ReplaceAllString(sql, "")
	sql = reJinjaTags.ReplaceAllString(sql, "")
	return strings.TrimSpace(sql)
}

func isLoadNode(entry *TableEntry) bool {
	for _, op := range entry.Ops {
		if strings.HasPrefix(op, "load(") {
			return true
		}
	}
	return false
}

func buildTransformations(chain *LineageChain) []map[string]any {
	var result []map[string]any
	for _, node := range chain.Nodes {
		switch node.NodeKind {
		case "load":
			result = append(result, map[string]any{
				"type": "load", "uri": node.Entry.SourceURI,
			})
		case "sql":
			result = append(result, map[string]any{
				"type": "sql", "expr": node.Entry.SourceSQL,
			})
		case "pipe":
			for _, op := range node.PipeOps {
				result = append(result, map[string]any{
					"type": op.Kind, "expr": op.Args,
				})
			}
		}
	}
	return result
}

// --- File generators ---

func buildModelYAML(art *PromotedArtifact, entry *TableEntry, warnings []string) string {
	var b strings.Builder
	b.WriteString("version: 2\n\nmodels:\n")
	b.WriteString(fmt.Sprintf("  - name: %s\n", art.ExportName))
	b.WriteString(fmt.Sprintf("    description: \"Promoted from ZeaOS session (table: %s)\"\n", art.PromotedFrom))
	if len(warnings) > 0 {
		b.WriteString("    meta:\n      zea_warnings:\n")
		for _, w := range warnings {
			b.WriteString(fmt.Sprintf("        - \"%s\"\n", w))
		}
	}
	if entry != nil && entry.schema != nil {
		b.WriteString("    columns:\n")
		for i := 0; i < entry.schema.NumFields(); i++ {
			f := entry.schema.Field(i)
			b.WriteString(fmt.Sprintf("      - name: %s\n", f.Name))
			b.WriteString("        description: \"\"\n")
			b.WriteString("        tests:\n          - not_null\n")
		}
	}
	b.WriteString("\n")
	return b.String()
}

func buildSourcesYAML(sources []sourceEntry) string {
	// Deduplicate by sourceName+tableName.
	seen := map[string]bool{}
	var deduped []sourceEntry
	for _, s := range sources {
		key := s.SourceName + "." + s.TableName
		if !seen[key] {
			seen[key] = true
			deduped = append(deduped, s)
		}
	}

	// Group by source name.
	grouped := map[string][]sourceEntry{}
	var order []string
	for _, s := range deduped {
		if _, ok := grouped[s.SourceName]; !ok {
			order = append(order, s.SourceName)
		}
		grouped[s.SourceName] = append(grouped[s.SourceName], s)
	}

	var b strings.Builder
	b.WriteString("version: 2\n\nsources:\n")
	for _, sn := range order {
		b.WriteString(fmt.Sprintf("  - name: %s\n", sn))
		b.WriteString("    description: \"ZeaOS data sources\"\n")
		b.WriteString("    tables:\n")
		for _, e := range grouped[sn] {
			b.WriteString(fmt.Sprintf("      - name: %s\n", e.TableName))
			b.WriteString(fmt.Sprintf("        description: \"%s\"\n", e.Description))
		}
	}
	return b.String()
}

func dbtProjectYML() string {
	return `name: 'zea_export'
version: '1.0.0'
config-version: 2

profile: 'zea_local'

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  zea_export:
    materialized: table
`
}

func dbtProfilesYML() string {
	return `zea_local:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'local.duckdb'
      threads: 4
`
}

// --- Validators ---

func isValidDbtName(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if i == 0 && !unicode.IsLetter(r) && r != '_' {
			return false
		}
		if i > 0 && !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
		if unicode.IsUpper(r) {
			return false
		}
	}
	return true
}

func sanitizeDbtName(s string) string {
	var b strings.Builder
	for i, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
		} else if i > 0 {
			b.WriteRune('_')
		}
	}
	return b.String()
}

func execDbtHelp() error {
	fmt.Print(`
Promotion & Export Commands

  promote <table> [as <name>] [model|semantic]
                              mark table for export (target-agnostic)
  list                        list session tables
  list --type=promotions       list promoted artifacts
  validate [<name>] --target=dbt
                              check portability without writing files
  export [<name>] --target=dbt [-o DIR | --output=DIR]
                              write export bundle (default: ./zea-dbt-export)

Workflow:
  ZeaOS> trips = load https://...parquet
  ZeaOS> cc    = trips | where payment_type = 1
  ZeaOS> rev   = zeaql "SELECT PULocationID, SUM(fare_amount) AS revenue FROM cc GROUP BY 1"
  ZeaOS> promote rev as credit_card_revenue model
  ZeaOS> validate credit_card_revenue --target=dbt
  ZeaOS> export credit_card_revenue --target=dbt -o ./my-dbt-project

`)
	return nil
}
