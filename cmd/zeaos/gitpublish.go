package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	gogit "github.com/go-git/go-git/v5"
	gitcfg "github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	gogithttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/google/go-github/v62/github"
	"golang.org/x/oauth2"
)

// ---------------------------------------------------------------------------
// ZeaOS config (~/.zeaos/config.json)
// ---------------------------------------------------------------------------

type zeaosConfig struct {
	GitHub struct {
		DefaultRepo   string `json:"default_repo,omitempty"`
		DefaultBranch string `json:"default_branch,omitempty"`
	} `json:"github,omitempty"`
	Push struct {
		DefaultTarget string `json:"default_target,omitempty"`
		DefaultSchema string `json:"default_schema,omitempty"`
	} `json:"push,omitempty"`
}

func configPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".zeaos", "config.json")
}

func loadConfig() (*zeaosConfig, error) {
	data, err := os.ReadFile(configPath())
	if err != nil {
		if os.IsNotExist(err) {
			return &zeaosConfig{}, nil
		}
		return nil, err
	}
	var cfg zeaosConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func saveConfig(cfg *zeaosConfig) error {
	path := configPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// ---------------------------------------------------------------------------
// Token storage — backed by the encrypted credential store
// ---------------------------------------------------------------------------

// githubCredPrefix is the credential name prefix for GitHub PATs.
const githubCredPrefix = "github."

// githubDefaultCred is the credential that stores the name of the default token.
const githubDefaultCred = "github.__default"

func githubCredName(tokenName string) string { return githubCredPrefix + tokenName }

func getDefaultTokenName(s *Session) string {
	if s == nil || s.Creds == nil || !s.Creds.IsUnlocked() {
		return ""
	}
	cred, err := s.Creds.Load(githubDefaultCred)
	if err != nil {
		return ""
	}
	name, _ := cred.Get("name")
	return name
}

func setDefaultTokenName(s *Session, name string) error {
	return s.Creds.Save(Credential{
		Name:   githubDefaultCred,
		Fields: []CredentialField{{Key: "name", Value: name}},
	})
}

func resolveToken(name string, s *Session) (string, error) {
	if s != nil && s.Creds != nil && s.Creds.IsUnlocked() {
		if name != "" {
			cred, err := s.Creds.Load(githubCredName(name))
			if err == nil {
				if tok, err := cred.Get("token"); err == nil && tok != "" {
					return tok, nil
				}
			}
			return "", fmt.Errorf("token %q not found — add it with: publish token add %s --pat <token>", name, name)
		}
		// Try the recorded default.
		if defName := getDefaultTokenName(s); defName != "" {
			if cred, err := s.Creds.Load(githubCredName(defName)); err == nil {
				if tok, _ := cred.Get("token"); tok != "" {
					return tok, nil
				}
			}
		}
		// If exactly one github token exists, use it.
		if allNames, _ := s.Creds.List(); len(allNames) > 0 {
			var githubTokens []string
			for _, n := range allNames {
				if strings.HasPrefix(n, githubCredPrefix) && n != githubDefaultCred {
					githubTokens = append(githubTokens, n)
				}
			}
			if len(githubTokens) == 1 {
				if cred, err := s.Creds.Load(githubTokens[0]); err == nil {
					if tok, _ := cred.Get("token"); tok != "" {
						return tok, nil
					}
				}
			}
		}
	}
	// Fall back to gh CLI token.
	if tok, err := ghCLIToken(); err == nil {
		return tok, nil
	}
	// Fall back to GITHUB_TOKEN env var.
	if tok := os.Getenv("GITHUB_TOKEN"); tok != "" {
		return tok, nil
	}
	return "", fmt.Errorf("no GitHub token found — add one with: publish token add <name> --pat <token>")
}

// ghCLIToken runs `gh auth token` and returns the token, or an error if gh
// is not installed or not authenticated.
func ghCLIToken() (string, error) {
	out, err := exec.Command("gh", "auth", "token").Output()
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(out))
	if tok == "" {
		return "", fmt.Errorf("gh auth token returned empty")
	}
	return tok, nil
}

// ---------------------------------------------------------------------------
// publish token subcommands
// ---------------------------------------------------------------------------

func execPublishToken(args []string, s *Session) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: publish token add <name> --pat <token> | list")
	}
	switch args[0] {
	case "add":
		return execPublishTokenAdd(args[1:], s)
	case "list":
		return execPublishTokenList(s)
	case "remove", "rm":
		return execPublishTokenRemove(args[1:], s)
	default:
		return fmt.Errorf("publish token: unknown subcommand %q (add | list | remove)", args[0])
	}
}

func execPublishTokenAdd(args []string, s *Session) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: publish token add <name> --pat <token>")
	}
	name := args[0]
	var pat string
	for i := 1; i < len(args); i++ {
		if (args[i] == "--pat" || args[i] == "-p") && i+1 < len(args) {
			pat = args[i+1]
			break
		}
		if strings.HasPrefix(args[i], "--pat=") {
			pat = strings.TrimPrefix(args[i], "--pat=")
			break
		}
	}
	if pat == "" {
		return fmt.Errorf("publish token add: --pat <token> required")
	}
	if s == nil || s.Creds == nil {
		return fmt.Errorf("publish token add: credential store not available")
	}
	if err := s.Creds.UnlockInteractive(); err != nil {
		return err
	}
	cred := Credential{
		Name:   githubCredName(name),
		Fields: []CredentialField{{Key: "token", Value: pat}},
	}
	if err := s.Creds.Save(cred); err != nil {
		return err
	}
	fmt.Printf("token %q saved (encrypted in credential store)\n", name)
	if getDefaultTokenName(s) == "" {
		_ = setDefaultTokenName(s, name)
		fmt.Printf("set as default token\n")
	}
	return nil
}

func execPublishTokenList(s *Session) error {
	if s == nil || s.Creds == nil {
		return fmt.Errorf("credential store not available")
	}
	if err := s.Creds.UnlockInteractive(); err != nil {
		return err
	}
	allNames, err := s.Creds.List()
	if err != nil {
		return err
	}
	defName := getDefaultTokenName(s)
	var tokenCredNames []string
	for _, n := range allNames {
		if strings.HasPrefix(n, githubCredPrefix) && n != githubDefaultCred {
			tokenCredNames = append(tokenCredNames, n)
		}
	}
	if len(tokenCredNames) == 0 {
		fmt.Println("No tokens stored. Add one with: publish token add <name> --pat <token>")
		return nil
	}
	fmt.Printf("%-20s  %s\n", "Name", "Token (masked)")
	fmt.Println(strings.Repeat("─", 50))
	for _, credName := range tokenCredNames {
		shortName := strings.TrimPrefix(credName, githubCredPrefix)
		cred, err := s.Creds.Load(credName)
		if err != nil {
			continue
		}
		tok, _ := cred.Get("token")
		def := ""
		if shortName == defName {
			def = "  (default)"
		}
		fmt.Printf("%-20s  %s%s\n", shortName, maskToken(tok), def)
	}
	return nil
}

func execPublishTokenRemove(args []string, s *Session) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: publish token remove <name>")
	}
	name := args[0]
	if s == nil || s.Creds == nil {
		return fmt.Errorf("credential store not available")
	}
	if err := s.Creds.UnlockInteractive(); err != nil {
		return err
	}
	if err := s.Creds.Delete(githubCredName(name)); err != nil {
		return fmt.Errorf("token %q not found", name)
	}
	// If we removed the default, pick a new one.
	if getDefaultTokenName(s) == name {
		allNames, _ := s.Creds.List()
		newDef := ""
		for _, n := range allNames {
			if strings.HasPrefix(n, githubCredPrefix) && n != githubDefaultCred {
				newDef = strings.TrimPrefix(n, githubCredPrefix)
				break
			}
		}
		_ = setDefaultTokenName(s, newDef)
	}
	fmt.Printf("token %q removed\n", name)
	return nil
}

func maskToken(tok string) string {
	if len(tok) <= 8 {
		return strings.Repeat("*", len(tok))
	}
	return tok[:4] + strings.Repeat("*", len(tok)-8) + tok[len(tok)-4:]
}

// ---------------------------------------------------------------------------
// publish args
// ---------------------------------------------------------------------------

type publishArgs struct {
	ArtifactName string
	Repo         string // OWNER/REPO
	Branch       string
	TokenName    string
	IsNew        bool
	IsPR         bool
	AutoPromote  bool // promote all unpromoted tables before publishing
	repoExplicit bool // true when --repo was given on the command line
}

func parsePublishArgs(args []string) (publishArgs, error) {
	pa := publishArgs{Branch: "main"}
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--repo" && i+1 < len(args):
			i++
			pa.Repo = args[i]
			pa.repoExplicit = true
		case strings.HasPrefix(args[i], "--repo="):
			pa.Repo = strings.TrimPrefix(args[i], "--repo=")
			pa.repoExplicit = true
		case args[i] == "--branch" && i+1 < len(args):
			i++
			pa.Branch = args[i]
		case strings.HasPrefix(args[i], "--branch="):
			pa.Branch = strings.TrimPrefix(args[i], "--branch=")
		case args[i] == "--token" && i+1 < len(args):
			i++
			pa.TokenName = args[i]
		case strings.HasPrefix(args[i], "--token="):
			pa.TokenName = strings.TrimPrefix(args[i], "--token=")
		case args[i] == "--new":
			pa.IsNew = true
		case args[i] == "--pr":
			pa.IsPR = true
		case args[i] == "--auto-promote":
			pa.AutoPromote = true
		case !strings.HasPrefix(args[i], "--") && pa.ArtifactName == "":
			pa.ArtifactName = args[i]
		}
	}
	// Fall back to configured default repo if --repo was not supplied.
	if pa.Repo == "" {
		cfg, err := loadConfig()
		if err == nil && cfg.GitHub.DefaultRepo != "" {
			pa.Repo = cfg.GitHub.DefaultRepo
			if cfg.GitHub.DefaultBranch != "" {
				pa.Branch = cfg.GitHub.DefaultBranch
			}
			fmt.Printf("using default repo: %s\n", pa.Repo)
		}
	}
	if pa.Repo == "" {
		return pa, fmt.Errorf("publish: --repo OWNER/REPO required (or set a default with: publish set-repo OWNER/REPO)")
	}
	return pa, nil
}

// ---------------------------------------------------------------------------
// execPublish — entry point
// ---------------------------------------------------------------------------

func execPublish(args []string, s *Session) error {
	if len(args) > 0 && args[0] == "token" {
		return execPublishToken(args[1:], s)
	}
	if len(args) > 0 && args[0] == "set-repo" {
		return execPublishSetRepo(args[1:])
	}
	if len(args) == 0 || args[0] == "help" {
		return execPublishHelp()
	}

	pa, err := parsePublishArgs(args)
	if err != nil {
		return err
	}

	// Persist an explicitly supplied --repo as the new default.
	if pa.repoExplicit {
		if cfg, err := loadConfig(); err == nil {
			cfg.GitHub.DefaultRepo = pa.Repo
			if pa.Branch != "main" {
				cfg.GitHub.DefaultBranch = pa.Branch
			}
			_ = saveConfig(cfg)
		}
	}

	// Auto-promote all eligible session tables if requested.
	if pa.AutoPromote {
		fmt.Println("Auto-promoting session tables...")
		added := autoPromoteAll(s)
		if len(added) == 0 && len(s.Promoted) == 0 {
			return fmt.Errorf("publish: no tables could be promoted — load some data first")
		}
	}

	// Resolve which artifacts to publish.
	var artifacts []*PromotedArtifact
	if pa.ArtifactName != "" {
		art, ok := s.Promoted[pa.ArtifactName]
		if !ok {
			return fmt.Errorf("publish: %q not found in promoted artifacts — use 'list --type=promotions'", pa.ArtifactName)
		}
		artifacts = []*PromotedArtifact{art}
	} else {
		if len(s.Promoted) == 0 {
			return fmt.Errorf("publish: nothing to publish — use 'promote <table>' first")
		}
		for _, a := range s.Promoted {
			artifacts = append(artifacts, a)
		}
	}

	// Warn about non-portable source URIs before doing any network work.
	warnSourcePortability(artifacts, s)

	token, err := resolveToken(pa.TokenName, s)
	if err != nil {
		return err
	}

	// Generate bundle into temp dir.
	tmpDir, err := os.MkdirTemp("", "zeaos-publish-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	fmt.Println("Generating export bundle...")
	count, err := generateDbtBundle(artifacts, tmpDir, s)
	if err != nil {
		return fmt.Errorf("publish: bundle generation failed: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("publish: no artifacts could be exported")
	}

	if pa.IsNew {
		return publishNewRepo(pa, tmpDir, artifacts, token)
	}
	return publishToRepo(pa, tmpDir, artifacts, token)
}

// ---------------------------------------------------------------------------
// Publish to existing repo
// ---------------------------------------------------------------------------

func publishToRepo(pa publishArgs, bundleDir string, artifacts []*PromotedArtifact, token string) error {
	repoURL := "https://github.com/" + pa.Repo + ".git"
	localPath := repoCachePath(pa.Repo)

	fmt.Printf("Cloning/updating %s...\n", pa.Repo)
	r, err := cloneOrPull(repoURL, localPath, token)
	if err != nil {
		return err
	}

	w, err := r.Worktree()
	if err != nil {
		return err
	}

	if pa.IsPR {
		branchName := "zeaos/" + strings.ToLower(strings.ReplaceAll(pa.ArtifactName, " ", "-"))
		if pa.ArtifactName == "" {
			branchName = fmt.Sprintf("zeaos/publish-%s", time.Now().Format("20060102-1504"))
		}
		if err := checkoutNewBranch(r, branchName); err != nil {
			return err
		}
		w, err = r.Worktree()
		if err != nil {
			return err
		}
		fmt.Printf("Created branch %s\n", branchName)

		if err := mergeBundle(bundleDir, localPath); err != nil {
			return err
		}

		commitMsg := buildCommitMsg(pa, artifacts)
		if _, err := stageAndCommit(w, commitMsg); err != nil {
			return err
		}

		refSpec := fmt.Sprintf("refs/heads/%s:refs/heads/%s", branchName, branchName)
		if err := pushWithFallback(r, pa, token, refSpec); err != nil {
			return err
		}

		// Create PR via GitHub API.
		artifactName := pa.ArtifactName
		if artifactName == "" {
			artifactName = "session artifacts"
		}
		prURL, err := createPullRequest(token, pa.Repo, branchName, pa.Branch,
			"ZeaOS: promote "+artifactName,
			buildPRBody(artifacts))
		if err != nil {
			return fmt.Errorf("publish: PR creation failed: %w", err)
		}
		fmt.Printf("✓ Created PR: %s\n", prURL)
		return nil
	}

	// Direct push to branch.
	_ = w // worktree already on target branch after clone/pull
	if err := mergeBundle(bundleDir, localPath); err != nil {
		return err
	}
	commitMsg := buildCommitMsg(pa, artifacts)
	hash, err := stageAndCommit(w, commitMsg)
	if err != nil {
		return err
	}

	refSpec := fmt.Sprintf("refs/heads/%s:refs/heads/%s", pa.Branch, pa.Branch)
	if err := pushWithFallback(r, pa, token, refSpec); err != nil {
		return err
	}

	fmt.Printf("✓ Published to %s (%s)\n", pa.Repo, hash.String()[:8])
	return nil
}

// ---------------------------------------------------------------------------
// Create new repo (--new)
// ---------------------------------------------------------------------------

func publishNewRepo(pa publishArgs, bundleDir string, artifacts []*PromotedArtifact, token string) error {
	ctx := context.Background()
	client := githubClient(ctx, token)

	// Split OWNER/REPO.
	parts := strings.SplitN(pa.Repo, "/", 2)
	var org, repoName string
	if len(parts) == 2 {
		org, repoName = parts[0], parts[1]
	} else {
		repoName = parts[0]
	}

	fmt.Printf("Creating GitHub repo %s/%s...\n", org, repoName)
	cloneURL, err := createGitHubRepo(ctx, client, org, repoName)
	if err != nil {
		return err
	}
	fmt.Printf("  created: https://github.com/%s\n", pa.Repo)

	localPath := repoCachePath(pa.Repo)
	// Remove any leftover directory from a previous failed attempt.
	_ = os.RemoveAll(localPath)
	if err := os.MkdirAll(localPath, 0o755); err != nil {
		return err
	}

	// Init local repo and add remote.
	r, err := gogit.PlainInit(localPath, false)
	if err != nil {
		return fmt.Errorf("git init: %w", err)
	}

	// Point HEAD at the target branch before the first commit so the branch
	// name is correct (PlainInit defaults to "master").
	if err := r.Storer.SetReference(
		plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName(pa.Branch)),
	); err != nil {
		return fmt.Errorf("git set HEAD: %w", err)
	}

	if _, err := r.CreateRemote(&gitcfg.RemoteConfig{
		Name: "origin",
		URLs: []string{cloneURL},
	}); err != nil {
		return fmt.Errorf("git remote add: %w", err)
	}

	// Write bundle files directly into the local repo dir.
	if err := copyDir(bundleDir, localPath); err != nil {
		return err
	}

	// Write --new only extras.
	extras := map[string]string{
		"README.md":  newRepoReadme(repoName),
		".gitignore": dbtGitignore(),
	}
	for name, content := range extras {
		if err := os.WriteFile(filepath.Join(localPath, name), []byte(content), 0o644); err != nil {
			return err
		}
	}

	w, err := r.Worktree()
	if err != nil {
		return err
	}

	commitMsg := buildCommitMsg(pa, artifacts)
	hash, err := stageAndCommit(w, commitMsg)
	if err != nil {
		return err
	}

	// Push to main branch (new repo).
	refSpec := fmt.Sprintf("refs/heads/%s:refs/heads/%s", pa.Branch, pa.Branch)
	auth := &gogithttp.BasicAuth{Username: "x-token", Password: token}
	pushErr := r.Push(&gogit.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []gitcfg.RefSpec{gitcfg.RefSpec(refSpec)},
		Auth:       auth,
	})
	if pushErr != nil && pushErr != gogit.NoErrAlreadyUpToDate {
		return fmt.Errorf("git push: %w", pushErr)
	}

	fmt.Printf("✓ Published to https://github.com/%s (%s)\n", pa.Repo, hash.String()[:8])
	return nil
}

// ---------------------------------------------------------------------------
// Git helpers
// ---------------------------------------------------------------------------

func repoCachePath(repo string) string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".zeaos", "github", "repos", repo)
}

func cloneOrPull(repoURL, localPath, token string) (*gogit.Repository, error) {
	auth := &gogithttp.BasicAuth{Username: "x-token", Password: token}

	if _, err := os.Stat(filepath.Join(localPath, ".git")); err == nil {
		r, err := gogit.PlainOpen(localPath)
		if err != nil {
			return nil, fmt.Errorf("open cached repo: %w", err)
		}
		w, err := r.Worktree()
		if err != nil {
			return nil, err
		}
		pullErr := w.Pull(&gogit.PullOptions{RemoteName: "origin", Auth: auth})
		if pullErr != nil && !errors.Is(pullErr, gogit.NoErrAlreadyUpToDate) {
			return nil, fmt.Errorf("git pull: %w", pullErr)
		}
		return r, nil
	}

	if err := os.MkdirAll(localPath, 0o755); err != nil {
		return nil, err
	}
	r, err := gogit.PlainClone(localPath, false, &gogit.CloneOptions{
		URL:  repoURL,
		Auth: auth,
	})
	if err != nil {
		return nil, fmt.Errorf("git clone: %w", err)
	}
	return r, nil
}

func checkoutNewBranch(r *gogit.Repository, branchName string) error {
	w, err := r.Worktree()
	if err != nil {
		return err
	}
	ref := plumbing.NewBranchReferenceName(branchName)
	return w.Checkout(&gogit.CheckoutOptions{
		Branch: ref,
		Create: true,
	})
}

func stageAndCommit(w *gogit.Worktree, message string) (plumbing.Hash, error) {
	if err := w.AddWithOptions(&gogit.AddOptions{All: true}); err != nil {
		return plumbing.ZeroHash, fmt.Errorf("git add: %w", err)
	}
	author := os.Getenv("USER")
	if author == "" {
		author = "zeaos"
	}
	hash, err := w.Commit(message, &gogit.CommitOptions{
		Author: &object.Signature{
			Name:  "ZeaOS",
			Email: author + "@zeaos.local",
			When:  time.Now(),
		},
	})
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("git commit: %w", err)
	}
	return hash, nil
}

func pushWithFallback(r *gogit.Repository, pa publishArgs, token, refSpec string) error {
	auth := &gogithttp.BasicAuth{Username: "x-token", Password: token}
	err := r.Push(&gogit.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []gitcfg.RefSpec{gitcfg.RefSpec(refSpec)},
		Auth:       auth,
	})
	if err == nil {
		return nil
	}

	// Detect auth/permission failures and offer PR as fallback.
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "authorization") ||
		strings.Contains(errStr, "forbidden") ||
		strings.Contains(errStr, "authentication") ||
		strings.Contains(errStr, "denied") {
		fmt.Printf("✗ No push access to %s\n", pa.Repo)
		fmt.Print("  Create a PR instead? [y/N] ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		if strings.ToLower(strings.TrimSpace(scanner.Text())) == "y" {
			// Caller should have set up a branch; just tell the user to re-run with --pr.
			fmt.Println("  Re-run with --pr to create a pull request.")
		}
		return fmt.Errorf("no push access to %s — try --pr to open a pull request", pa.Repo)
	}
	return fmt.Errorf("git push: %w", err)
}

// ---------------------------------------------------------------------------
// Post-push repo update
// ---------------------------------------------------------------------------

// updateRepoStagingMacro rewrites macros/stage_zea_sources.sql in the default
// published repo to add a MotherDuck guard, so that dbt run --target prod
// skips HTTPS view creation and uses the already-materialized MotherDuck tables.
// sources is the list of HTTPS sources that were just pushed.
// A no-op if no default repo is configured or the macro file doesn't exist.
func updateRepoStagingMacro(pushTarget string, sources []sourceEntry, s *Session) error {
	cfg, err := loadConfig()
	if err != nil || cfg.GitHub.DefaultRepo == "" {
		return nil // no default repo configured — nothing to update
	}

	token, err := resolveToken("", s)
	if err != nil || token == "" {
		return nil // no token available — skip silently
	}

	repo := cfg.GitHub.DefaultRepo
	branch := cfg.GitHub.DefaultBranch
	if branch == "" {
		branch = "main"
	}

	localPath := repoCachePath(repo)
	repoURL := "https://github.com/" + repo + ".git"

	r, err := cloneOrPull(repoURL, localPath, token)
	if err != nil {
		return fmt.Errorf("update macro: %w", err)
	}

	macroPath := filepath.Join(localPath, "macros", "stage_zea_sources.sql")
	if _, err := os.Stat(macroPath); err != nil {
		return nil // macro doesn't exist in this repo — nothing to update
	}

	updated := buildMotherDuckAwareStagingMacro(sources)
	if err := os.WriteFile(macroPath, []byte(updated), 0o644); err != nil {
		return fmt.Errorf("update macro: write: %w", err)
	}

	w, err := r.Worktree()
	if err != nil {
		return fmt.Errorf("update macro: worktree: %w", err)
	}

	hash, err := stageAndCommit(w,
		fmt.Sprintf("ZeaOS: update staging macro for MotherDuck target (%s)", pushTarget))
	if err != nil {
		return fmt.Errorf("update macro: commit: %w", err)
	}

	refSpec := fmt.Sprintf("refs/heads/%s:refs/heads/%s", branch, branch)
	if err := pushWithFallback(r, publishArgs{Repo: repo, Branch: branch}, token, refSpec); err != nil {
		return fmt.Errorf("update macro: push: %w", err)
	}

	fmt.Printf("  updated macros/stage_zea_sources.sql in %s (%s)\n", repo, hash.String()[:8])
	return nil
}

// ---------------------------------------------------------------------------
// File merge logic
// ---------------------------------------------------------------------------

// mergeBundle copies bundle files into the target repo directory using
// the file-type merge rules:
//   - models/*.sql, models/*.yml  → always overwrite (with conflict prompt)
//   - sources/zea_sources.yml     → overwrite if ZeaOS-generated, warn otherwise
//   - profiles.yml, dbt_project.yml → only create if missing
//   - zea_export.json             → always overwrite
func mergeBundle(bundleDir, repoDir string) error {
	return filepath.WalkDir(bundleDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(bundleDir, path)
		dst := filepath.Join(repoDir, rel)

		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}

		switch {
		case strings.HasSuffix(rel, ".sql") && strings.HasPrefix(rel, "models/"):
			return mergeSQLFile(path, dst, rel)

		case strings.HasSuffix(rel, ".yml") && strings.HasPrefix(rel, "models/"):
			return copyFile(path, dst)

		case rel == "sources/zea_sources.yml":
			return mergeSourcesYAML(path, dst)

		case rel == "profiles.yml" || rel == "dbt_project.yml":
			if _, err := os.Stat(dst); err == nil {
				fmt.Printf("  skipped %s (already exists)\n", rel)
				return nil
			}
			return copyFile(path, dst)

		default: // zea_export.json, seeds/, etc.
			return copyFile(path, dst)
		}
	})
}

func mergeSQLFile(src, dst, rel string) error {
	existing, err := os.ReadFile(dst)
	if err == nil {
		// File exists — check if it looks like a ZeaOS model.
		trimmed := strings.TrimSpace(string(existing))
		if !strings.HasPrefix(trimmed, "{{ config(") {
			// Foreign content — prompt before overwriting.
			fmt.Printf("  ⚠  %s exists and may not be ZeaOS-generated.\n", rel)
			fmt.Print("     Overwrite? [y/N] ")
			scanner := bufio.NewScanner(os.Stdin)
			scanner.Scan()
			if strings.ToLower(strings.TrimSpace(scanner.Text())) != "y" {
				fmt.Printf("  skipped %s\n", rel)
				return nil
			}
		}
	}
	return copyFile(src, dst)
}

func mergeSourcesYAML(src, dst string) error {
	existing, err := os.ReadFile(dst)
	if err != nil {
		// Not present — create it.
		return copyFile(src, dst)
	}
	if strings.Contains(string(existing), "# generated by ZeaOS") {
		// ZeaOS-generated — safe to overwrite.
		return copyFile(src, dst)
	}
	// Foreign sources file — do not overwrite, warn.
	fmt.Printf("  ⚠  sources/zea_sources.yml has non-ZeaOS content — not overwriting.\n")
	fmt.Printf("     Manually merge: %s\n", src)
	return nil
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o644)
}

// copyDir copies all files from src into dst (non-recursive for top-level only).
// Used for --new mode where we copy the entire bundle into the init'd repo.
func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return copyFile(path, target)
	})
}

// ---------------------------------------------------------------------------
// GitHub API helpers
// ---------------------------------------------------------------------------

func githubClient(ctx context.Context, token string) *github.Client {
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
	tc := oauth2.NewClient(ctx, ts)
	return github.NewClient(tc)
}

func createGitHubRepo(ctx context.Context, client *github.Client, org, repoName string) (string, error) {
	newRepo := &github.Repository{
		Name:        github.String(repoName),
		Description: github.String("dbt project exported from ZeaOS"),
		AutoInit:    github.Bool(false),
		Private:     github.Bool(false),
	}
	// org="" creates under the authenticated user; pass org name for org repos.
	created, _, err := client.Repositories.Create(ctx, org, newRepo)
	if err != nil {
		return "", fmt.Errorf("create GitHub repo: %w", err)
	}
	return created.GetCloneURL(), nil
}

func createPullRequest(token, repo, head, base, title, body string) (string, error) {
	ctx := context.Background()
	client := githubClient(ctx, token)
	parts := strings.SplitN(repo, "/", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid repo %q: expected OWNER/REPO", repo)
	}
	owner, repoName := parts[0], parts[1]
	pr, _, err := client.PullRequests.Create(ctx, owner, repoName, &github.NewPullRequest{
		Title: github.String(title),
		Head:  github.String(head),
		Base:  github.String(base),
		Body:  github.String(body),
	})
	if err != nil {
		return "", err
	}
	return pr.GetHTMLURL(), nil
}

// ---------------------------------------------------------------------------
// Message builders
// ---------------------------------------------------------------------------

func buildCommitMsg(pa publishArgs, artifacts []*PromotedArtifact) string {
	if pa.ArtifactName != "" {
		return fmt.Sprintf("ZeaOS v%s: promote %s", version, pa.ArtifactName)
	}
	names := make([]string, 0, len(artifacts))
	for _, a := range artifacts {
		names = append(names, a.ExportName)
	}
	return fmt.Sprintf("ZeaOS v%s: promote %s [%s]",
		version, strings.Join(names, ", "), time.Now().Format("2006-01-02"))
}

func buildPRBody(artifacts []*PromotedArtifact) string {
	var b strings.Builder
	b.WriteString("## ZeaOS Export\n\nPromoted from ZeaOS session.\n\n")
	b.WriteString("### Artifacts\n\n")
	for _, a := range artifacts {
		b.WriteString(fmt.Sprintf("- **%s** (`%s`) — from session table `%s`\n",
			a.ExportName, a.Kind, a.PromotedFrom))
	}
	b.WriteString("\n_Generated by [`zeaos publish --pr`](https://github.com/open-tempest-labs/zeaos)_\n")
	return b.String()
}

func newRepoReadme(repoName string) string {
	return "# " + repoName + "\n\n" +
		"dbt project exported from [ZeaOS](https://github.com/open-tempest-labs/zeaos).\n\n" +
		"## Quickstart — local dbt + DuckDB\n\n" +
		"Requires Python 3.12 (dbt does not yet support 3.13/3.14). On macOS:\n\n" +
		"```bash\n" +
		"brew install pipx python@3.12\n" +
		"pipx install dbt-duckdb --python $(brew --prefix python@3.12)/bin/python3.12\n" +
		"pipx ensurepath   # restart your shell after this\n" +
		"```\n\n" +
		"Or with a virtual environment:\n\n" +
		"```bash\n" +
		"python3.12 -m venv .venv && source .venv/bin/activate\n" +
		"pip install dbt-duckdb\n" +
		"```\n\n" +
		"Then:\n\n" +
		"```bash\n" +
		"dbt run\n" +
		"dbt test\n" +
		"```\n\n" +
		"DuckDB reads source Parquet files directly over HTTPS — no warehouse, no data loading, " +
		"no extra configuration required. Results are materialised into `local.duckdb`.\n\n" +
		"> **Portability note:** This works out of the box when source data comes from public HTTPS " +
		"or S3 URLs. If sources were loaded from local files the models will only run on the " +
		"machine where the session was created. See `zea_export.json` for source URI details.\n\n" +
		"## Import into dbt Cloud\n\n" +
		"1. In [dbt Cloud](https://cloud.getdbt.com), create a new project and connect this repository.\n" +
		"2. Configure your warehouse connection in the dbt Cloud UI.\n" +
		"3. For non-DuckDB warehouses, source data must be loaded into your warehouse first — " +
		"the `{{ source() }}` references in `sources/zea_sources.yml` and model SQL will need " +
		"to point at warehouse tables rather than HTTPS URLs.\n" +
		"4. For DuckDB in dbt Cloud, models run as-is against public HTTPS sources.\n\n" +
		"## Lineage\n\n" +
		"See `zea_export.json` for the full lineage of each model — every load, filter, and " +
		"SQL transformation recorded by ZeaOS at export time, including source URIs, row counts, " +
		"and portability status.\n"
}

func dbtGitignore() string {
	return "target/\ndbt_packages/\nlogs/\n*.duckdb\n"
}

// ---------------------------------------------------------------------------
// Help
// ---------------------------------------------------------------------------

// isPortableURI returns true if the URI is reachable by anyone with the repo —
// i.e. a public HTTPS URL or an S3/GCS/Azure cloud storage URL.
func isPortableURI(uri string) bool {
	return strings.HasPrefix(uri, "https://") ||
		strings.HasPrefix(uri, "http://") ||
		strings.HasPrefix(uri, "s3://") ||
		strings.HasPrefix(uri, "gs://") ||
		strings.HasPrefix(uri, "abfs://") ||
		strings.HasPrefix(uri, "az://")
}

// warnSourcePortability checks all source URIs across the artifacts being
// published and prints a warning for any that are local paths. Local sources
// mean the published repo's dbt models will only run on this machine.
func warnSourcePortability(artifacts []*PromotedArtifact, s *Session) {
	var localSources []string
	seen := map[string]bool{}
	for _, art := range artifacts {
		chain, err := walkLineage(s, art.PromotedFrom)
		if err != nil {
			continue
		}
		for _, uri := range chain.SourceURIs {
			if !isPortableURI(uri) && !seen[uri] {
				seen[uri] = true
				localSources = append(localSources, uri)
			}
		}
	}
	if len(localSources) == 0 {
		return
	}
	fmt.Println("⚠  Non-portable sources detected:")
	for _, uri := range localSources {
		fmt.Printf("     %s\n", uri)
	}
	fmt.Println("   The published dbt repo will only run on this machine.")
	fmt.Println("   To make it portable: save source tables to a ZeaDrive S3 backend")
	fmt.Println("   and re-publish so the source URIs point to S3.")
	fmt.Println("   Example: save trips zea://s3-data/taxi/trips.parquet")
	fmt.Println()
}

func execPublishSetRepo(args []string) error {
	if len(args) == 0 {
		// Show current default.
		cfg, err := loadConfig()
		if err != nil {
			return err
		}
		if cfg.GitHub.DefaultRepo == "" {
			fmt.Println("no default repo set — use: publish set-repo OWNER/REPO")
		} else {
			fmt.Printf("default repo: %s\n", cfg.GitHub.DefaultRepo)
		}
		return nil
	}
	repo := args[0]
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	cfg.GitHub.DefaultRepo = repo
	if err := saveConfig(cfg); err != nil {
		return err
	}
	fmt.Printf("default repo set to %s (saved to %s)\n", repo, configPath())
	return nil
}

func execPublishHelp() error {
	fmt.Print(`
publish — push promoted artifacts to GitHub as a dbt project

USAGE
  publish <name> --repo OWNER/REPO [--branch BRANCH] [--new] [--pr]
  publish --repo OWNER/REPO              publish all promoted artifacts

OPTIONS
  --repo OWNER/REPO    target GitHub repository (required)
  --branch BRANCH      target branch (default: main)
  --new                create the repository if it does not exist
  --pr                 open a pull request instead of pushing directly
  --token NAME         use named token from token store
  --auto-promote       promote all eligible session tables before publishing

DEFAULT REPO
  publish set-repo OWNER/REPO   set default repo (saved to ~/.zeaos/config.json)
  publish set-repo              show current default

  Once set, --repo can be omitted and the default is used automatically.
  Passing --repo explicitly updates the default for next time.

TOKEN MANAGEMENT
  publish token add <name> --pat <token>   store a GitHub PAT
  publish token list                        list stored tokens
  publish token remove <name>               remove a token

  Tokens are stored encrypted in the credential store (AES-256-GCM).
  Use a fine-grained PAT with Contents: read/write scope.

EXAMPLES
  publish set-repo lmccay/nyc-taxi-dbt
  publish zone_revenue --repo team/my-dbt-project --new
  publish zone_revenue --pr
  publish --auto-promote

`)
	return nil
}
