package bitci

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type Stage struct {
	PR  int    `json:"pr,omitempty"`
	Ref string `json:"ref,omitempty"`
	SHA string `json:"sha"`
}

type pullRequest struct {
	Head struct {
		SHA  string `json:"sha"`
		Repo struct {
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"head"`
	Base struct {
		Repo struct {
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"base"`
}

// StagePR checks a same-repository GitHub pull request and detaches the
// dedicated checkout at its verified head SHA.
func (controller *Controller) StagePR(ctx context.Context, number int, token string) (stage Stage, returnErr error) {
	if number < 1 {
		return Stage{}, fmt.Errorf("pull request number must be positive")
	}
	if token == "" {
		return Stage{}, fmt.Errorf("stage-pr requires BITCI_GITHUB_TOKEN")
	}
	repository, err := controller.githubRepository()
	if err != nil {
		return Stage{}, err
	}
	pull, err := controller.githubPull(ctx, repository, number, token)
	if err != nil {
		return Stage{}, err
	}
	if pull.Head.Repo.FullName != repository || pull.Base.Repo.FullName != repository || !isCheckoutSHA(pull.Head.SHA) {
		return Stage{}, fmt.Errorf("pull request must have a verified same-repository head")
	}
	sourceRef := fmt.Sprintf("refs/pull/%d/head", number)
	sha, err := controller.stageTrustedRef(ctx, number, sourceRef, fmt.Sprintf("refs/bitci/staged/%d", number), pull.Head.SHA, "pull request", repository)
	if err != nil {
		return Stage{}, err
	}
	return Stage{PR: number, SHA: sha}, nil
}

// StageMain fetches and verifies only origin/main before detaching the
// dedicated checkout at that exact commit.
func (controller *Controller) StageMain(ctx context.Context) (Stage, error) {
	sha, err := controller.stageTrustedRef(ctx, 0, "refs/heads/main", "refs/bitci/staged/main", "", "main ref", "")
	if err != nil {
		return Stage{}, err
	}
	return Stage{Ref: "main", SHA: sha}, nil
}

// StageBranch pins a same-repository origin branch for CI without requiring a
// pull request. The remote SHA is read first and checked again after fetch so
// a branch update during staging cannot change the submitted candidate.
func (controller *Controller) StageBranch(ctx context.Context, branch string) (Stage, error) {
	if branch == "" || strings.TrimSpace(branch) != branch || strings.HasPrefix(branch, "refs/") {
		return Stage{}, fmt.Errorf("branch must be a short remote branch name")
	}
	sourceRef := "refs/heads/" + branch
	if _, err := controller.git(ctx, "check-ref-format", sourceRef); err != nil {
		return Stage{}, fmt.Errorf("invalid remote branch name")
	}
	repository, err := controller.githubRepository()
	if err != nil {
		return Stage{}, err
	}
	output, err := controller.git(ctx, "ls-remote", "--exit-code", "--refs", "origin", sourceRef)
	if err != nil {
		return Stage{}, fmt.Errorf("read remote branch: %w", err)
	}
	fields := strings.Fields(strings.TrimSpace(output))
	if len(fields) != 2 || fields[1] != sourceRef || !isCheckoutSHA(fields[0]) {
		return Stage{}, fmt.Errorf("remote branch did not resolve to one commit")
	}
	sha := strings.ToLower(fields[0])
	if _, err := controller.stageTrustedRef(ctx, 0, sourceRef, "refs/bitci/staged/branch", sha, "branch ref", repository); err != nil {
		return Stage{}, err
	}
	return Stage{Ref: branch, SHA: sha}, nil
}

func (controller *Controller) stageTrustedRef(ctx context.Context, pullRequest int, sourceRef, stageRef, expectedSHA, sourceName, expectedRepository string) (sha string, returnErr error) {
	release, err := controller.acquireStageLock(ctx)
	if err != nil {
		return "", fmt.Errorf("lock checkout for staging: %w", err)
	}
	defer release()
	if err := controller.noActiveJobs(); err != nil {
		return "", err
	}
	repository, err := controller.githubRepository()
	if err != nil {
		return "", err
	}
	if expectedRepository != "" && repository != expectedRepository {
		return "", fmt.Errorf("origin repository changed before staging")
	}
	originalSHA, err := controller.checkoutSHA()
	if err != nil {
		return "", fmt.Errorf("read current checkout SHA: %w", err)
	}
	originalConfig := controller.configSnapshot()
	targetCheckedOut := false
	staged := false
	defer func() {
		if !targetCheckedOut || staged {
			return
		}
		if _, err := controller.git(ctx, "checkout", "--detach", originalSHA); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("restore checkout after failed staging: %w", err))
		}
		controller.configMu.Lock()
		controller.config = originalConfig
		controller.configMu.Unlock()
	}()
	if err := controller.clearStagedCheckout(); err != nil {
		return "", err
	}
	if err := controller.cleanGeneratedNext(ctx); err != nil {
		return "", err
	}
	if err := controller.cleanCheckout(ctx); err != nil {
		return "", err
	}
	if _, err := controller.git(ctx, "fetch", "--no-tags", "origin", fmt.Sprintf("+%s:%s", sourceRef, stageRef)); err != nil {
		return "", fmt.Errorf("fetch trusted %s: %w", sourceName, err)
	}
	fetched, err := controller.git(ctx, "rev-parse", "--verify", stageRef+"^{commit}")
	fetched = strings.ToLower(strings.TrimSpace(fetched))
	if err != nil || !isCheckoutSHA(fetched) || expectedSHA != "" && !strings.EqualFold(fetched, expectedSHA) {
		return "", fmt.Errorf("fetched %s SHA does not match its trusted source", sourceName)
	}
	if err := controller.protectStateFromTarget(ctx, fetched); err != nil {
		return "", err
	}
	if _, err := controller.git(ctx, "checkout", "--detach", fetched); err != nil {
		return "", fmt.Errorf("checkout trusted %s: %w", sourceName, err)
	}
	targetCheckedOut = true
	sha, err = controller.checkoutSHA()
	if err != nil || !strings.EqualFold(sha, fetched) {
		return "", fmt.Errorf("checked out SHA does not match trusted %s", sourceName)
	}
	stagedConfig, err := LoadConfig(controller.configPath)
	if err != nil {
		return "", fmt.Errorf("load staged BitCI configuration: %w", err)
	}
	controller.configMu.Lock()
	controller.config = stagedConfig
	controller.configMu.Unlock()
	if err := controller.recordStagedCheckout(sourceRef, pullRequest, sha); err != nil {
		return "", err
	}
	staged = true
	return sha, nil
}

func (controller *Controller) stagedCheckoutSHA() (string, error) {
	var sha string
	err := controller.db.QueryRow("SELECT sha FROM staged_checkouts WHERE id = 1").Scan(&sha)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read staged checkout: %w", err)
	}
	if !isCheckoutSHA(sha) {
		return "", fmt.Errorf("staged checkout has invalid SHA")
	}
	return strings.ToLower(sha), nil
}

func (controller *Controller) clearStagedCheckout() error {
	if _, err := controller.db.Exec("DELETE FROM staged_checkouts WHERE id = 1"); err != nil {
		return fmt.Errorf("clear staged checkout: %w", err)
	}
	return nil
}

func (controller *Controller) recordStagedCheckout(sourceRef string, number int, sha string) error {
	if !isCheckoutSHA(sha) || sourceRef == "" {
		return fmt.Errorf("cannot record invalid staged checkout SHA")
	}
	if _, err := controller.db.Exec("INSERT INTO staged_checkouts(id, pull_request, source_ref, sha, staged_at) VALUES (1, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET pull_request = excluded.pull_request, source_ref = excluded.source_ref, sha = excluded.sha, staged_at = excluded.staged_at", number, sourceRef, strings.ToLower(sha), timestamp()); err != nil {
		return fmt.Errorf("record staged checkout: %w", err)
	}
	return nil
}

func (controller *Controller) protectStateFromTarget(ctx context.Context, sha string) error {
	relative, ok, err := controller.checkoutStatePath()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	conflict, err := controller.gitTreeContainsStateConflict(ctx, sha, relative, caseInsensitiveFilesystem(controller.gitDirectory()))
	if err != nil {
		return err
	}
	if conflict {
		return fmt.Errorf("staged checkout must not contain tracked BitCI state files")
	}
	return nil
}

func (controller *Controller) gitTreeContainsStateConflict(ctx context.Context, sha, relative string, foldCase bool) (bool, error) {
	tree := sha + "^{tree}"
	parts := strings.Split(filepath.ToSlash(relative), "/")
	for index, part := range parts {
		output, err := controller.git(ctx, "ls-tree", "-z", tree)
		if err != nil {
			return false, err
		}
		matched := false
		for _, record := range strings.Split(output, "\x00") {
			metadata, name, ok := strings.Cut(record, "\t")
			fields := strings.Fields(metadata)
			matches := name == part || foldCase && strings.EqualFold(name, part)
			if !ok || len(fields) != 3 || !matches {
				continue
			}
			if index == len(parts)-1 {
				return true, nil
			}
			if fields[1] != "tree" {
				return true, nil
			}
			tree = fields[2]
			matched = true
			break
		}
		if !matched {
			return false, nil
		}
	}
	return false, nil
}

func (controller *Controller) noActiveJobs() error {
	var active int
	if err := controller.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE state IN ('queued', 'running')").Scan(&active); err != nil {
		return err
	}
	if active != 0 {
		return fmt.Errorf("cannot stage while BitCI jobs are queued or running")
	}
	return nil
}

func (controller *Controller) cleanGeneratedNext(ctx context.Context) error {
	if _, _, err := controller.checkoutStatePath(); err != nil {
		return err
	}
	nextRelative := filepath.Join(controller.configRelative, ".next")
	nextPath := filepath.Join(controller.gitDirectory(), nextRelative)
	if pathsOverlap(nextPath, controller.stateDir) {
		return fmt.Errorf("state directory must not overlap generated output .next")
	}
	info, err := os.Lstat(nextPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refuse generated cleanup for non-directory .next")
	}
	if _, err := controller.git(ctx, "clean", "-fdX", "--", ":(top,literal)"+filepath.ToSlash(nextRelative)); err != nil {
		return fmt.Errorf("clean ignored .next: %w", err)
	}
	return nil
}

func pathsOverlap(first, second string) bool {
	first = resolvedPathForComparison(first)
	second = resolvedPathForComparison(second)
	return pathWithin(first, second) || pathWithin(second, first)
}

// resolvedPathForComparison resolves the deepest existing ancestor. It keeps
// overlap checks safe for paths that do not yet exist below a symlink.
func resolvedPathForComparison(path string) string {
	path = filepath.Clean(path)
	var missing []string
	for {
		if resolved, err := filepath.EvalSymlinks(path); err == nil {
			for index := len(missing) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, missing[index])
			}
			return filepath.Clean(resolved)
		}
		parent := filepath.Dir(path)
		if parent == path {
			return path
		}
		missing = append(missing, filepath.Base(path))
		path = parent
	}
}

func (controller *Controller) cleanCheckout(ctx context.Context) error {
	args := []string{"status", "--porcelain", "--untracked-files=all"}
	relative, ok, err := controller.checkoutStatePath()
	if err != nil {
		return err
	}
	if ok {
		foldCase := caseInsensitiveFilesystem(controller.gitDirectory())
		tracked, err := controller.git(ctx, "ls-files", "--", statePathspec(relative, foldCase))
		if err != nil {
			return err
		}
		if tracked != "" {
			return fmt.Errorf("state directory must not contain tracked files")
		}
		exclude := ":(top,exclude,literal)" + filepath.ToSlash(relative)
		if foldCase {
			exclude = ":(top,exclude,icase,literal)" + filepath.ToSlash(relative)
		}
		args = append(args, "--", ":(top)", exclude)
	}
	output, err := controller.git(ctx, args...)
	if err != nil {
		return err
	}
	if strings.TrimSpace(output) != "" {
		return fmt.Errorf("dedicated checkout must be clean before staging")
	}
	return nil
}

func (controller *Controller) checkoutStatePath() (string, bool, error) {
	checkout := controller.gitDirectory()
	if controller.checkoutRoot != "" {
		checkout = controller.checkoutRoot
	}
	checkout = filepath.Clean(checkout)
	state := filepath.Clean(controller.stateDir)
	resolvedState := resolvedPathForComparison(state)
	if relative, ok := relativeWithin(checkout, resolvedState); ok && relative != "." {
		usesSymlink, err := pathUsesSymlinkInto(checkout, state)
		if err != nil {
			return "", false, err
		}
		if usesSymlink {
			return "", false, fmt.Errorf("state directory must not use a symlink inside the checkout")
		}
	}
	if lexicalRoot := controller.lexicalCheckoutRoot(checkout); lexicalRoot != "" {
		if lexicalRelative, err := filepath.Rel(lexicalRoot, state); err == nil && lexicalRelative != "." && lexicalRelative != ".." && !strings.HasPrefix(lexicalRelative, ".."+string(filepath.Separator)) {
			usesSymlink, err := pathUsesSymlink(lexicalRoot, lexicalRelative)
			if err != nil {
				return "", false, err
			}
			if usesSymlink {
				return "", false, fmt.Errorf("state directory must not use a symlink inside the checkout")
			}
		}
	}
	if relative, ok := relativeLexicallyWithin(checkout, state); ok && relative != "." {
		usesSymlink, err := pathUsesSymlink(checkout, relative)
		if err != nil {
			return "", false, err
		}
		if usesSymlink {
			return "", false, fmt.Errorf("state directory must not use a symlink inside the checkout")
		}
	}
	relative, ok := relativeWithin(checkout, resolvedState)
	if !ok || relative == "." {
		return "", false, nil
	}
	usesSymlink, err := pathUsesSymlink(checkout, relative)
	if err != nil {
		return "", false, err
	}
	if usesSymlink {
		return "", false, fmt.Errorf("state directory must not use a symlink inside the checkout")
	}
	return relative, true, nil
}

func statePathspec(relative string, foldCase bool) string {
	magic := ":(top,literal)"
	if foldCase {
		magic = ":(top,icase,literal)"
	}
	return magic + filepath.ToSlash(relative)
}

func caseInsensitiveFilesystem(path string) bool {
	path = resolvedPathForComparison(path)
	for current := path; ; current = filepath.Dir(current) {
		base := filepath.Base(current)
		for index, character := range base {
			var replacement rune
			switch {
			case 'a' <= character && character <= 'z':
				replacement = character - ('a' - 'A')
			case 'A' <= character && character <= 'Z':
				replacement = character + ('a' - 'A')
			default:
				continue
			}
			alias := filepath.Join(filepath.Dir(current), base[:index]+string(replacement)+base[index+1:])
			originalInfo, originalErr := os.Stat(current)
			aliasInfo, aliasErr := os.Stat(alias)
			return originalErr == nil && aliasErr == nil && os.SameFile(originalInfo, aliasInfo)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return false
		}
	}
}

func relativeLexicallyWithin(root, path string) (string, bool) {
	path = filepath.Clean(path)
	suffix := filepath.Base(path)
	for parent := filepath.Dir(path); ; parent = filepath.Dir(parent) {
		if resolved, err := filepath.EvalSymlinks(parent); err == nil {
			if relative, ok := relativeWithin(root, filepath.Join(resolved, suffix)); ok {
				return relative, true
			}
		}
		if parent == filepath.Dir(parent) {
			return "", false
		}
		suffix = filepath.Join(filepath.Base(parent), suffix)
	}
}

func relativeWithin(root, path string) (string, bool) {
	relative, err := filepath.Rel(root, path)
	if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return relative, true
	}
	root = filepath.Clean(root)
	path = filepath.Clean(path)
	prefix := root + string(filepath.Separator)
	if !pathWithin(root, path) || len(path) <= len(prefix) || !strings.EqualFold(path[:len(prefix)], prefix) {
		return "", false
	}
	return path[len(prefix):], true
}

func (controller *Controller) lexicalCheckoutRoot(checkout string) string {
	path := filepath.Dir(controller.configPath)
	for {
		if samePath(resolvedPathForComparison(path), checkout) {
			return path
		}
		parent := filepath.Dir(path)
		if parent == path {
			return ""
		}
		path = parent
	}
}

func samePath(first, second string) bool {
	return pathWithin(first, second) && pathWithin(second, first)
}

func pathUsesSymlink(root, relative string) (bool, error) {
	path := root
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		path = filepath.Join(path, part)
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return true, nil
		}
	}
	return false, nil
}

func pathUsesSymlinkInto(root, path string) (bool, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}
	volumeRoot := filepath.VolumeName(absolute) + string(filepath.Separator)
	current := volumeRoot
	parts := strings.Split(strings.TrimPrefix(absolute, volumeRoot), string(filepath.Separator))
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			resolved, err := filepath.EvalSymlinks(current)
			if err != nil {
				return false, err
			}
			if _, ok := relativeWithin(root, resolvedPathForComparison(resolved)); ok {
				return true, nil
			}
		}
	}
	return false, nil
}

func (controller *Controller) git(ctx context.Context, args ...string) (string, error) {
	return gitAt(ctx, controller.gitDirectory(), args...)
}

func gitAt(ctx context.Context, directory string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "git", append([]string{"-C", directory}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w", args[0], err)
	}
	return string(output), nil
}

func (controller *Controller) githubRepository() (string, error) {
	if controller.githubRepo != "" {
		return controller.githubRepo, nil
	}
	remote, err := controller.git(context.Background(), "remote", "get-url", "origin")
	if err != nil {
		return "", fmt.Errorf("get origin remote: %w", err)
	}
	value := strings.TrimSuffix(strings.TrimSpace(remote), ".git")
	for _, prefix := range []string{"https://github.com/", "git@github.com:", "ssh://git@github.com/"} {
		if repository, ok := strings.CutPrefix(value, prefix); ok && strings.Count(repository, "/") == 1 {
			return repository, nil
		}
	}
	return "", fmt.Errorf("origin must be a github.com repository")
}

func (controller *Controller) githubPull(ctx context.Context, repository string, number int, token string) (pullRequest, error) {
	api := controller.githubAPI
	if api == "" {
		api = "https://api.github.com"
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/repos/%s/pulls/%d", strings.TrimSuffix(api, "/"), repository, number), nil)
	if err != nil {
		return pullRequest{}, err
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-GitHub-Api-Version", "2026-03-10")
	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return pullRequest{}, fmt.Errorf("read pull request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return pullRequest{}, fmt.Errorf("read pull request: GitHub returned %s", response.Status)
	}
	var pull pullRequest
	if err := json.NewDecoder(response.Body).Decode(&pull); err != nil {
		return pullRequest{}, err
	}
	return pull, nil
}
