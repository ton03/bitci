package bitci

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type Stage struct {
	PR  int    `json:"pr"`
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
func (controller *Controller) StagePR(ctx context.Context, number int, token string) (Stage, error) {
	if number < 1 {
		return Stage{}, fmt.Errorf("pull request number must be positive")
	}
	if token == "" {
		return Stage{}, fmt.Errorf("stage-pr requires BITCI_GITHUB_TOKEN")
	}
	if err := controller.noActiveJobs(); err != nil {
		return Stage{}, err
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
	if err := controller.cleanGeneratedNext(ctx); err != nil {
		return Stage{}, err
	}
	if err := controller.cleanCheckout(ctx); err != nil {
		return Stage{}, err
	}
	stageRef := fmt.Sprintf("refs/bitci/staged/%d", number)
	if _, err := controller.git(ctx, "fetch", "--no-tags", "origin", fmt.Sprintf("+refs/pull/%d/head:%s", number, stageRef)); err != nil {
		return Stage{}, fmt.Errorf("fetch trusted pull request: %w", err)
	}
	fetched, err := controller.git(ctx, "rev-parse", "--verify", stageRef+"^{commit}")
	if err != nil || strings.TrimSpace(fetched) != pull.Head.SHA {
		return Stage{}, fmt.Errorf("fetched pull request SHA does not match GitHub")
	}
	if err := controller.protectStateFromTarget(ctx, pull.Head.SHA); err != nil {
		return Stage{}, err
	}
	if _, err := controller.git(ctx, "checkout", "--detach", "--no-overwrite-ignore", pull.Head.SHA); err != nil {
		return Stage{}, fmt.Errorf("checkout trusted pull request: %w", err)
	}
	sha, err := controller.checkoutSHA()
	if err != nil || sha != pull.Head.SHA {
		return Stage{}, fmt.Errorf("checked out SHA does not match GitHub")
	}
	return Stage{PR: number, SHA: sha}, nil
}

func (controller *Controller) protectStateFromTarget(ctx context.Context, sha string) error {
	relative, ok := controller.checkoutStatePath()
	if !ok {
		return nil
	}
	output, err := controller.git(ctx, "ls-tree", "-r", "--name-only", sha, "--", ":(top,literal)"+filepath.ToSlash(relative))
	if err != nil {
		return err
	}
	if strings.TrimSpace(output) != "" {
		return fmt.Errorf("staged pull request must not contain tracked BitCI state files")
	}
	return nil
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
	nextRelative := filepath.Join(controller.configRelative, ".next")
	nextPath := filepath.Join(controller.gitDirectory(), nextRelative)
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

func (controller *Controller) cleanCheckout(ctx context.Context) error {
	args := []string{"status", "--porcelain", "--untracked-files=all"}
	if relative, ok := controller.checkoutStatePath(); ok {
		tracked, err := controller.git(ctx, "ls-files", "--", ":(top,literal)"+filepath.ToSlash(relative))
		if err != nil {
			return err
		}
		if strings.TrimSpace(tracked) != "" {
			return fmt.Errorf("state directory must not contain tracked files")
		}
		args = append(args, "--", ":(top)", ":(top,exclude,literal)"+filepath.ToSlash(relative))
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

func (controller *Controller) checkoutStatePath() (string, bool) {
	checkout, err := controller.git(context.Background(), "rev-parse", "--show-toplevel")
	if err != nil {
		return "", false
	}
	checkout, err = filepath.EvalSymlinks(strings.TrimSpace(checkout))
	if err != nil {
		return "", false
	}
	state, err := filepath.EvalSymlinks(controller.stateDir)
	if err != nil {
		return "", false
	}
	relative, err := filepath.Rel(checkout, state)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", false
	}
	return relative, true
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
