package bitci

import (
	"crypto/sha256"
	"fmt"
	"html"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

type Service struct {
	Label      string
	ConfigPath string
	StateDir   string
	MaxWorkers int
	BinaryPath string
	PathEnv    string
	PlistPath  string
	Domain     string
}

func NewService(configPath, stateDir string, maxWorkers int) (Service, error) {
	if runtime.GOOS != "darwin" {
		return Service{}, fmt.Errorf("managed service currently requires macOS")
	}
	config, err := LoadConfig(configPath)
	if err != nil {
		return Service{}, err
	}
	absoluteConfig, err := filepath.Abs(configPath)
	if err != nil {
		return Service{}, err
	}
	if stateDir == "" {
		stateDir = DefaultStateDir(absoluteConfig, "")
	}
	absoluteState, err := filepath.Abs(stateDir)
	if err != nil {
		return Service{}, err
	}
	binary, err := os.Executable()
	if err != nil {
		return Service{}, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return Service{}, err
	}
	if maxWorkers < 1 {
		return Service{}, fmt.Errorf("max-workers must be positive")
	}
	digest := sha256.Sum256([]byte(absoluteConfig))
	label := fmt.Sprintf("com.bitci.%x", digest[:6])
	service := Service{
		Label:      label,
		ConfigPath: absoluteConfig,
		StateDir:   absoluteState,
		MaxWorkers: maxWorkers,
		BinaryPath: binary,
		PathEnv:    "",
		PlistPath:  filepath.Join(home, "Library", "LaunchAgents", label+".plist"),
		Domain:     fmt.Sprintf("gui/%d", os.Getuid()),
	}
	service.PathEnv, err = servicePath(config, filepath.Dir(absoluteConfig))
	if err != nil {
		return Service{}, err
	}
	return service, nil
}

func servicePath(config Config, checkout string) (string, error) {
	if path := os.Getenv("BITCI_PATH"); path != "" {
		return path, nil
	}
	directories := make([]string, 0, len(config.Tasks)+5)
	seen := map[string]bool{}
	add := func(directory string) {
		if directory != "" && !seen[directory] {
			seen[directory] = true
			directories = append(directories, directory)
		}
	}
	for _, name := range config.TaskNames() {
		command := config.Tasks[name].Run[0]
		if strings.ContainsRune(command, filepath.Separator) {
			if !filepath.IsAbs(command) {
				command = filepath.Join(checkout, command)
			}
			info, err := os.Stat(command)
			if err != nil || info.IsDir() {
				return "", fmt.Errorf("resolve configured task command %q", config.Tasks[name].Run[0])
			}
			add(filepath.Dir(command))
			continue
		}
		resolved, err := exec.LookPath(command)
		if err != nil {
			return "", fmt.Errorf("resolve configured task command %q: %w", command, err)
		}
		add(filepath.Dir(resolved))
	}
	for _, directory := range []string{"/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"} {
		add(directory)
	}
	return strings.Join(directories, ":"), nil
}

func (service Service) Install() error {
	if err := service.ensureNoActiveJobs(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(service.PlistPath), 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(service.StateDir, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(service.PlistPath, []byte(service.plist()), 0o600); err != nil {
		return err
	}
	_ = exec.Command("launchctl", "bootout", service.Domain+"/"+service.Label).Run()
	if output, err := exec.Command("launchctl", "bootstrap", service.Domain, service.PlistPath).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl bootstrap: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if output, err := exec.Command("launchctl", "kickstart", "-k", service.Domain+"/"+service.Label).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl kickstart: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (service Service) ensureNoActiveJobs() error {
	controller, err := OpenState(service.ConfigPath, service.StateDir)
	if err != nil {
		return err
	}
	defer controller.Close()
	var active int
	if err := controller.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE state IN ('queued', 'running')").Scan(&active); err != nil {
		return err
	}
	if active != 0 {
		return fmt.Errorf("cannot upgrade service while BitCI jobs are queued or running")
	}
	return nil
}

func (service Service) Status() (string, error) {
	output, err := exec.Command("launchctl", "print", service.Domain+"/"+service.Label).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("launchctl status: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

func (service Service) Uninstall() error {
	_ = exec.Command("launchctl", "bootout", service.Domain+"/"+service.Label).Run()
	if err := os.Remove(service.PlistPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (service Service) plist() string {
	argument := func(value string) string { return "<string>" + html.EscapeString(value) + "</string>" }
	return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
		"<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n" +
		"<plist version=\"1.0\"><dict>\n" +
		"<key>Label</key>" + argument(service.Label) + "\n" +
		"<key>ProgramArguments</key><array>" + argument(service.BinaryPath) + argument("serve") + argument("--config") + argument(service.ConfigPath) + argument("--state-dir") + argument(service.StateDir) + argument("--max-workers") + argument(fmt.Sprint(service.MaxWorkers)) + "</array>\n" +
		"<key>WorkingDirectory</key>" + argument(filepath.Dir(service.ConfigPath)) + "\n" +
		"<key>EnvironmentVariables</key><dict><key>PATH</key>" + argument(service.PathEnv) + "</dict>\n" +
		"<key>KeepAlive</key><true/>\n" +
		"<key>RunAtLoad</key><true/>\n" +
		"<key>ProcessType</key><string>Background</string>\n" +
		"<key>StandardOutPath</key>" + argument(filepath.Join(service.StateDir, "bitci.stdout.log")) + "\n" +
		"<key>StandardErrorPath</key>" + argument(filepath.Join(service.StateDir, "bitci.stderr.log")) + "\n" +
		"<key>ThrottleInterval</key><integer>10</integer>\n" +
		"</dict></plist>\n"
}
