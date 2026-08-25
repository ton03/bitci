package bitci

import (
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

type Service struct {
	Label       string
	ConfigPath  string
	StateDir    string
	MaxWorkers  int
	HTTPAddress string
	BinaryPath  string
	PathEnv     string
	PlistPath   string
	LockPath    string
	Domain      string
}

func NewService(configPath, stateDir string, maxWorkers int) (Service, error) {
	return NewServiceWithHTTP(configPath, stateDir, maxWorkers, "")
}

func NewServiceWithHTTP(configPath, stateDir string, maxWorkers int, httpAddress string) (Service, error) {
	service, err := serviceIdentity(configPath, stateDir)
	if err != nil {
		return Service{}, err
	}
	config, err := LoadConfig(service.ConfigPath)
	if err != nil {
		return Service{}, err
	}
	if maxWorkers < 1 {
		return Service{}, fmt.Errorf("max-workers must be positive")
	}
	if httpAddress != "" {
		if err := validateDashboardAddress(httpAddress); err != nil {
			return Service{}, err
		}
	}
	service.MaxWorkers = maxWorkers
	service.HTTPAddress = httpAddress
	service.PathEnv, err = servicePath(config, filepath.Dir(service.ConfigPath))
	if err != nil {
		return Service{}, err
	}
	return service, nil
}

func NewServiceForStop(configPath, stateDir string) (Service, error) {
	return serviceIdentity(configPath, stateDir)
}

// NewServiceForStart identifies a service without reading bitci.json. Start can
// therefore remain idempotent when the service already runs.
func NewServiceForStart(configPath, stateDir string, maxWorkers int, httpAddress string) (Service, error) {
	service, err := serviceIdentity(configPath, stateDir)
	if err != nil {
		return Service{}, err
	}
	if maxWorkers < 1 {
		return Service{}, fmt.Errorf("max-workers must be positive")
	}
	if httpAddress != "" {
		if err := validateDashboardAddress(httpAddress); err != nil {
			return Service{}, err
		}
	}
	service.MaxWorkers = maxWorkers
	service.HTTPAddress = httpAddress
	return service, nil
}

func serviceIdentity(configPath, stateDir string) (Service, error) {
	if runtime.GOOS != "darwin" {
		return Service{}, fmt.Errorf("managed service currently requires macOS")
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
	home, err := os.UserHomeDir()
	if err != nil {
		return Service{}, err
	}
	binary, err := os.Executable()
	if err != nil {
		return Service{}, err
	}
	digest := sha256.Sum256([]byte(absoluteConfig))
	label := fmt.Sprintf("com.bitci.%x", digest[:6])
	service := Service{
		Label:      label,
		ConfigPath: absoluteConfig,
		StateDir:   absoluteState,
		BinaryPath: binary,
		PathEnv:    "",
		PlistPath:  filepath.Join(home, "Library", "LaunchAgents", label+".plist"),
		LockPath:   filepath.Join(home, ".local", "state", "bitci", "services", label+".lock"),
		Domain:     fmt.Sprintf("gui/%d", os.Getuid()),
	}
	return service, nil
}

func servicePath(config Config, checkout string) (string, error) {
	if path := os.Getenv("BITCI_PATH"); path != "" {
		return path, nil
	}
	gitRoot := ""
	verifiedCommit := false
	if output, err := exec.Command("git", "-C", checkout, "rev-parse", "--show-toplevel").Output(); err == nil {
		gitRoot = filepath.Clean(strings.TrimSpace(string(output)))
		if _, err := exec.Command("git", "-C", gitRoot, "rev-parse", "--verify", "HEAD^{commit}").Output(); err == nil {
			verifiedCommit = true
		}
	}
	directories := make([]string, 0, len(config.Tasks)+6)
	seen := map[string]bool{}
	add := func(directory string) {
		if directory != "" && !seen[directory] {
			seen[directory] = true
			directories = append(directories, directory)
		}
	}
	addOutsideCheckout := func(directory string) {
		if gitRoot == "" || !pathWithin(gitRoot, directory) {
			add(directory)
		}
	}
	commands := make([]struct {
		name          string
		requireExists bool
		environment   map[string]string
	}, 0, len(config.Tasks)+1)
	if len(config.Prepare) > 0 {
		commands = append(commands, struct {
			name          string
			requireExists bool
			environment   map[string]string
		}{name: config.Prepare[0], requireExists: true})
	}
	for _, name := range config.TaskNames() {
		commands = append(commands, struct {
			name          string
			requireExists bool
			environment   map[string]string
		}{name: config.Tasks[name].Run[0], requireExists: len(config.Prepare) == 0 || !verifiedCommit, environment: config.Tasks[name].Env})
	}
	for _, configured := range commands {
		command := configured.name
		if strings.ContainsRune(command, filepath.Separator) {
			absolute := filepath.IsAbs(command)
			if !absolute {
				command = filepath.Join(checkout, command)
			}
			info, err := os.Stat(command)
			if err != nil {
				if !configured.requireExists && os.IsNotExist(err) {
					continue
				}
				return "", fmt.Errorf("stat configured command %q: %w", command, err)
			}
			if info.IsDir() {
				return "", fmt.Errorf("configured command %q is a directory", command)
			}
			if absolute {
				addOutsideCheckout(filepath.Dir(command))
			}
			continue
		}
		resolved, err := lookPath(command, taskEnvironment(configured.environment, checkout), checkout)
		if err != nil {
			if !configured.requireExists && errors.Is(err, exec.ErrNotFound) {
				continue
			}
			return "", fmt.Errorf("resolve configured command %q: %w", command, err)
		}
		if _, configuredPath := configured.environment["PATH"]; !configuredPath {
			addOutsideCheckout(filepath.Dir(resolved))
		}
	}
	for _, directory := range []string{"/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"} {
		add(directory)
	}
	return strings.Join(directories, ":"), nil
}

func (service Service) Install() error {
	stateDir, err := ValidateStateDir(service.ConfigPath, service.StateDir)
	if err != nil {
		return err
	}
	service.StateDir = stateDir
	return service.withLifecycleLock(func() error { return service.install(true) })
}

func (service Service) install(checkActiveJobs bool) error {
	if checkActiveJobs {
		if err := service.ensureNoActiveJobs(); err != nil {
			return err
		}
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

func (service Service) installAbsent() error {
	if err := os.MkdirAll(filepath.Dir(service.PlistPath), 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(service.StateDir, 0o700); err != nil {
		return err
	}
	plist, err := os.OpenFile(service.PlistPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	if _, err := plist.WriteString(service.plist()); err != nil {
		_ = plist.Close()
		_ = os.Remove(service.PlistPath)
		return err
	}
	if err := plist.Close(); err != nil {
		_ = os.Remove(service.PlistPath)
		return err
	}
	if output, err := exec.Command("launchctl", "bootstrap", service.Domain, service.PlistPath).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl bootstrap: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if output, err := exec.Command("launchctl", "kickstart", "-k", service.Domain+"/"+service.Label).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl kickstart: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

// Start creates the managed service when absent. It does not replace a running
// service, so repeated project-root starts do not interrupt queued work.
func (service Service) Start() (bool, error) {
	started := false
	err := service.withLifecycleLock(func() error {
		if _, err := service.Status(); err == nil {
			return nil
		}
		if _, err := os.Lstat(service.PlistPath); err == nil {
			return fmt.Errorf("existing service needs inspection: run bitci service status")
		} else if !os.IsNotExist(err) {
			return err
		}
		stateDir, err := ValidateStateDir(service.ConfigPath, service.StateDir)
		if err != nil {
			return err
		}
		configured, err := NewServiceWithHTTP(service.ConfigPath, stateDir, service.MaxWorkers, service.HTTPAddress)
		if err != nil {
			return err
		}
		if err := configured.installAbsent(); err != nil {
			return err
		}
		started = true
		return nil
	})
	return started, err
}

func (service Service) ensureNoActiveJobs() error {
	return service.ensureNoActiveJobsFor("upgrade")
}

func (service Service) ensureNoActiveJobsFor(operation string) error {
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
		return fmt.Errorf("cannot %s service while BitCI jobs are queued or running", operation)
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
	return service.withLifecycleLock(service.uninstall)
}

func (service Service) uninstall() error {
	if output, err := exec.Command("launchctl", "bootout", service.Domain+"/"+service.Label).CombinedOutput(); err != nil {
		message := strings.ToLower(string(output) + err.Error())
		if !strings.Contains(message, "no such process") && !strings.Contains(message, "could not find service") {
			return fmt.Errorf("launchctl bootout: %w: %s", err, strings.TrimSpace(string(output)))
		}
	}
	if err := os.Remove(service.PlistPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (service Service) Stop() error {
	return service.withLifecycleLock(func() error {
		service, err := service.withInstalledStateDir()
		if err != nil {
			return err
		}
		if err := service.ensureNoActiveJobsFor("stop"); err != nil {
			return err
		}
		return service.uninstall()
	})
}

func (service Service) withLifecycleLock(run func() error) error {
	if err := os.MkdirAll(filepath.Dir(service.LockPath), 0o700); err != nil {
		return err
	}
	lock, err := os.OpenFile(service.LockPath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return run()
}

func (service Service) withInstalledStateDir() (Service, error) {
	plist, err := os.OpenFile(service.PlistPath, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if os.IsNotExist(err) {
		return service, nil
	}
	if err != nil {
		return Service{}, err
	}
	defer plist.Close()
	stateDir, err := plistStateDir(plist)
	if err != nil {
		return Service{}, err
	}
	stateDir, err = ValidateStateDir(service.ConfigPath, stateDir)
	if err != nil {
		return Service{}, err
	}
	service.StateDir = stateDir
	return service, nil
}

func plistStateDir(reader io.Reader) (string, error) {
	decoder := xml.NewDecoder(reader)
	arguments := false
	wantArguments := false
	var values []string
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		switch token := token.(type) {
		case xml.StartElement:
			switch token.Name.Local {
			case "key":
				var key string
				if err := decoder.DecodeElement(&key, &token); err != nil {
					return "", err
				}
				wantArguments = key == "ProgramArguments"
			case "array":
				arguments = wantArguments
				wantArguments = false
			case "string":
				if arguments {
					var value string
					if err := decoder.DecodeElement(&value, &token); err != nil {
						return "", err
					}
					values = append(values, value)
				}
			}
		case xml.EndElement:
			if token.Name.Local == "array" {
				arguments = false
			}
		}
	}
	for index, value := range values {
		if value == "--state-dir" && index+1 < len(values) {
			return values[index+1], nil
		}
	}
	return "", fmt.Errorf("service plist does not contain --state-dir")
}

func (service Service) plist() string {
	argument := func(value string) string { return "<string>" + html.EscapeString(value) + "</string>" }
	arguments := argument(service.BinaryPath) + argument("serve") + argument("--config") + argument(service.ConfigPath) + argument("--state-dir") + argument(service.StateDir) + argument("--max-workers") + argument(fmt.Sprint(service.MaxWorkers))
	if service.HTTPAddress != "" {
		arguments += argument("--http") + argument(service.HTTPAddress)
	}
	return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
		"<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n" +
		"<plist version=\"1.0\"><dict>\n" +
		"<key>Label</key>" + argument(service.Label) + "\n" +
		"<key>ProgramArguments</key><array>" + arguments + "</array>\n" +
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
