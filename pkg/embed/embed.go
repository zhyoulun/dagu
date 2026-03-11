package embed

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dagu-org/dagu/internal/cmn/config"
	"github.com/dagu-org/dagu/internal/cmn/eval"
	"github.com/dagu-org/dagu/internal/cmn/fileutil"
	"github.com/dagu-org/dagu/internal/cmn/logger"
	"github.com/dagu-org/dagu/internal/core"
	"github.com/dagu-org/dagu/internal/core/exec"
	"github.com/dagu-org/dagu/internal/core/spec"
	"github.com/dagu-org/dagu/internal/persis/filedag"
	"github.com/dagu-org/dagu/internal/persis/filedagrun"
	"github.com/dagu-org/dagu/internal/persis/fileproc"
	"github.com/dagu-org/dagu/internal/persis/fileserviceregistry"
	"github.com/dagu-org/dagu/internal/runtime"
	"github.com/dagu-org/dagu/internal/runtime/agent"
	dagext "github.com/dagu-org/dagu/internal/runtime/executor"
	"github.com/goccy/go-yaml"
	"github.com/google/uuid"
	"github.com/spf13/viper"
)

// StartOptions controls an in-process DAG execution.
type StartOptions struct {
	ConfigFile string
	Config     any
	DAGPath    string
	Params     []string
	Quiet      bool
	RunID      string
	LogWriter  io.Writer
}

// Config is the public config overlay for embedded execution.
// Any exported struct with matching YAML field names can also be passed via StartOptions.Config.
type Config struct {
	SkipExamples       *bool   `yaml:"SkipExamples"`
	LatestStatusToday  *bool   `yaml:"LatestStatusToday"`
	Debug              *bool   `yaml:"Debug"`
	LogFormat          *string `yaml:"LogFormat"`
	DagsDir            *string `yaml:"DagsDir"`
	DataDir            *string `yaml:"DataDir"`
	LogDir             *string `yaml:"LogDir"`
	AdminLogsDir       *string `yaml:"AdminLogsDir"`
	SuspendFlagsDir    *string `yaml:"SuspendFlagsDir"`
	BaseConfig         *string `yaml:"BaseConfig"`
	DAGRunsDir         *string `yaml:"DAGRunsDir"`
	QueueDir           *string `yaml:"QueueDir"`
	ProcDir            *string `yaml:"ProcDir"`
	ServiceRegistryDir *string `yaml:"ServiceRegistryDir"`
}

// Step is the executor-facing step view used by embedded custom executors.
// Handler is optional and may be left empty when the runner dispatches by step name.
type Step struct {
	Name    string
	Handler string
	Input   map[string]any
}

// Runner is a simplified executor contract for embedded callers.
// The returned string is written to step stdout and can be captured by Dagu output variables.
type Runner interface {
	Run(ctx context.Context, step Step) (string, error)
}

// RunnerFunc adapts a function into a Runner.
type RunnerFunc func(ctx context.Context, step Step) (string, error)

// Run implements Runner.
func (f RunnerFunc) Run(ctx context.Context, step Step) (string, error) {
	return f(ctx, step)
}

// ExecutorCapabilities declares which Dagu step fields the custom executor supports.
type ExecutorCapabilities struct {
	Command          bool
	MultipleCommands bool
	Script           bool
	Shell            bool
	Container        bool
	SubDAG           bool
	WorkerSelector   bool
	LLM              bool
}

type embeddedRuntime struct {
	cfg             *config.Config
	dagStore        exec.DAGStore
	dagRunStore     exec.DAGRunStore
	procStore       exec.ProcStore
	serviceRegistry exec.ServiceRegistry
}

type embeddedSubDAGLockKey struct{}

var embeddedSubDAGMu sync.Mutex

// Start executes a DAG in the current process without invoking the CLI package.
func Start(ctx context.Context, opts StartOptions) error {
	dagPath := strings.TrimSpace(opts.DAGPath)
	if dagPath == "" {
		return fmt.Errorf("dag path is required")
	}

	cfg, dagCtx, err := loadEmbedConfig(ctx, opts)
	if err != nil {
		return err
	}

	runID, err := resolveRunID(opts.RunID)
	if err != nil {
		return err
	}

	dag, err := spec.Load(
		dagCtx,
		dagPath,
		spec.WithBaseConfig(cfg.Paths.BaseConfig),
		spec.WithParams(opts.Params),
		spec.WithDAGsDir(cfg.Paths.DAGsDir),
	)
	if err != nil {
		return err
	}

	dagStore := filedag.New(
		cfg.Paths.DAGsDir,
		filedag.WithSearchPaths([]string{filepath.Dir(dagPath)}),
		filedag.WithSkipExamples(true),
	)
	dagRunStore := filedagrun.New(
		cfg.Paths.DAGRunsDir,
		filedagrun.WithLatestStatusToday(cfg.Server.LatestStatusToday),
		filedagrun.WithLocation(cfg.Core.Location),
	)
	procStore := fileproc.New(cfg.Paths.ProcDir)
	serviceRegistry := fileserviceregistry.New(cfg.Paths.ServiceRegistryDir)
	root := exec.NewDAGRunRef(dag.Name, runID)
	runner := &embeddedRuntime{
		cfg:             cfg,
		dagStore:        dagStore,
		dagRunStore:     dagRunStore,
		procStore:       procStore,
		serviceRegistry: serviceRegistry,
	}
	_, err = runner.RunSubDAG(dagCtx, exec.SubDAGRunRequest{
		DAG:         dag,
		DAGRunID:    runID,
		RootDAGRun:  root,
		Params:      opts.Params,
		TriggerType: core.TriggerTypeManual,
	})
	if err != nil {
		return err
	}
	return nil
}

// RegisterExecutor registers a custom executor type for embedded execution.
func RegisterExecutor(executorType string, runner Runner, caps ExecutorCapabilities) {
	dagext.RegisterExecutor(
		strings.TrimSpace(executorType),
		func(_ context.Context, step core.Step) (dagext.Executor, error) {
			embeddedStep, err := newEmbeddedStep(step)
			if err != nil {
				return nil, err
			}
			return &registeredExecutor{
				runner: runner,
				step:   embeddedStep,
			}, nil
		},
		validateEmbeddedStep,
		core.ExecutorCapabilities{
			Command:          caps.Command,
			MultipleCommands: caps.MultipleCommands,
			Script:           caps.Script,
			Shell:            caps.Shell,
			Container:        caps.Container,
			SubDAG:           caps.SubDAG,
			WorkerSelector:   caps.WorkerSelector,
			LLM:              caps.LLM,
		},
	)
}

type registeredExecutor struct {
	runner Runner
	step   Step
	stdout io.Writer
	cancel context.CancelFunc
}

func (e *registeredExecutor) SetStdout(out io.Writer) {
	e.stdout = out
}

func (e *registeredExecutor) SetStderr(_ io.Writer) {}

func (e *registeredExecutor) Kill(_ os.Signal) error {
	if e.cancel != nil {
		e.cancel()
	}
	return nil
}

func (e *registeredExecutor) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	defer cancel()

	output, err := e.runner.Run(runCtx, e.step)
	if output != "" && e.stdout != nil {
		if _, writeErr := io.WriteString(e.stdout, output); writeErr != nil {
			return fmt.Errorf("write executor output: %w", writeErr)
		}
	}
	return err
}

func validateEmbeddedStep(step core.Step) error {
	_, err := newEmbeddedStep(step)
	return err
}

func newEmbeddedStep(step core.Step) (Step, error) {
	config := cloneMap(step.ExecutorConfig.Config)

	handler := ""
	if rawHandler, ok := config["handler"]; ok {
		typedHandler, ok := rawHandler.(string)
		if !ok || strings.TrimSpace(typedHandler) == "" {
			return Step{}, fmt.Errorf("executor config.handler must be a non-empty string")
		}
		handler = strings.TrimSpace(typedHandler)
		delete(config, "handler")
	}

	input := map[string]any{}
	if rawInput, ok := config["input"]; ok {
		typedInput, ok := rawInput.(map[string]any)
		if !ok {
			return Step{}, fmt.Errorf("executor config.input must be an object")
		}
		input = cloneMap(typedInput)
	} else {
		delete(config, "handler")
		input = config
	}

	return Step{
		Name:    step.Name,
		Handler: handler,
		Input:   input,
	}, nil
}

func cloneMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return map[string]any{}
	}

	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func (r *embeddedRuntime) RunSubDAG(ctx context.Context, req exec.SubDAGRunRequest) (*exec.RunStatus, error) {
	if req.DAG == nil {
		return nil, fmt.Errorf("dag is required")
	}

	runID, err := resolveRunID(req.DAGRunID)
	if err != nil {
		return nil, err
	}

	dag, err := r.loadDAG(ctx, req)
	if err != nil {
		return nil, err
	}

	root := req.RootDAGRun
	if root.Zero() {
		root = exec.NewDAGRunRef(dag.Name, runID)
	}

	triggerType := req.TriggerType
	if triggerType == core.TriggerTypeUnknown {
		if req.ParentDAGRun.Zero() {
			triggerType = core.TriggerTypeManual
		} else {
			triggerType = core.TriggerTypeSubDAG
		}
	}

	ctx, unlock := lockEmbeddedSubDAG(ctx)
	defer unlock()

	previousDir, restoreDir := captureWorkingDir()
	if restoreDir != nil {
		defer restoreDir(previousDir)
	}

	logFile, logDir, err := openLogFile(ctx, r.cfg, dag, runID)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = logFile.Close()
	}()

	if err := r.procStore.Lock(ctx, dag.ProcGroup()); err != nil {
		return nil, err
	}
	proc, err := r.procStore.Acquire(ctx, dag.ProcGroup(), exec.NewDAGRunRef(dag.Name, runID))
	r.procStore.Unlock(ctx, dag.ProcGroup())
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = proc.Stop(ctx)
	}()

	manager := runtime.NewManager(r.dagRunStore, r.procStore, r.cfg)
	agentInstance := agent.New(
		runID,
		dag,
		logDir,
		logFile.Name(),
		manager,
		r.dagStore,
		agent.Options{
			DAGRunStore:     r.dagRunStore,
			ServiceRegistry: r.serviceRegistry,
			ParentDAGRun:    req.ParentDAGRun,
			RootDAGRun:      root,
			PeerConfig:      r.cfg.Core.Peer,
			TriggerType:     triggerType,
			DefaultExecMode: r.cfg.DefaultExecMode,
			WorkerID:        "local",
			SubDAGRunner:    r,
		},
	)
	if err := agentInstance.Run(ctx); err != nil {
		return nil, err
	}

	return r.readRunStatus(ctx, dag, runID, root, req.ParentDAGRun)
}

func (r *embeddedRuntime) loadDAG(ctx context.Context, req exec.SubDAGRunRequest) (*core.DAG, error) {
	loadOpts := []spec.LoadOption{
		spec.WithBaseConfig(r.cfg.Paths.BaseConfig),
		spec.WithDAGsDir(r.cfg.Paths.DAGsDir),
		spec.WithParams(req.Params),
	}

	if !req.ParentDAGRun.Zero() {
		loadOpts = append(loadOpts, spec.WithSkipBaseHandlers())
	}
	if strings.TrimSpace(req.WorkDir) != "" {
		loadOpts = append(loadOpts, spec.WithDefaultWorkingDir(strings.TrimSpace(req.WorkDir)))
	}

	return spec.Load(ctx, req.DAG.Location, loadOpts...)
}

func (r *embeddedRuntime) readRunStatus(
	ctx context.Context,
	dag *core.DAG,
	runID string,
	root exec.DAGRunRef,
	parent exec.DAGRunRef,
) (*exec.RunStatus, error) {
	var (
		attempt exec.DAGRunAttempt
		err     error
	)

	if parent.Zero() {
		attempt, err = r.dagRunStore.FindAttempt(ctx, exec.NewDAGRunRef(dag.Name, runID))
	} else {
		attempt, err = r.dagRunStore.FindSubAttempt(ctx, root, runID)
	}
	if err != nil {
		return nil, err
	}

	status, err := attempt.ReadStatus(ctx)
	if err != nil {
		return nil, err
	}

	return &exec.RunStatus{
		Name:     status.Name,
		DAGRunID: status.DAGRunID,
		Params:   status.Params,
		Outputs:  extractOutputs(status.Nodes),
		Status:   status.Status,
	}, nil
}

func extractOutputs(nodes []*exec.Node) map[string]string {
	outputs := make(map[string]string)
	for _, node := range nodes {
		if node.OutputVariables == nil {
			continue
		}
		node.OutputVariables.Range(func(_, value any) bool {
			text, ok := value.(string)
			if !ok {
				return true
			}
			key, val, found := strings.Cut(text, "=")
			if found {
				outputs[key] = val
			}
			return true
		})
	}
	return outputs
}

func lockEmbeddedSubDAG(ctx context.Context) (context.Context, func()) {
	if locked, _ := ctx.Value(embeddedSubDAGLockKey{}).(bool); locked {
		return ctx, func() {}
	}

	embeddedSubDAGMu.Lock()
	return context.WithValue(ctx, embeddedSubDAGLockKey{}, true), func() {
		embeddedSubDAGMu.Unlock()
	}
}

func captureWorkingDir() (string, func(string)) {
	dir, err := os.Getwd()
	if err != nil {
		return "", nil
	}
	return dir, func(previous string) {
		_ = os.Chdir(previous)
	}
}

func loadEmbedConfig(ctx context.Context, opts StartOptions) (*config.Config, context.Context, error) {
	loader := config.NewConfigLoader(
		viper.New(),
		config.WithConfigFile(strings.TrimSpace(opts.ConfigFile)),
		config.WithService(config.ServiceAgent),
	)
	cfg, err := loader.Load()
	if err != nil {
		return nil, nil, err
	}
	if opts.Config != nil {
		overlay, decodeErr := decodeConfigOverlay(opts.Config)
		if decodeErr != nil {
			return nil, nil, decodeErr
		}
		applyConfigOverlay(cfg, overlay)
	}

	ctx = config.WithConfig(ctx, cfg)

	var loggerOpts []logger.Option
	if cfg.Core.Debug {
		loggerOpts = append(loggerOpts, logger.WithDebug())
	}
	if opts.Quiet {
		loggerOpts = append(loggerOpts, logger.WithQuiet())
	}
	if opts.LogWriter != nil {
		loggerOpts = append(loggerOpts, logger.WithWriter(opts.LogWriter))
	}
	if cfg.Core.LogFormat != "" {
		loggerOpts = append(loggerOpts, logger.WithFormat(cfg.Core.LogFormat))
	}
	return cfg, logger.WithLogger(ctx, logger.NewLogger(loggerOpts...)), nil
}

func decodeConfigOverlay(value any) (Config, error) {
	switch typed := value.(type) {
	case Config:
		return typed, nil
	case *Config:
		if typed == nil {
			return Config{}, nil
		}
		return *typed, nil
	}

	data, err := yaml.Marshal(value)
	if err != nil {
		return Config{}, fmt.Errorf("marshal embed config: %w", err)
	}

	var overlay Config
	if err := yaml.Unmarshal(data, &overlay); err != nil {
		return Config{}, fmt.Errorf("unmarshal embed config: %w", err)
	}
	return overlay, nil
}

func applyConfigOverlay(cfg *config.Config, overlay Config) {
	if overlay.SkipExamples != nil {
		cfg.Core.SkipExamples = *overlay.SkipExamples
	}
	if overlay.LatestStatusToday != nil {
		cfg.Server.LatestStatusToday = *overlay.LatestStatusToday
	}
	if overlay.Debug != nil {
		cfg.Core.Debug = *overlay.Debug
	}
	if overlay.LogFormat != nil {
		cfg.Core.LogFormat = strings.TrimSpace(*overlay.LogFormat)
	}
	if overlay.DagsDir != nil {
		cfg.Paths.DAGsDir = strings.TrimSpace(*overlay.DagsDir)
	}
	if overlay.DataDir != nil {
		cfg.Paths.DataDir = strings.TrimSpace(*overlay.DataDir)
	}
	if overlay.LogDir != nil {
		cfg.Paths.LogDir = strings.TrimSpace(*overlay.LogDir)
	}
	if overlay.AdminLogsDir != nil {
		cfg.Paths.AdminLogsDir = strings.TrimSpace(*overlay.AdminLogsDir)
	}
	if overlay.SuspendFlagsDir != nil {
		cfg.Paths.SuspendFlagsDir = strings.TrimSpace(*overlay.SuspendFlagsDir)
	}
	if overlay.BaseConfig != nil {
		cfg.Paths.BaseConfig = strings.TrimSpace(*overlay.BaseConfig)
	}
	if overlay.DAGRunsDir != nil {
		cfg.Paths.DAGRunsDir = strings.TrimSpace(*overlay.DAGRunsDir)
	}
	if overlay.QueueDir != nil {
		cfg.Paths.QueueDir = strings.TrimSpace(*overlay.QueueDir)
	}
	if overlay.ProcDir != nil {
		cfg.Paths.ProcDir = strings.TrimSpace(*overlay.ProcDir)
	}
	if overlay.ServiceRegistryDir != nil {
		cfg.Paths.ServiceRegistryDir = strings.TrimSpace(*overlay.ServiceRegistryDir)
	}

	derivePathDefaults(cfg, overlay)
}

func derivePathDefaults(cfg *config.Config, overlay Config) {
	if overlay.DataDir != nil {
		if overlay.DAGRunsDir == nil {
			cfg.Paths.DAGRunsDir = filepath.Join(cfg.Paths.DataDir, "dag-runs")
		}
		if overlay.ProcDir == nil {
			cfg.Paths.ProcDir = filepath.Join(cfg.Paths.DataDir, "proc")
		}
		if overlay.QueueDir == nil {
			cfg.Paths.QueueDir = filepath.Join(cfg.Paths.DataDir, "queue")
		}
		if overlay.ServiceRegistryDir == nil {
			cfg.Paths.ServiceRegistryDir = filepath.Join(cfg.Paths.DataDir, "service-registry")
		}
	}
}

func resolveRunID(raw string) (string, error) {
	runID := strings.TrimSpace(raw)
	if runID == "" {
		generated, err := uuid.NewV7()
		if err != nil {
			return "", err
		}
		runID = generated.String()
	}
	if err := exec.ValidateDAGRunID(runID); err != nil {
		return "", err
	}
	return runID, nil
}

func openLogFile(ctx context.Context, cfg *config.Config, dag *core.DAG, dagRunID string) (*os.File, string, error) {
	baseLogDir, err := eval.String(ctx, cfg.Paths.LogDir, eval.WithOSExpansion())
	if err != nil {
		return nil, "", fmt.Errorf("failed to expand log directory: %w", err)
	}

	dagLogDir, err := eval.String(ctx, dag.LogDir, eval.WithOSExpansion())
	if err != nil {
		return nil, "", fmt.Errorf("failed to expand DAG log directory: %w", err)
	}

	logDir, err := newLogConfig(baseLogDir, dagLogDir, dag.Name, dagRunID).logDir()
	if err != nil {
		return nil, "", err
	}

	logFile, err := fileutil.OpenOrCreateFile(filepath.Join(logDir, newLogConfig(baseLogDir, dagLogDir, dag.Name, dagRunID).logFile()))
	if err != nil {
		return nil, "", err
	}
	return logFile, logDir, nil
}

type logConfig struct {
	baseDir   string
	dagLogDir string
	name      string
	dagRunID  string
}

func newLogConfig(baseDir string, dagLogDir string, name string, dagRunID string) logConfig {
	return logConfig{
		baseDir:   baseDir,
		dagLogDir: dagLogDir,
		name:      name,
		dagRunID:  dagRunID,
	}
}

func (cfg logConfig) logDir() (string, error) {
	baseDir := cfg.baseDir
	if cfg.dagLogDir != "" {
		baseDir = cfg.dagLogDir
	}
	if baseDir == "" {
		return "", fmt.Errorf("base log directory is not set")
	}

	utcTimestamp := time.Now().UTC().Format("20060102_150405Z")
	safeName := fileutil.SafeName(cfg.name)
	logDir := filepath.Join(baseDir, safeName, "dag-run_"+utcTimestamp+"_"+cfg.dagRunID)
	if err := os.MkdirAll(logDir, 0o750); err != nil {
		return "", fmt.Errorf("failed to initialize directory %s: %w", logDir, err)
	}
	return logDir, nil
}

func (cfg logConfig) logFile() string {
	return "dag-run_" + time.Now().Format("20060102.150405.000") + ".log"
}
