package bitci

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type MCPOptions struct {
	SocketPath string
	AllowRuns  bool
}

type MCPJob struct {
	ID        int64  `json:"id"`
	Task      string `json:"task"`
	Ref       string `json:"ref"`
	TestedSHA string `json:"tested_sha,omitempty"`
	State     string `json:"state"`
	ExitCode  *int   `json:"exit_code,omitempty"`
}

type MCPStatus struct {
	Jobs []MCPJob `json:"jobs"`
}

type MCPPlanInput struct {
	Paths []string `json:"paths" jsonschema:"changed repository paths"`
}

type MCPPlanOutput struct {
	TaskIDs []string `json:"task_ids"`
}

type MCPLogInput struct {
	ID    int64  `json:"id" jsonschema:"BitCI job ID"`
	Limit int    `json:"limit,omitempty" jsonschema:"maximum lines, capped at 80"`
	Query string `json:"query,omitempty" jsonschema:"required for search_logs"`
}

type MCPLogOutput struct {
	Lines []string `json:"lines"`
}

type MCPLogCursorInput struct {
	ID     int64 `json:"id" jsonschema:"BitCI job ID"`
	Cursor int64 `json:"cursor,omitempty" jsonschema:"byte cursor from the prior read_logs result"`
	Limit  int   `json:"limit,omitempty" jsonschema:"maximum lines, capped at 80"`
}

type MCPLogCursorOutput struct {
	Lines  []string `json:"lines"`
	Cursor int64    `json:"cursor"`
	State  string   `json:"state"`
}

type MCPSubmitInput struct {
	TaskIDs []string `json:"task_ids" jsonschema:"configured task IDs only"`
	Ref     string   `json:"ref,omitempty" jsonschema:"source reference"`
}

type MCPJobInput struct {
	ID int64 `json:"id" jsonschema:"BitCI job ID"`
}

func RunMCP(ctx context.Context, options MCPOptions) error {
	server := mcp.NewServer(&mcp.Implementation{Name: "bitci", Version: Version}, nil)
	call := func(method string, params any, output any) error {
		return Call(options.SocketPath, method, params, output)
	}
	mcp.AddTool(server, &mcp.Tool{Name: "status", Description: "Read a compact summary of local BitCI jobs."}, func(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, MCPStatus, error) {
		var jobs []Job
		if err := call("status", struct{}{}, &jobs); err != nil {
			return nil, MCPStatus{}, err
		}
		if len(jobs) > 25 {
			jobs = jobs[len(jobs)-25:]
		}
		result := MCPStatus{Jobs: make([]MCPJob, 0, len(jobs))}
		for _, job := range jobs {
			result.Jobs = append(result.Jobs, MCPJob{ID: job.ID, Task: job.Task, Ref: job.Ref, TestedSHA: job.TestedSHA, State: job.State, ExitCode: job.ExitCode})
		}
		return nil, result, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "plan", Description: "Select configured task IDs for changed paths. Submit only this plan."}, func(_ context.Context, _ *mcp.CallToolRequest, input MCPPlanInput) (*mcp.CallToolResult, MCPPlanOutput, error) {
		var taskIDs []string
		if err := call("plan", PlanParams{Paths: input.Paths}, &taskIDs); err != nil {
			return nil, MCPPlanOutput{}, err
		}
		return nil, MCPPlanOutput{TaskIDs: taskIDs}, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "tail_logs", Description: "Read the final capped lines for one BitCI job."}, func(_ context.Context, _ *mcp.CallToolRequest, input MCPLogInput) (*mcp.CallToolResult, MCPLogOutput, error) {
		var lines []string
		if err := call("tail_logs", LogParams{ID: input.ID, Limit: input.Limit}, &lines); err != nil {
			return nil, MCPLogOutput{}, err
		}
		return nil, MCPLogOutput{Lines: lines}, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "search_logs", Description: "Search capped lines for one BitCI job."}, func(_ context.Context, _ *mcp.CallToolRequest, input MCPLogInput) (*mcp.CallToolResult, MCPLogOutput, error) {
		var lines []string
		if err := call("search_logs", LogParams{ID: input.ID, Limit: input.Limit, Query: input.Query}, &lines); err != nil {
			return nil, MCPLogOutput{}, err
		}
		return nil, MCPLogOutput{Lines: lines}, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "read_logs", Description: "Read new capped log lines from a byte cursor for one BitCI job."}, func(_ context.Context, _ *mcp.CallToolRequest, input MCPLogCursorInput) (*mcp.CallToolResult, MCPLogCursorOutput, error) {
		var output MCPLogCursorOutput
		if err := call("read_logs", LogCursorParams{ID: input.ID, Cursor: input.Cursor, Limit: input.Limit}, &output); err != nil {
			return nil, MCPLogCursorOutput{}, err
		}
		return nil, output, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "doctor", Description: "Check the BitCI disk guard."}, func(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, map[string]string, error) {
		var result map[string]string
		if err := call("doctor", struct{}{}, &result); err != nil {
			return nil, nil, err
		}
		return nil, result, nil
	})
	if options.AllowRuns {
		addRunTools(server, call)
	}
	return server.Run(ctx, &mcp.StdioTransport{})
}

func addRunTools(server *mcp.Server, call func(string, any, any) error) {
	mcp.AddTool(server, &mcp.Tool{Name: "submit", Description: "Queue configured task IDs after calling plan."}, func(_ context.Context, _ *mcp.CallToolRequest, input MCPSubmitInput) (*mcp.CallToolResult, MCPStatus, error) {
		var jobs []Job
		if err := call("submit", SubmitParams{TaskIDs: input.TaskIDs, Ref: input.Ref}, &jobs); err != nil {
			return nil, MCPStatus{}, err
		}
		result := MCPStatus{Jobs: make([]MCPJob, 0, len(jobs))}
		for _, job := range jobs {
			result.Jobs = append(result.Jobs, MCPJob{ID: job.ID, Task: job.Task, Ref: job.Ref, State: job.State})
		}
		return nil, result, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "cancel", Description: "Cancel one queued BitCI job."}, func(_ context.Context, _ *mcp.CallToolRequest, input MCPJobInput) (*mcp.CallToolResult, map[string]bool, error) {
		var result map[string]bool
		if err := call("cancel", JobParams{ID: input.ID}, &result); err != nil {
			return nil, nil, err
		}
		return nil, result, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "retry", Description: "Retry one configured BitCI job after log inspection."}, func(_ context.Context, _ *mcp.CallToolRequest, input MCPJobInput) (*mcp.CallToolResult, MCPStatus, error) {
		var jobs []Job
		if err := call("retry", JobParams{ID: input.ID}, &jobs); err != nil {
			return nil, MCPStatus{}, err
		}
		result := MCPStatus{Jobs: make([]MCPJob, 0, len(jobs))}
		for _, job := range jobs {
			result.Jobs = append(result.Jobs, MCPJob{ID: job.ID, Task: job.Task, Ref: job.Ref, State: job.State})
		}
		return nil, result, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "recover", Description: "Fail one running job only when its recorded task process group is gone."}, func(_ context.Context, _ *mcp.CallToolRequest, input MCPJobInput) (*mcp.CallToolResult, map[string]bool, error) {
		var result map[string]bool
		if err := call("recover", JobParams{ID: input.ID}, &result); err != nil {
			return nil, nil, err
		}
		return nil, result, nil
	})
}
