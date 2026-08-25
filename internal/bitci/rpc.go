package bitci

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type RPCRequest struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

type RPCResponse struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type PlanParams struct {
	Paths []string `json:"paths"`
}

type JobParams struct {
	ID int64 `json:"id"`
}

type LogParams struct {
	ID    int64  `json:"id"`
	Limit int    `json:"limit"`
	Query string `json:"query"`
}

type LogCursorParams struct {
	ID     int64 `json:"id"`
	Cursor int64 `json:"cursor"`
	Limit  int   `json:"limit"`
}

type SubmitParams struct {
	TaskIDs []string `json:"task_ids"`
	Ref     string   `json:"ref"`
}

func DefaultSocketPath(configPath, stateDir string) string {
	return filepath.Join(DefaultStateDir(configPath, stateDir), "bitci.sock")
}

func (controller *Controller) Listen(socketPath string) (net.Listener, error) {
	if socketPath == "" {
		socketPath = DefaultSocketPath(controller.configPath, controller.stateDir)
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(socketPath); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("socket path %s is not a socket", socketPath)
		}
		connection, dialErr := net.DialTimeout("unix", socketPath, time.Second)
		if dialErr == nil {
			connection.Close()
			return nil, fmt.Errorf("controller already owns socket %s", socketPath)
		}
		if err := os.Remove(socketPath); err != nil {
			return nil, fmt.Errorf("remove stale socket %s: %w", socketPath, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", socketPath, err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		listener.Close()
		return nil, err
	}
	return &socketListener{Listener: listener, path: socketPath}, nil
}

type socketListener struct {
	net.Listener
	path string
}

func (listener *socketListener) Close() error {
	err := listener.Listener.Close()
	if removeErr := os.Remove(listener.path); removeErr != nil && !os.IsNotExist(removeErr) && err == nil {
		err = removeErr
	}
	return err
}

func (controller *Controller) ServeRPC(ctx context.Context, listener net.Listener) error {
	var handlers sync.WaitGroup
	connections := map[net.Conn]struct{}{}
	var connectionsMu sync.Mutex
	stopCloser := make(chan struct{})
	closerDone := make(chan struct{})
	defer func() {
		close(stopCloser)
		connectionsMu.Lock()
		for connection := range connections {
			_ = connection.Close()
		}
		connectionsMu.Unlock()
		<-closerDone
		handlers.Wait()
	}()
	go func() {
		defer close(closerDone)
		select {
		case <-ctx.Done():
			_ = listener.Close()
			connectionsMu.Lock()
			for connection := range connections {
				_ = connection.Close()
			}
			connectionsMu.Unlock()
		case <-stopCloser:
		}
	}()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if ctx.Err() != nil {
			_ = connection.Close()
			return nil
		}
		connectionsMu.Lock()
		if ctx.Err() != nil {
			connectionsMu.Unlock()
			_ = connection.Close()
			return nil
		}
		connections[connection] = struct{}{}
		connectionsMu.Unlock()
		handlers.Add(1)
		go func(connection net.Conn) {
			defer handlers.Done()
			defer func() {
				_ = connection.Close()
				connectionsMu.Lock()
				delete(connections, connection)
				connectionsMu.Unlock()
			}()
			_ = connection.SetDeadline(time.Now().Add(10 * time.Second))
			decoder := json.NewDecoder(connection)
			encoder := json.NewEncoder(connection)
			var request RPCRequest
			if err := decoder.Decode(&request); err != nil {
				_ = encoder.Encode(RPCResponse{Error: err.Error()})
				return
			}
			if ctx.Err() != nil {
				return
			}
			_ = encoder.Encode(controller.handleRPC(ctx, request))
		}(connection)
	}
}

func (controller *Controller) handleRPC(_ context.Context, request RPCRequest) RPCResponse {
	result := func(value any, err error) RPCResponse {
		if err != nil {
			return RPCResponse{Error: err.Error()}
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return RPCResponse{Error: err.Error()}
		}
		return RPCResponse{Result: encoded}
	}
	switch request.Method {
	case "status":
		return result(controller.Jobs())
	case "plan":
		var params PlanParams
		if err := json.Unmarshal(request.Params, &params); err != nil {
			return RPCResponse{Error: err.Error()}
		}
		return result(controller.Plan(params.Paths))
	case "tail_logs":
		var params LogParams
		if err := json.Unmarshal(request.Params, &params); err != nil {
			return RPCResponse{Error: err.Error()}
		}
		return result(controller.TailLog(params.ID, params.Limit))
	case "search_logs":
		var params LogParams
		if err := json.Unmarshal(request.Params, &params); err != nil {
			return RPCResponse{Error: err.Error()}
		}
		return result(controller.SearchLog(params.ID, params.Query, params.Limit))
	case "read_logs":
		var params LogCursorParams
		if err := json.Unmarshal(request.Params, &params); err != nil {
			return RPCResponse{Error: err.Error()}
		}
		return result(controller.ReadLog(params.ID, params.Cursor, params.Limit))
	case "doctor":
		return result(map[string]string{"disk": "OK"}, controller.DiskOK())
	case "submit":
		var params SubmitParams
		if err := json.Unmarshal(request.Params, &params); err != nil {
			return RPCResponse{Error: err.Error()}
		}
		return result(controller.Submit(params.TaskIDs, params.Ref))
	case "cancel":
		var params JobParams
		if err := json.Unmarshal(request.Params, &params); err != nil {
			return RPCResponse{Error: err.Error()}
		}
		cancelled, err := controller.Cancel(params.ID)
		return result(map[string]bool{"cancelled": cancelled}, err)
	case "retry":
		var params JobParams
		if err := json.Unmarshal(request.Params, &params); err != nil {
			return RPCResponse{Error: err.Error()}
		}
		return result(controller.Retry(params.ID))
	default:
		return RPCResponse{Error: fmt.Sprintf("unknown method %q", request.Method)}
	}
}

func Call(socketPath, method string, params any, output any) error {
	connection, err := net.DialTimeout("unix", socketPath, 5*time.Second)
	if err != nil {
		return fmt.Errorf("connect local controller: %w", err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	encodedParams, err := json.Marshal(params)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(connection).Encode(RPCRequest{Method: method, Params: encodedParams}); err != nil {
		return err
	}
	var response RPCResponse
	if err := json.NewDecoder(connection).Decode(&response); err != nil {
		return err
	}
	if response.Error != "" {
		return fmt.Errorf("controller: %s", response.Error)
	}
	return json.Unmarshal(response.Result, output)
}
