// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package reporter

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/apache/skywalking-go/plugins/core/operator"
	commonv3 "github.com/apache/skywalking-go/protocols/collect/common/v3"
	pprofv10 "github.com/apache/skywalking-go/protocols/collect/pprof/v10"
)

const (
	// max chunk size for pprof data
	maxChunkSize = 1 * 1024 * 1024
	// max send queue size for pprof data
	maxPprofSendQueueSize = 30000
	// max duration for pprof task
	pprofTaskDurationMaxMinute = 15 * time.Minute
)

type PprofTaskCommand interface {
	GetTaskID() string
	GetCreateTime() int64
	GetDuration() time.Duration
	GetDumpPeriod() int
	StartTask() (io.Writer, error)
	StopTask(io.Writer)
	IsDirectSamplingType() bool
	IsInvalidEvent() bool
	HasDumpPeriod() bool
}
type PprofReporter interface {
	ReportPprof(taskID string, content []byte)
}

var NewPprofTaskCommand func(taskID, events string, duration time.Duration,
	createTime int64, dumpPeriod int, pprofFilePath string,
	logger operator.LogOperator, manager PprofReporter) PprofTaskCommand

type PprofTaskManager struct {
	logger         operator.LogOperator
	serverAddr     string
	pprofInterval  time.Duration
	connManager    *ConnectionManager
	entity         *Entity
	pprofFilePath  string
	LastUpdateTime int64
	commands       PprofTaskCommand
	pprofSendCh    chan *pprofv10.PprofData
	// pprofClientBundle is swapped atomically so poll vs upload cannot race
	// on the client interface field (Codex P2).
	pprofClientBundle atomic.Pointer[pprofClientBundle]
}

type pprofClientBundle struct {
	conn   *grpc.ClientConn
	client pprofv10.PprofTaskClient
}

func (r *PprofTaskManager) storePprofClient(conn *grpc.ClientConn, client pprofv10.PprofTaskClient) {
	if client == nil {
		return
	}
	r.pprofClientBundle.Store(&pprofClientBundle{conn: conn, client: client})
}

func (r *PprofTaskManager) loadPprofClient() pprofv10.PprofTaskClient {
	b := r.pprofClientBundle.Load()
	if b == nil {
		return nil
	}
	return b.client
}

// currentPprofClient returns a stub bound to the latest PeekConnection for
// multi-backend, publishing a new bundle only when the ClientConn identity changes.
func (r *PprofTaskManager) currentPprofClient() pprofv10.PprofTaskClient {
	if r.connManager != nil && r.connManager.IsMultiBackend() {
		if conn := r.connManager.PeekConnection(r.serverAddr); conn != nil {
			if cur := r.pprofClientBundle.Load(); cur != nil && cur.conn == conn {
				return cur.client
			}
			client := pprofv10.NewPprofTaskClient(conn)
			r.storePprofClient(conn, client)
			return client
		}
	}
	return r.loadPprofClient()
}

func NewPprofTaskManager(logger operator.LogOperator, serverAddr string,
	pprofInterval time.Duration, connManager *ConnectionManager,
	pprofFilePath string) (*PprofTaskManager, error) {
	if pprofInterval <= 0 {
		logger.Errorf("pprof interval less than zero, pprof profiling is disabled")
		return nil, fmt.Errorf("pprof interval less than zero, pprof profiling is disabled")
	}
	pprofManager := &PprofTaskManager{
		logger:        logger,
		serverAddr:    serverAddr,
		pprofInterval: pprofInterval,
		connManager:   connManager,
		pprofFilePath: pprofFilePath,
		pprofSendCh:   make(chan *pprofv10.PprofData, maxPprofSendQueueSize),
	}
	conn, err := connManager.GetConnection(serverAddr)
	if err != nil {
		return nil, err
	}
	pprofManager.storePprofClient(conn, pprofv10.NewPprofTaskClient(conn))
	pprofManager.commands = nil
	return pprofManager, nil
}

func (r *PprofTaskManager) InitPprofTask(entity *Entity) {
	r.entity = entity
	r.initPprofSendPipeline()
	go func() {
		for {
			switch r.connManager.GetConnectionStatus(r.serverAddr) {
			case ConnectionStatusShutdown:
				return
			case ConnectionStatusDisconnect:
				time.Sleep(r.pprofInterval)
				continue
			}
			ctx := context.Background()
			cancel := func() {}
			if r.connManager.IsMultiBackend() {
				ctx, cancel = BackendRPCContext(r.serverAddr, r.pprofInterval)
			}
			client := r.currentPprofClient()
			if client == nil {
				cancel()
				time.Sleep(r.pprofInterval)
				continue
			}
			pprofCommand, err := client.GetPprofTaskCommands(ctx, &pprofv10.PprofTaskCommandQuery{
				Service:         r.entity.ServiceName,
				ServiceInstance: r.entity.ServiceInstanceName,
				LastCommandTime: r.LastUpdateTime,
			})
			cancel()
			if err != nil {
				r.logger.Errorf("fetch pprof task commands error %v", err)
				time.Sleep(r.pprofInterval)
				continue
			}

			if len(pprofCommand.GetCommands()) > 0 && pprofCommand.GetCommands()[0].Command == "PprofTaskQuery" {
				rawCommand := pprofCommand.GetCommands()[0]
				r.HandleCommand(rawCommand)
			}

			time.Sleep(r.pprofInterval)
		}
	}()
}

func (r *PprofTaskManager) HandleCommand(rawCommand *commonv3.Command) {
	command := r.deserializePprofTaskCommand(rawCommand)
	if command.GetCreateTime() > r.LastUpdateTime {
		r.LastUpdateTime = command.GetCreateTime()
	} else {
		return
	}
	if err := r.checkCommand(command); err != nil {
		r.logger.Errorf("check command error, cannot process this pprof task. reason: %v", err)
		return
	}

	if command.IsDirectSamplingType() {
		// direct sampling of Heap, Allocs, Goroutine, Thread
		writer, err := command.StartTask()
		if err != nil {
			err = fmt.Errorf("start %s pprof task error %v", command.GetTaskID(), err)
			r.ReportPprofError(command.GetTaskID(), err)
			r.logger.Errorf(err.Error())
			return
		}
		command.StopTask(writer)
	} else {
		// The CPU, Block and Mutex sampling lasts for a duration and then stops
		writer, err := command.StartTask()
		if err != nil {
			err = fmt.Errorf("start %s pprof task error %v", command.GetTaskID(), err)
			r.ReportPprofError(command.GetTaskID(), err)
			r.logger.Errorf(err.Error())
			return
		}
		time.AfterFunc(command.GetDuration(), func() {
			command.StopTask(writer)
		})
	}
}

func (r *PprofTaskManager) deserializePprofTaskCommand(command *commonv3.Command) PprofTaskCommand {
	args := command.Args
	taskID := ""
	events := ""
	duration := 0
	dumpPeriod := 0 // Use -1 to indicate no explicit value provided
	var createTime int64 = 0
	for _, pair := range args {
		if pair.GetKey() == "TaskId" {
			taskID = pair.GetValue()
		} else if pair.GetKey() == "Events" {
			events = pair.GetValue()
		} else if pair.GetKey() == "Duration" {
			if val, err := strconv.Atoi(pair.GetValue()); err == nil && val > 0 {
				duration = val
			}
		} else if pair.GetKey() == "DumpPeriod" {
			if val, err := strconv.Atoi(pair.GetValue()); err == nil && val >= 0 {
				dumpPeriod = val
			}
		} else if pair.GetKey() == "CreateTime" {
			createTime, _ = strconv.ParseInt(pair.GetValue(), 10, 64)
		}
	}

	return NewPprofTaskCommand(
		taskID,
		events,
		time.Duration(duration)*time.Minute,
		createTime,
		dumpPeriod,
		r.pprofFilePath,
		r.logger,
		r,
	)
}

func (r *PprofTaskManager) checkCommand(command PprofTaskCommand) error {
	if command.GetTaskID() == "" {
		return fmt.Errorf("pprof task id cannot be empty, task id is %s", command.GetTaskID())
	}
	if command.IsInvalidEvent() {
		return fmt.Errorf("pprof task event is invalid, task id is %s", command.GetTaskID())
	}
	if !command.IsDirectSamplingType() {
		if command.GetDuration() <= 0 || command.GetDuration() > pprofTaskDurationMaxMinute {
			return fmt.Errorf("pprof task duration must be between 0 and %v, task id is %s", pprofTaskDurationMaxMinute, command.GetTaskID())
		}
	}
	if command.HasDumpPeriod() && command.GetDumpPeriod() <= 0 {
		return fmt.Errorf("pprof task dumpperiod must be greater than 0, task id is %s", command.GetTaskID())
	}
	return nil
}

func (r *PprofTaskManager) ReportPprof(taskID string, content []byte) {
	metaData := &pprofv10.PprofMetaData{
		Service:         r.entity.ServiceName,
		ServiceInstance: r.entity.ServiceInstanceName,
		TaskId:          taskID,
		Type:            pprofv10.PprofProfilingStatus_PPROF_PROFILING_SUCCESS,
		ContentSize:     int32(len(content)),
	}

	pprofData := &pprofv10.PprofData{
		Metadata: metaData,
		Result: &pprofv10.PprofData_Content{
			Content: content,
		},
	}

	select {
	case r.pprofSendCh <- pprofData:
	default:
		r.logger.Errorf("reach max pprof send buffer")
	}
}

func (r *PprofTaskManager) ReportPprofError(taskID string, err error) {
	metaData := &pprofv10.PprofMetaData{
		Service:         r.entity.ServiceName,
		ServiceInstance: r.entity.ServiceInstanceName,
		TaskId:          taskID,
		Type:            pprofv10.PprofProfilingStatus_PPROF_EXECUTION_TASK_ERROR,
		ContentSize:     0,
	}

	pprofData := &pprofv10.PprofData{
		Metadata: metaData,
		Result: &pprofv10.PprofData_ErrorMessage{
			ErrorMessage: err.Error(),
		},
	}

	select {
	case r.pprofSendCh <- pprofData:
	default:
		r.logger.Errorf("reach max pprof send buffer")
	}
}

func (r *PprofTaskManager) initPprofSendPipeline() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				r.logger.Errorf("PprofTaskManager initPprofSendPipeline panic err %v", err)
			}
		}()
	StreamLoop:
		for {
			switch r.connManager.GetConnectionStatus(r.serverAddr) {
			case ConnectionStatusShutdown:
				return
			case ConnectionStatusDisconnect:
				time.Sleep(5 * time.Second)
				continue StreamLoop
			}

			for pprofData := range r.pprofSendCh {
				r.uploadPprofData(pprofData)
			}
			break
		}
	}()
}

func (r *PprofTaskManager) uploadPprofData(pprofData *pprofv10.PprofData) {
	err := r.uploadPprofDataOnce(pprofData)
	if err == nil {
		return
	}
	r.logger.Errorf("failed to upload pprof: %v", err)
	if r.connManager == nil || !r.connManager.IsMultiBackend() {
		return
	}
	// Refresh stub from the current ClientConn and retry once after recreate/failover.
	_ = r.currentPprofClient()
	if err := r.uploadPprofDataOnce(pprofData); err != nil {
		r.logger.Errorf("retry upload pprof failed: %v", err)
	}
}

func (r *PprofTaskManager) uploadPprofDataOnce(pprofData *pprofv10.PprofData) error {
	client := r.currentPprofClient()
	if client == nil {
		return fmt.Errorf("pprof client unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stream, err := client.Collect(ctx)
	if err != nil {
		return fmt.Errorf("start collect stream: %w", err)
	}

	// Send metadata first
	metadataMsg := &pprofv10.PprofData{
		Metadata: pprofData.Metadata,
	}
	if err = stream.Send(metadataMsg); err != nil {
		return fmt.Errorf("send metadata: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("receive server response: %w", err)
	}

	switch resp.Status {
	case pprofv10.PprofProfilingStatus_PPROF_TERMINATED_BY_OVERSIZE:
		r.closePprofStream(stream)
		return fmt.Errorf("pprof is too large to be received by the oap server")
	case pprofv10.PprofProfilingStatus_PPROF_EXECUTION_TASK_ERROR:
		r.closePprofStream(stream)
		return fmt.Errorf("server rejected pprof upload due to execution task error")
	}

	// Upload content in chunks
	content := pprofData.GetContent()
	chunkCount := 0
	contentSize := len(content)

	for offset := 0; offset < contentSize; offset += maxChunkSize {
		end := offset + maxChunkSize
		if end > contentSize {
			end = contentSize
		}

		chunkData := &pprofv10.PprofData{
			Result: &pprofv10.PprofData_Content{
				Content: content[offset:end],
			},
		}

		if err := stream.Send(chunkData); err != nil {
			return fmt.Errorf("send pprof chunk %d: %w", chunkCount, err)
		}
		chunkCount++
		// Check context timeout
		select {
		case <-ctx.Done():
			return fmt.Errorf("context timeout during chunk upload for task %s", pprofData.Metadata.TaskId)
		default:
		}
	}

	r.closePprofStream(stream)
	return nil
}
func (r *PprofTaskManager) closePprofStream(stream pprofv10.PprofTask_CollectClient) {
	if err := stream.CloseSend(); err != nil {
		r.logger.Errorf("failed to close send stream: %v", err)
		return
	}

	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			r.logger.Errorf("error receiving final response %v", err)
			break
		}
	}
}
