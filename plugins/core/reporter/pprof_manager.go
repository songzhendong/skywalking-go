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
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

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

// activePprofTask tracks a duration-based profiling session so Close can
// always StopTask (CPU / block / mutex rate) even if the AfterFunc is canceled.
type activePprofTask struct {
	command  PprofTaskCommand
	writer   io.Writer
	timer    *time.Timer
	stopOnce sync.Once
}

func (t *activePprofTask) stop() {
	t.stopOnce.Do(func() {
		if t.timer != nil {
			t.timer.Stop()
		}
		t.command.StopTask(t.writer)
	})
}

type PprofTaskManager struct {
	logger         operator.LogOperator
	serverAddr     string
	pprofInterval  time.Duration
	PprofClient    pprofv10.PprofTaskClient // for grpc
	connManager    *ConnectionManager
	entity         *Entity
	pprofFilePath  string
	LastUpdateTime int64
	commands       PprofTaskCommand
	pprofSendCh    chan *pprofv10.PprofData
	closeOnce      sync.Once
	sendMu         sync.Mutex
	// rejectCommands is set first so HandleCommand stops accepting work while
	// StopTask can still ReportPprof into pprofSendCh.
	rejectCommands bool
	closed         bool // true once pprofSendCh is closed
	// closingCh is closed when Close marks the manager closed, so trySendPprof
	// can unblock instead of waiting forever on a full queue.
	closingCh      chan struct{}
	sendPipelineWG sync.WaitGroup
	// senderWG tracks trySendPprof callers that passed the !closed check so
	// Close can wait for them to finish the blocking enqueue before closing
	// the channel (avoids send-on-closed panic and drops).
	senderWG sync.WaitGroup
	// commandWG tracks in-flight HandleCommand calls so Close can wait for
	// StartTask/StopTask races before closing the send channel.
	commandWG   sync.WaitGroup
	timerMu     sync.Mutex
	activeTasks []*activePprofTask
	// uploadCancels tracks in-flight Collect contexts so Close can cancel a
	// long upload instead of waiting up to 60s.
	uploadMu      sync.Mutex
	uploadCancels map[*pprofUploadCancel]struct{}
}

type pprofUploadCancel struct {
	cancel context.CancelFunc
}

// Close rejects new commands, stops any running profiling tasks (so CPU/block/
// mutex profiling cannot leak), waits for StopTask final reports to enqueue,
// then closes the upload channel. The send pipeline keeps draining while the
// channel is open, so enqueue blocks rather than dropping under backpressure.
func (r *PprofTaskManager) Close() {
	r.closeOnce.Do(func() {
		r.sendMu.Lock()
		r.rejectCommands = true
		r.sendMu.Unlock()

		// Cancel in-flight uploads promptly so the queue can drain and
		// trySendPprof callers are not stuck behind a 60s Collect.
		r.cancelInflightUploads()

		// Wait for in-flight HandleCommand (may StartTask then StopTask/ReportPprof).
		r.commandWG.Wait()

		// Lock order: timerMu then sendMu (same as HandleCommand registration).
		r.timerMu.Lock()
		r.sendMu.Lock()
		tasks := r.activeTasks
		r.activeTasks = nil
		r.sendMu.Unlock()
		r.timerMu.Unlock()

		for _, t := range tasks {
			t.stop()
		}

		// Mark closed before waiting: senders registered while the channel was
		// open finish below, and any later sender bails out instead of racing
		// close (send on a closed channel would panic).
		r.sendMu.Lock()
		r.closed = true
		select {
		case <-r.closingCh:
		default:
			close(r.closingCh)
		}
		r.sendMu.Unlock()

		// Unblock any trySend still waiting on a full queue, then wait for them.
		r.cancelInflightUploads()
		r.senderWG.Wait()

		r.sendMu.Lock()
		defer r.sendMu.Unlock()
		if r.pprofSendCh != nil {
			close(r.pprofSendCh)
			// Keep the closed channel reference so the send pipeline can range it
			// and exit; never assign nil (ranging a nil chan blocks forever).
		}
	})
}

func (r *PprofTaskManager) isClosed() bool {
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	return r.rejectCommands || r.closed
}

// WaitSendPipeline blocks until the pprof upload goroutine exits after Close.
func (r *PprofTaskManager) WaitSendPipeline() {
	r.sendPipelineWG.Wait()
}

// trySendPprof enqueues pprof data without dropping under normal load. It
// blocks until the send pipeline accepts the item, or Close has marked closed
// (closingCh), so Close cannot stall forever on a full queue. sendMu is never
// held across the channel send so the uploader can keep draining.
//
// Do not select on ShutdownNotify here: SignalShutdown runs before Close, and
// StopTask still needs to enqueue the final report while the channel is open.
func (r *PprofTaskManager) trySendPprof(pprofData *pprofv10.PprofData) {
	r.sendMu.Lock()
	if r.closed || r.pprofSendCh == nil {
		r.sendMu.Unlock()
		return
	}
	ch := r.pprofSendCh
	closingCh := r.closingCh
	r.senderWG.Add(1)
	r.sendMu.Unlock()
	defer r.senderWG.Done()

	select {
	case ch <- pprofData:
	case <-closingCh:
		// Close is waiting on senderWG; drop to unblock rather than hang.
	}
}

func (r *PprofTaskManager) trackUploadCancel(h *pprofUploadCancel) {
	r.uploadMu.Lock()
	if r.uploadCancels == nil {
		r.uploadCancels = make(map[*pprofUploadCancel]struct{})
	}
	r.uploadCancels[h] = struct{}{}
	r.uploadMu.Unlock()
}

func (r *PprofTaskManager) untrackUploadCancel(h *pprofUploadCancel) {
	r.uploadMu.Lock()
	delete(r.uploadCancels, h)
	r.uploadMu.Unlock()
}

func (r *PprofTaskManager) cancelInflightUploads() {
	r.uploadMu.Lock()
	cancels := make([]*pprofUploadCancel, 0, len(r.uploadCancels))
	for h := range r.uploadCancels {
		cancels = append(cancels, h)
	}
	r.uploadMu.Unlock()
	for _, h := range cancels {
		if h != nil && h.cancel != nil {
			h.cancel()
		}
	}
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
		closingCh:     make(chan struct{}),
		uploadCancels: make(map[*pprofUploadCancel]struct{}),
	}
	conn, err := connManager.GetConnection(serverAddr)
	if err != nil {
		return nil, err
	}
	pprofManager.PprofClient = pprofv10.NewPprofTaskClient(conn)
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
				if !r.connManager.Wait(r.pprofInterval) {
					return
				}
				continue
			}
			pprofCommand, err := r.PprofClient.GetPprofTaskCommands(context.Background(), &pprofv10.PprofTaskCommandQuery{
				Service:         r.entity.ServiceName,
				ServiceInstance: r.entity.ServiceInstanceName,
				LastCommandTime: r.LastUpdateTime,
			})
			if err != nil {
				r.logger.Errorf("fetch pprof task commands error %v", err)
				if !r.connManager.Wait(r.pprofInterval) {
					return
				}
				continue
			}

			if len(pprofCommand.GetCommands()) > 0 && pprofCommand.GetCommands()[0].Command == "PprofTaskQuery" {
				rawCommand := pprofCommand.GetCommands()[0]
				r.HandleCommand(rawCommand)
			}

			if !r.connManager.Wait(r.pprofInterval) {
				return
			}
		}
	}()
}

func (r *PprofTaskManager) HandleCommand(rawCommand *commonv3.Command) {
	if !r.beginCommand() {
		return
	}
	defer r.endCommand()

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
		if r.isClosed() {
			command.StopTask(writer)
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
		tracked := &activePprofTask{command: command, writer: writer}
		timer := time.AfterFunc(command.GetDuration(), func() {
			r.finishActiveTask(tracked)
		})
		tracked.timer = timer

		// Lock order: timerMu then sendMu (same as Close).
		r.timerMu.Lock()
		r.sendMu.Lock()
		closed := r.rejectCommands || r.closed
		if !closed {
			r.activeTasks = append(r.activeTasks, tracked)
		}
		r.sendMu.Unlock()
		r.timerMu.Unlock()
		if closed {
			// Close raced after StartTask: always stop profiling immediately.
			// Channel is still open until commandWG Wait completes, so ReportPprof works.
			tracked.stop()
		}
	}
}

func (r *PprofTaskManager) beginCommand() bool {
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	if r.rejectCommands || r.closed {
		return false
	}
	r.commandWG.Add(1)
	return true
}

func (r *PprofTaskManager) endCommand() {
	r.commandWG.Done()
}

// finishActiveTask stops the task before deregistering it so a concurrent Close
// still sees it, blocks on the shared stopOnce and therefore waits for the final
// ReportPprof to enqueue before the send channel is closed.
func (r *PprofTaskManager) finishActiveTask(tracked *activePprofTask) {
	tracked.stop()
	r.timerMu.Lock()
	for i, t := range r.activeTasks {
		if t == tracked {
			r.activeTasks = append(r.activeTasks[:i], r.activeTasks[i+1:]...)
			break
		}
	}
	r.timerMu.Unlock()
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

	r.trySendPprof(pprofData)
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

	r.trySendPprof(pprofData)
}

func (r *PprofTaskManager) initPprofSendPipeline() {
	r.sendPipelineWG.Add(1)
	go func() {
		defer r.sendPipelineWG.Done()
		for {
			panicked := false
			func() {
				defer func() {
					if err := recover(); err != nil {
						r.logger.Errorf("PprofTaskManager initPprofSendPipeline panic err %v", err)
						panicked = true
					}
				}()
				r.runPprofSendLoop()
			}()
			if !panicked {
				return
			}
			// Restart the consumer: trySendPprof blocks on a full buffer and
			// Close waits for those senders, so the channel must keep draining.
			select {
			case <-r.connManager.ShutdownNotify():
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
}

func (r *PprofTaskManager) runPprofSendLoop() {
StreamLoop:
	for {
		switch r.connManager.GetConnectionStatus(r.serverAddr) {
		case ConnectionStatusShutdown:
			// Wait for Close to close the channel, then drain/upload what remains.
			for pprofData := range r.pprofSendCh {
				r.uploadPprofData(pprofData)
			}
			return
		case ConnectionStatusDisconnect:
			// Keep draining so Close/trySend cannot block forever on a full
			// buffer while we wait to reconnect.
			select {
			case pprofData, ok := <-r.pprofSendCh:
				if !ok {
					return
				}
				r.uploadPprofData(pprofData)
			case <-r.connManager.ShutdownNotify():
				for pprofData := range r.pprofSendCh {
					r.uploadPprofData(pprofData)
				}
				return
			case <-time.After(5 * time.Second):
			}
			continue StreamLoop
		}

		for pprofData := range r.pprofSendCh {
			r.uploadPprofData(pprofData)
		}
		return
	}
}

// maxPprofUploadAttempts bounds retries for one payload: a backend that keeps
// failing must not wedge the queue, because trySendPprof blocks on a full buffer.
const maxPprofUploadAttempts = 3

// uploadPprofData retries a transient failure a few times, then gives up on this
// payload so the pipeline keeps draining. It never discards while the upload can
// still succeed, and a terminal server rejection stops retrying immediately.
func (r *PprofTaskManager) uploadPprofData(pprofData *pprofv10.PprofData) {
	for attempt := 1; ; attempt++ {
		err := r.tryUploadPprofData(pprofData)
		if err == nil {
			return
		}
		if errors.Is(err, errPprofUploadTerminal) {
			return
		}
		if attempt >= maxPprofUploadAttempts {
			r.logger.Errorf("pprof upload failed after %d attempts, giving up: %v", attempt, err)
			return
		}
		r.logger.Errorf("pprof upload failed, will retry: %v", err)
		if !r.connManager.Wait(time.Second) {
			// Shutdown: one last short attempt, then give up so the pipeline can exit.
			if retryErr := r.tryUploadPprofData(pprofData); retryErr != nil {
				r.logger.Errorf("pprof upload failed during shutdown: %v", retryErr)
			}
			return
		}
	}
}

var errPprofUploadTerminal = errors.New("pprof upload rejected by server")

func (r *PprofTaskManager) tryUploadPprofData(pprofData *pprofv10.PprofData) error {
	timeout := 60 * time.Second
	select {
	case <-r.connManager.ShutdownNotify():
		timeout = 3 * time.Second
	case <-r.closingCh:
		timeout = 3 * time.Second
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	h := &pprofUploadCancel{cancel: cancel}
	r.trackUploadCancel(h)
	defer func() {
		cancel()
		r.untrackUploadCancel(h)
	}()

	stream, err := r.PprofClient.Collect(ctx)
	if err != nil {
		return fmt.Errorf("failed to start collect stream: %w", err)
	}

	metadataMsg := &pprofv10.PprofData{
		Metadata: pprofData.Metadata,
	}
	if err = stream.Send(metadataMsg); err != nil {
		return fmt.Errorf("failed to send metadata: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive server response: %w", err)
	}

	switch resp.Status {
	case pprofv10.PprofProfilingStatus_PPROF_TERMINATED_BY_OVERSIZE:
		r.logger.Errorf("pprof is too large to be received by the oap server")
		r.closePprofStream(stream)
		return errPprofUploadTerminal
	case pprofv10.PprofProfilingStatus_PPROF_EXECUTION_TASK_ERROR:
		r.logger.Errorf("server rejected pprof upload due to execution task error")
		r.closePprofStream(stream)
		return errPprofUploadTerminal
	}

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
			return fmt.Errorf("failed to send pprof chunk %d: %w", chunkCount, err)
		}
		chunkCount++
		select {
		case <-ctx.Done():
			return fmt.Errorf("context timeout during chunk upload for task %s", pprofData.Metadata.TaskId)
		case <-r.connManager.ShutdownNotify():
			return fmt.Errorf("shutdown during pprof chunk upload")
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

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				r.logger.Errorf("error receiving final response %v", err)
				return
			}
		}
	}()

	timeout := 3 * time.Second
	select {
	case <-done:
		return
	case <-r.connManager.ShutdownNotify():
		timeout = time.Second
	case <-time.After(timeout):
		r.logger.Errorf("timed out waiting for pprof stream close")
		return
	}
	select {
	case <-done:
	case <-time.After(timeout):
		r.logger.Errorf("timed out waiting for pprof stream close after shutdown")
	}
}
