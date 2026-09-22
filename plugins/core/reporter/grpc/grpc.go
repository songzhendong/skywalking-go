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

package grpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/apache/skywalking-go/plugins/core/operator"
	"github.com/apache/skywalking-go/plugins/core/reporter"
	common "github.com/apache/skywalking-go/protocols/collect/common/v3"
	agentv3 "github.com/apache/skywalking-go/protocols/collect/language/agent/v3"
	profilev3 "github.com/apache/skywalking-go/protocols/collect/language/profile/v3"
	logv3 "github.com/apache/skywalking-go/protocols/collect/logging/v3"
	managementv3 "github.com/apache/skywalking-go/protocols/collect/management/v3"
)

const (
	maxSendQueueSize         int32 = 30000
	sendPipelineDrainWait          = 15 * time.Second
	profileSendTimeout             = 3 * time.Second
	maxAbandonedProfileSends       = 2
	// Bound open/retry loops in uploadProfileResults so a down backend cannot
	// wedge the profile send goroutine (and thus Close) indefinitely.
	maxShutdownStreamOpenAttempts = 30
	maxShutdownProfileSendRetries = 30
)

// errProfileStreamAbandoned means Send is still in-flight on this stream; the
// caller must not CloseAndRecv/Send further on it. At most
// maxAbandonedProfileSends such streams may be abandoned concurrently; when the
// soft-cap is reached we cancel and wait for Send instead of leaking another
// waiter/stream.
var errProfileStreamAbandoned = errors.New("profile stream abandoned with in-flight send")

// errProfileSendPanic means stream.Send panicked, so the chunk was not delivered.
var errProfileSendPanic = errors.New("profile stream send panicked")

// maxProfileChunkRetries bounds how many streams one chunk may be retried on, so
// a chunk the backend keeps rejecting cannot wedge the profile pipeline.
const maxProfileChunkRetries = 3

// profileStream owns a GoProfileReport client stream and its cancel func.
// The cancel must outlive openProfileStream — it governs the whole RPC.
type profileStream struct {
	client profilev3.ProfileTask_GoProfileReportClient
	cancel context.CancelFunc
}

func (s *profileStream) Send(data *profilev3.GoProfileData) error {
	if s == nil || s.client == nil {
		return errors.New("nil profile stream")
	}
	return s.client.Send(data)
}

// NewGRPCReporter create a new reporter to send data to gRPC oap server. Only one backend address is allowed.
func NewGRPCReporter(logger operator.LogOperator,
	serverAddr string,
	checkInterval time.Duration,
	profileFetchInterval time.Duration,
	connManager *reporter.ConnectionManager,
	cdsManager *reporter.CDSManager,
	pprofTaskManager *reporter.PprofTaskManager,
	opts ...ReporterOption,
) (reporter.Reporter, error) {
	r := &gRPCReporter{
		logger:               logger,
		serverAddr:           serverAddr,
		tracingSendCh:        make(chan *agentv3.SegmentObject, maxSendQueueSize),
		metricsSendCh:        make(chan []*agentv3.MeterData, maxSendQueueSize),
		logSendCh:            make(chan *logv3.LogData, maxSendQueueSize),
		checkInterval:        checkInterval,
		profileFetchInterval: profileFetchInterval,
		connManager:          connManager,
		cdsManager:           cdsManager,
		pprofTaskManager:     pprofTaskManager,
	}
	for _, o := range opts {
		o(r)
	}
	r.lastProfileCommandTime = -1
	conn, err := connManager.GetConnection(serverAddr)
	if err != nil {
		return nil, err
	}
	r.traceClient = agentv3.NewTraceSegmentReportServiceClient(conn)
	r.metricsClient = agentv3.NewMeterReportServiceClient(conn)
	r.logClient = logv3.NewLogReportServiceClient(conn)
	r.managementClient = managementv3.NewManagementServiceClient(conn)
	r.profileTaskClient = profilev3.NewProfileTaskClient(conn)
	return r, nil
}

type gRPCReporter struct {
	entity               *reporter.Entity
	serverAddr           string
	logger               operator.LogOperator
	tracingSendCh        chan *agentv3.SegmentObject
	metricsSendCh        chan []*agentv3.MeterData
	logSendCh            chan *logv3.LogData
	traceClient          agentv3.TraceSegmentReportServiceClient
	metricsClient        agentv3.MeterReportServiceClient
	logClient            logv3.LogReportServiceClient
	managementClient     managementv3.ManagementServiceClient
	profileTaskClient    profilev3.ProfileTaskClient
	profileTaskManager   reporter.ProfileTaskManager
	checkInterval        time.Duration
	profileFetchInterval time.Duration
	// lastProfileCommandTime is the last timestamp we used to fetch profile commands.
	lastProfileCommandTime int64
	// bootFlag is set if Boot be executed
	bootFlag         bool
	transform        *reporter.Transform
	connManager      *reporter.ConnectionManager
	cdsManager       *reporter.CDSManager
	pprofTaskManager *reporter.PprofTaskManager

	closeOnce      sync.Once
	sendPipelineWG sync.WaitGroup

	// abandonedProfileSends caps in-flight Sends left on abandoned streams so a
	// stuck backend cannot accumulate unbounded goroutines/streams.
	abandonedProfileMu    sync.Mutex
	abandonedProfileSends int
}

func (r *gRPCReporter) Boot(entity *reporter.Entity, cdsWatchers []reporter.AgentConfigChangeWatcher) {
	r.entity = entity
	r.transform = reporter.NewTransform(entity)
	r.initSendPipeline()
	r.check()
	r.fetchProfileTasks()
	r.cdsManager.InitCDS(entity, cdsWatchers)
	r.pprofTaskManager.InitPprofTask(entity)
	r.bootFlag = true
}

func (r *gRPCReporter) ConnectionStatus() reporter.ConnectionStatus {
	return r.connManager.GetConnectionStatus(r.serverAddr)
}

func (r *gRPCReporter) SendTracing(spans []reporter.ReportedSpan) {
	// The recover must be registered BEFORE the transform call: SendTracing
	// runs on the segment collector goroutine, so a panic escaping from the
	// transform (or the channel send below, e.g. on a closed tracingSendCh)
	// would otherwise kill the whole process.
	defer func() {
		if err := recover(); err != nil {
			r.logger.Errorf("reporter segment err %v", err)
		}
	}()
	segmentObject := r.transform.TransformSegmentObject(spans)
	if segmentObject == nil {
		return
	}
	select {
	case r.tracingSendCh <- segmentObject:
	default:
		r.logger.Errorf("reach max tracing send buffer")
	}
}

func (r *gRPCReporter) SendMetrics(metrics []reporter.ReportedMeter) {
	meters := r.transform.TransformMeterData(metrics)
	if meters == nil {
		return
	}
	defer func() {
		// recover the panic caused by close metricsSendCh
		if err := recover(); err != nil {
			r.logger.Errorf("reporter metrics err %v", err)
		}
	}()
	select {
	case r.metricsSendCh <- meters:
	default:
		r.logger.Errorf("reach max metrics send buffer")
	}
}

func (r *gRPCReporter) SendLog(log *logv3.LogData) {
	defer func() {
		if err := recover(); err != nil {
			r.logger.Errorf("reporter log err %v", err)
		}
	}()
	select {
	case r.logSendCh <- log:
	default:
	}
}

func (r *gRPCReporter) Close() {
	r.closeOnce.Do(func() {
		// Wake disconnect/retry sleeps first so pipelines can leave Wait promptly,
		// while ClientConns stay open for the drain below.
		if r.connManager != nil {
			r.connManager.SignalShutdown()
		}
		if r.bootFlag {
			// Stop the profile producer and close its results channel so the
			// profile send pipeline can drain buffered results then exit.
			// Close is optional for source compatibility with external
			// ProfileTaskManager implementations that predate the method.
			if c, ok := r.profileTaskManager.(reporter.ProfileTaskManagerCloser); ok {
				c.Close()
			}
			if r.tracingSendCh != nil {
				close(r.tracingSendCh)
			}
			if r.metricsSendCh != nil {
				close(r.metricsSendCh)
			}
			if r.logSendCh != nil {
				close(r.logSendCh)
			}
			if r.pprofTaskManager != nil {
				r.pprofTaskManager.Close()
			}
			done := make(chan struct{})
			go func() {
				r.sendPipelineWG.Wait()
				if r.pprofTaskManager != nil {
					r.pprofTaskManager.WaitSendPipeline()
				}
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(sendPipelineDrainWait):
				if r.logger != nil {
					r.logger.Errorf("gRPCReporter Close: send pipelines did not finish within %s", sendPipelineDrainWait)
				}
			}
		}
		r.closeGRPCConn()
	})
}

// closeGRPCConn force-closes the shared connection because other components
// (CDS, pprof) hold their own references; any remaining reference would keep
// the ClientConn - and the periodic DNS resolver it owns - alive.
func (r *gRPCReporter) closeGRPCConn() {
	if r.connManager != nil {
		r.connManager.Close()
	}
}

// sendWithRecover invokes send and recovers from a panic raised while encoding or
// transmitting a single message, so that one corrupted payload cannot tear down the
// whole send pipeline. On a recovered panic it logs via the existing logger and
// returns recovered=true, telling the caller to skip the current message and keep
// streaming the rest.
//
// Such a panic originates in protobuf size/marshal computation (the #13885 crash),
// which runs before any bytes are written to the stream, so the stream stays valid
// and may be reused for the next message. Should a panic ever leave the stream
// inconsistent, the following send returns an error and the caller reconnects.
func (r *gRPCReporter) sendWithRecover(send func() error) (recovered bool, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			r.logger.Errorf("gRPCReporter recovered from panic while sending, skip current message: %v", rec)
			recovered = true
		}
	}()
	err = send()
	return recovered, err
}

// nolint
func (r *gRPCReporter) initSendPipeline() {
	if r.traceClient == nil {
		return
	}
	r.sendPipelineWG.Add(4)
	go func() {
		defer r.sendPipelineWG.Done()
		defer func() {
			if err := recover(); err != nil {
				r.logger.Errorf("gRPCReporter initSendPipeline trace client Collect panic err %v", err)
			}
		}()
	StreamLoop:
		for {
			switch r.connManager.GetConnectionStatus(r.serverAddr) {
			case reporter.ConnectionStatusShutdown:
				break StreamLoop
			case reporter.ConnectionStatusDisconnect:
				if r.connManager.Wait(5 * time.Second) {
					continue StreamLoop
				}
				// Shutdown signaled while disconnected: try one flush pass below.
			}

			stream, err := r.traceClient.Collect(metadata.NewOutgoingContext(context.Background(), r.connManager.GetMD()))
			if err != nil {
				r.logger.Errorf("open stream error %v", err)
				if !r.connManager.Wait(5 * time.Second) {
					break StreamLoop
				}
				continue StreamLoop
			}
			for s := range r.tracingSendCh {
				recovered, sendErr := r.sendWithRecover(func() error { return stream.Send(s) })
				if recovered {
					continue
				}
				if sendErr != nil {
					r.logger.Errorf("send segment error %v", sendErr)
					r.closeTracingStream(stream)
					continue StreamLoop
				}
			}
			r.closeTracingStream(stream)
			break StreamLoop
		}
	}()
	go func() {
		defer r.sendPipelineWG.Done()
		defer func() {
			if err := recover(); err != nil {
				r.logger.Errorf("gRPCReporter initSendPipeline metrics client CollectBatch panic err %v", err)
			}
		}()
	StreamLoop:
		for {
			switch r.connManager.GetConnectionStatus(r.serverAddr) {
			case reporter.ConnectionStatusShutdown:
				break StreamLoop
			case reporter.ConnectionStatusDisconnect:
				if r.connManager.Wait(5 * time.Second) {
					continue StreamLoop
				}
			}

			stream, err := r.metricsClient.CollectBatch(metadata.NewOutgoingContext(context.Background(), r.connManager.GetMD()))
			if err != nil {
				r.logger.Errorf("open stream error %v", err)
				if !r.connManager.Wait(5 * time.Second) {
					break StreamLoop
				}
				continue StreamLoop
			}
			for s := range r.metricsSendCh {
				recovered, sendErr := r.sendWithRecover(func() error {
					return stream.Send(&agentv3.MeterDataCollection{MeterData: s})
				})
				if recovered {
					continue
				}
				if sendErr != nil {
					r.logger.Errorf("send metrics error %v", sendErr)
					r.closeMetricsStream(stream)
					continue StreamLoop
				}
			}
			r.closeMetricsStream(stream)
			break StreamLoop
		}
	}()
	go func() {
		defer r.sendPipelineWG.Done()
		defer func() {
			if err := recover(); err != nil {
				r.logger.Errorf("gRPCReporter initSendPipeline log client Collect panic err %v", err)
			}
		}()
	StreamLoop:
		for {
			switch r.connManager.GetConnectionStatus(r.serverAddr) {
			case reporter.ConnectionStatusShutdown:
				break StreamLoop
			case reporter.ConnectionStatusDisconnect:
				if r.connManager.Wait(5 * time.Second) {
					continue StreamLoop
				}
			}

			stream, err := r.logClient.Collect(metadata.NewOutgoingContext(context.Background(), r.connManager.GetMD()))
			if err != nil {
				r.logger.Errorf("open stream error %v", err)
				if !r.connManager.Wait(5 * time.Second) {
					break StreamLoop
				}
				continue StreamLoop
			}
			for s := range r.logSendCh {
				recovered, sendErr := r.sendWithRecover(func() error { return stream.Send(s) })
				if recovered {
					continue
				}
				if sendErr != nil {
					r.logger.Errorf("send log error %v", sendErr)
					r.closeLogStream(stream)
					continue StreamLoop
				}
			}
			r.closeLogStream(stream)
			break StreamLoop
		}
	}()
	go func() {
		defer r.sendPipelineWG.Done()
		if r.profileTaskManager == nil || r.profileTaskClient == nil {
			return
		}
		for {
			panicked := false
			func() {
				defer func() {
					if err := recover(); err != nil {
						r.logger.Errorf("gRPCReporter reportProfileResult panic err %v", err)
						panicked = true
					}
				}()
				r.runProfileSendLoop()
			}()
			if !panicked {
				return
			}
			// Keep a drain path alive after panic so ProfileManager.Close cannot
			// block forever in flushOverflowBlocking.
			select {
			case <-r.connManager.ShutdownNotify():
				r.flushProfileResultsBestEffort()
				return
			case <-time.After(time.Second):
			}
		}
	}()
}

func (r *gRPCReporter) runProfileSendLoop() {
	shutdownCh := r.connManager.ShutdownNotify()
	var pendingSend *reporter.ProfileResult
	pendingRetries := 0

	for {
		if r.shouldFlushProfileAndExit(shutdownCh, pendingSend) {
			return
		}

		ps, err := r.openProfileStream()
		if err != nil {
			r.logger.Errorf("open profile stream error %v", err)
			if !r.connManager.Wait(5 * time.Second) {
				r.flushPendingProfile(pendingSend)
				return
			}
			continue
		}
		re := r.profileTaskManager.GetProfileResults()

		for {
			task, ok, cont := r.nextProfileTask(shutdownCh, ps, re, &pendingSend)
			if !ok {
				return
			}
			if cont {
				continue
			}

			profileData := &profilev3.GoProfileData{
				TaskId:  task.TaskID,
				Payload: task.Payload,
				IsLast:  task.IsLast,
			}
			r.logger.Infof("Sending profile task: TaskID='%s', PayloadSize=%d, IsLast=%v",
				task.TaskID, len(task.Payload), task.IsLast)
			sendErr := r.sendProfileDataWithTimeout(ps, profileData)
			if sendErr != nil {
				if r.handleProfileSendFailure(shutdownCh, ps, sendErr, task, &pendingSend, &pendingRetries) {
					return
				}
				break
			}
			pendingRetries = 0
			if task.IsLast {
				r.finishProfileTask(task.TaskID)
			}
		}
	}
}

func (r *gRPCReporter) flushPendingProfile(pending *reporter.ProfileResult) {
	if pending != nil {
		r.flushProfileResultsBestEffort(*pending)
		return
	}
	r.flushProfileResultsBestEffort()
}

func (r *gRPCReporter) shouldFlushProfileAndExit(shutdownCh <-chan struct{}, pending *reporter.ProfileResult) bool {
	select {
	case <-shutdownCh:
		r.flushPendingProfile(pending)
		return true
	default:
	}
	switch r.connManager.GetConnectionStatus(r.serverAddr) {
	case reporter.ConnectionStatusShutdown:
		r.flushPendingProfile(pending)
		return true
	case reporter.ConnectionStatusDisconnect:
		if r.connManager.Wait(5 * time.Second) {
			return false
		}
		r.flushPendingProfile(pending)
		return true
	}
	return false
}

// nextProfileTask returns the next chunk to send.
// ok=false means the loop should exit; cont=true means retry the select.
func (r *gRPCReporter) nextProfileTask(
	shutdownCh <-chan struct{},
	ps *profileStream,
	re <-chan reporter.ProfileResult,
	pendingSend **reporter.ProfileResult,
) (task reporter.ProfileResult, ok, cont bool) {
	if *pendingSend != nil {
		task = **pendingSend
		*pendingSend = nil
		return task, true, false
	}
	select {
	case <-shutdownCh:
		r.flushProfileResultsBestEffort()
		if ps != nil {
			r.closeProfileStream(ps)
		}
		return task, false, false
	case got, open := <-re:
		if !open {
			if ps != nil {
				r.closeProfileStream(ps)
			}
			return task, false, false
		}
		return got, true, false
	case <-time.After(time.Second):
		if r.connManager.GetConnectionStatus(r.serverAddr) == reporter.ConnectionStatusShutdown {
			r.flushProfileResultsBestEffort()
			if ps != nil {
				r.closeProfileStream(ps)
			}
			return task, false, false
		}
		return task, true, true
	}
}

// handleProfileSendFailure closes or abandons the stream and requeues the chunk.
// Returns true when the send loop should exit (shutdown).
func (r *gRPCReporter) handleProfileSendFailure(
	shutdownCh <-chan struct{},
	ps *profileStream,
	sendErr error,
	task reporter.ProfileResult,
	pendingSend **reporter.ProfileResult,
	pendingRetries *int,
) bool {
	r.logger.Errorf("send profile data error %v", sendErr)
	if !errors.Is(sendErr, errProfileStreamAbandoned) {
		r.closeProfileStream(ps)
	}
	select {
	case <-shutdownCh:
		r.flushProfileResultsBestEffort(task)
		return true
	default:
	}
	*pendingRetries++
	if *pendingRetries > maxProfileChunkRetries {
		r.logger.Errorf("stream retries exhausted for profile task %s after %d attempts; uploading via flush path",
			task.TaskID, *pendingRetries-1)
		*pendingRetries = 0
		// Do not drop: try a dedicated upload so IsLast cannot vanish while the
		// results channel is still open.
		r.uploadProfileResults([]reporter.ProfileResult{task})
		return false
	}
	cp := task
	*pendingSend = &cp
	return false
}

// openProfileStream opens a profile upload stream. The returned cancel must be
// invoked from closeProfileStream (or after abandon + Conn teardown) — it must
// NOT be deferred in this function or the stream dies immediately.
func (r *gRPCReporter) openProfileStream() (*profileStream, error) {
	ctx, cancel := context.WithCancel(context.Background())
	type openRes struct {
		client profilev3.ProfileTask_GoProfileReportClient
		err    error
	}
	ch := make(chan openRes, 1)
	go func() {
		client, err := r.profileTaskClient.GoProfileReport(metadata.NewOutgoingContext(ctx, r.connManager.GetMD()))
		ch <- openRes{client: client, err: err}
	}()
	select {
	case res := <-ch:
		if res.err != nil {
			cancel()
			return nil, res.err
		}
		return &profileStream{client: res.client, cancel: cancel}, nil
	case <-time.After(profileSendTimeout):
		cancel()
		go func() {
			res := <-ch
			if res.client != nil {
				_, _ = res.client.CloseAndRecv()
			}
		}()
		return nil, context.DeadlineExceeded
	}
}

// sendProfileDataWithTimeout bounds how long we wait for stream.Send so a stuck
// backend cannot block the receive loop forever. If the wait expires while Send
// is still running, the stream is abandoned (do not CloseAndRecv) to avoid
// racing Send. When the abandon soft-cap is reached, cancel and wait for Send
// instead of leaking another waiter. If ShutdownNotify and done are both ready,
// prefer the completed Send result.
func (r *gRPCReporter) sendProfileDataWithTimeout(ps *profileStream, data *profilev3.GoProfileData) error {
	if ps == nil {
		return errors.New("nil profile stream")
	}
	done := make(chan error, 1)
	go func() {
		recovered, sendErr := r.sendWithRecover(func() error { return ps.Send(data) })
		if recovered {
			// A panicking Send delivered nothing: report failure so the caller
			// reopens the stream and requeues instead of losing the chunk.
			done <- errProfileSendPanic
			return
		}
		done <- sendErr
	}()
	select {
	case err := <-done:
		return err
	case <-r.connManager.ShutdownNotify():
		// Prefer a completed Send when both are ready (select is random otherwise).
		select {
		case err := <-done:
			return err
		default:
			return r.finishOrAbandonProfileSend(done, ps.cancel)
		}
	case <-time.After(profileSendTimeout):
		select {
		case err := <-done:
			return err
		default:
			return r.finishOrAbandonProfileSend(done, ps.cancel)
		}
	}
}

func (r *gRPCReporter) finishOrAbandonProfileSend(done <-chan error, cancel context.CancelFunc) error {
	r.abandonedProfileMu.Lock()
	canAbandon := r.abandonedProfileSends < maxAbandonedProfileSends
	if canAbandon {
		r.abandonedProfileSends++
	}
	r.abandonedProfileMu.Unlock()

	if !canAbandon {
		// Soft-cap reached: cancel to unblock Send and wait. Do not spawn another
		// abandon waiter — that would leak goroutines/streams unboundedly.
		if cancel != nil {
			cancel()
		}
		return <-done
	}

	// Cancel promptly so gRPC can finish Send and free the soft-cap slot.
	if cancel != nil {
		cancel()
	}
	go func() {
		<-done
		r.abandonedProfileMu.Lock()
		if r.abandonedProfileSends > 0 {
			r.abandonedProfileSends--
		}
		r.abandonedProfileMu.Unlock()
	}()
	return errProfileStreamAbandoned
}

func (r *gRPCReporter) finishProfileTask(taskID string) {
	if r.profileTaskManager != nil {
		r.profileTaskManager.ProfileFinish()
	}
	if r.profileTaskClient == nil || r.entity == nil {
		return
	}
	report := profilev3.ProfileTaskFinishReport{
		TaskId:          taskID,
		Service:         r.entity.ServiceName,
		ServiceInstance: r.entity.ServiceInstanceName,
	}
	ctx, cancel := context.WithTimeout(context.Background(), profileSendTimeout)
	defer cancel()
	_, err := r.profileTaskClient.ReportTaskFinish(metadata.NewOutgoingContext(ctx, r.connManager.GetMD()), &report)
	if err != nil {
		r.logger.Errorf("report profile task finish error %v", err)
	}
}

// flushProfileResultsBestEffort uploads remaining profile chunks on shutdown.
// Failed sends are kept and retried after the producer channel is drained so
// ProfileManager.Close (flushOverflowBlocking) cannot deadlock on a full buffer.
// Results are only abandoned after shutdown retries are exhausted.
// extra holds chunks already dequeued (e.g. abandoned in-flight Send) so they
// are not lost before the channel drain.
func (r *gRPCReporter) flushProfileResultsBestEffort(extra ...reporter.ProfileResult) {
	if r.profileTaskManager == nil || r.profileTaskClient == nil {
		return
	}
	re := r.profileTaskManager.GetProfileResults()
	if re == nil {
		if len(extra) > 0 {
			r.uploadProfileResults(append([]reporter.ProfileResult(nil), extra...))
		}
		return
	}

	pending := append([]reporter.ProfileResult(nil), extra...)
	for task := range re {
		pending = append(pending, task)
	}
	r.uploadProfileResults(pending)
}

func (r *gRPCReporter) uploadProfileResults(tasks []reporter.ProfileResult) {
	if len(tasks) == 0 {
		return
	}
	var ps *profileStream
	var abandoned bool
	defer func() {
		if ps != nil && !abandoned {
			r.closeProfileStream(ps)
		}
	}()

	sendRetries := 0
	for len(tasks) > 0 {
		if ps == nil || abandoned {
			if !r.ensureShutdownProfileStream(&ps, &abandoned) {
				r.logger.Errorf("shutdown profile flush: abandoning %d results (no stream)", len(tasks))
				return
			}
		}
		task := tasks[0]
		profileData := &profilev3.GoProfileData{
			TaskId:  task.TaskID,
			Payload: task.Payload,
			IsLast:  task.IsLast,
		}
		sendErr := r.sendProfileDataWithTimeout(ps, profileData)
		if sendErr != nil {
			r.noteShutdownProfileSendFailure(&ps, &abandoned, sendErr)
			sendRetries++
			if sendRetries > maxShutdownProfileSendRetries {
				r.logger.Errorf("shutdown profile flush: abandoning %d results after %d send retries",
					len(tasks), sendRetries)
				return
			}
			if r.connManager.Wait(100 * time.Millisecond) {
				// Not fully shut down yet — reopen and retry the same chunk.
				continue
			}
			if ps == nil && !r.openShutdownProfileStream(&ps, &abandoned) {
				r.logger.Errorf("shutdown profile flush: abandoning %d results after send errors", len(tasks))
				return
			}
			if r.sendProfileDataWithTimeout(ps, profileData) != nil {
				r.logger.Errorf("shutdown profile flush: abandoning %d results: %v", len(tasks), sendErr)
				return
			}
		}
		sendRetries = 0
		if task.IsLast {
			r.finishProfileTask(task.TaskID)
		}
		tasks = tasks[1:]
	}
}

func (r *gRPCReporter) ensureShutdownProfileStream(ps **profileStream, abandoned *bool) bool {
	for attempt := 0; attempt < maxShutdownStreamOpenAttempts; attempt++ {
		if r.openShutdownProfileStream(ps, abandoned) {
			return true
		}
		if !r.connManager.Wait(100 * time.Millisecond) {
			return r.openShutdownProfileStream(ps, abandoned)
		}
	}
	return false
}

func (r *gRPCReporter) openShutdownProfileStream(ps **profileStream, abandoned *bool) bool {
	s, err := r.openProfileStream()
	if err != nil {
		r.logger.Errorf("open profile stream for shutdown flush error %v", err)
		return false
	}
	*ps = s
	*abandoned = false
	return true
}

func (r *gRPCReporter) noteShutdownProfileSendFailure(ps **profileStream, abandoned *bool, sendErr error) {
	r.logger.Errorf("shutdown profile flush send error %v", sendErr)
	if errors.Is(sendErr, errProfileStreamAbandoned) {
		*abandoned = true
		*ps = nil
		return
	}
	if *ps != nil {
		r.closeProfileStream(*ps)
		*ps = nil
	}
}

// drainRemainingProfileResults consumes any leftover profile results after the
// producer has closed the channel (or when shutdown prevents opening a stream).
func (r *gRPCReporter) drainRemainingProfileResults() {
	r.flushProfileResultsBestEffort()
}

func (r *gRPCReporter) closeTracingStream(stream agentv3.TraceSegmentReportService_CollectClient) {
	r.closeStreamWithTimeout("trace", func() error {
		_, err := stream.CloseAndRecv()
		return err
	}, nil)
}

func (r *gRPCReporter) closeMetricsStream(stream agentv3.MeterReportService_CollectBatchClient) {
	r.closeStreamWithTimeout("metrics", func() error {
		_, err := stream.CloseAndRecv()
		return err
	}, nil)
}

func (r *gRPCReporter) closeLogStream(stream logv3.LogReportService_CollectClient) {
	r.closeStreamWithTimeout("log", func() error {
		_, err := stream.CloseAndRecv()
		return err
	}, nil)
}

// closeStreamWithTimeout bounds CloseAndRecv so a stalled backend cannot consume
// the entire sendPipelineDrainWait budget on shutdown.
func (r *gRPCReporter) closeStreamWithTimeout(kind string, closeFn func() error, onTimeout func()) {
	done := make(chan struct{})
	var closeErr error
	go func() {
		defer close(done)
		closeErr = closeFn()
	}()
	select {
	case <-done:
		if closeErr != nil && closeErr != io.EOF {
			r.logger.Errorf("send %s closing error %v", kind, closeErr)
		}
	case <-time.After(profileSendTimeout):
		if onTimeout != nil {
			onTimeout()
		}
		select {
		case <-done:
			if closeErr != nil && closeErr != io.EOF {
				r.logger.Errorf("send %s closing error %v", kind, closeErr)
			}
		case <-time.After(profileSendTimeout):
			r.logger.Errorf("close %s stream timed out after %s", kind, profileSendTimeout)
		}
	}
}

func (r *gRPCReporter) closeProfileStream(ps *profileStream) {
	if ps == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if ps.client != nil {
			_, err := ps.client.CloseAndRecv()
			if err != nil && err != io.EOF {
				r.logger.Errorf("send profile closing error %v", err)
			}
		}
		if ps.cancel != nil {
			ps.cancel()
		}
	}()
	select {
	case <-done:
	case <-time.After(profileSendTimeout):
		// Cancel the stream context to unblock CloseAndRecv, then wait briefly.
		if ps.cancel != nil {
			ps.cancel()
		}
		select {
		case <-done:
		case <-time.After(profileSendTimeout):
			r.logger.Errorf("close profile stream timed out after %s", profileSendTimeout)
		}
	}
}
func (r *gRPCReporter) reportInstanceProperties() (err error) {
	_, err = r.managementClient.ReportInstanceProperties(
		metadata.NewOutgoingContext(context.Background(), r.connManager.GetMD()),
		&managementv3.InstanceProperties{
			Service:         r.entity.ServiceName,
			ServiceInstance: r.entity.ServiceInstanceName,
			Properties:      r.entity.Props,
		})
	return err
}

func (r *gRPCReporter) check() {
	if r.checkInterval < 0 || r.managementClient == nil {
		return
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				r.logger.Errorf("gRPCReporter check panic err %v", err)
			}
		}()
		instancePropertiesSubmitted := false
		for {
			switch r.connManager.GetConnectionStatus(r.serverAddr) {
			case reporter.ConnectionStatusShutdown:
				return
			case reporter.ConnectionStatusDisconnect:
				if !r.connManager.Wait(r.checkInterval) {
					return
				}
				continue
			}

			if !instancePropertiesSubmitted {
				err := r.reportInstanceProperties()
				if err != nil {
					r.logger.Errorf("report serviceInstance properties error %v", err)
					if !r.connManager.Wait(r.checkInterval) {
						return
					}
					continue
				}
				instancePropertiesSubmitted = true
			}

			_, err := r.managementClient.KeepAlive(
				metadata.NewOutgoingContext(context.Background(), r.connManager.GetMD()),
				&managementv3.InstancePingPkg{
					Service:         r.entity.ServiceName,
					ServiceInstance: r.entity.ServiceInstanceName,
				})

			if err != nil {
				r.logger.Errorf("send keep alive signal error %v", err)
			}
			if !r.connManager.Wait(r.checkInterval) {
				return
			}
		}
	}()
}

func (r *gRPCReporter) fetchProfileTasks() {
	if r.profileFetchInterval < 0 {
		r.logger.Errorf("profile init error:profileFetchInterval is %v", r.profileFetchInterval)
		return
	}
	go func() {
		for {
			switch r.connManager.GetConnectionStatus(r.serverAddr) {
			case reporter.ConnectionStatusShutdown:
				return
			case reporter.ConnectionStatusDisconnect:
				if !r.connManager.Wait(r.profileFetchInterval) {
					return
				}
				continue
			}
			// The recover wraps a single iteration: this long-lived goroutine
			// has no other protection and a panic while handling the profile
			// commands would otherwise kill the whole process.
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						r.logger.Errorf("gRPCReporter recovered from panic while fetching profile tasks: %v", rec)
					}
				}()
				r.fetchProfileTasksOnce()
			}()
			if !r.connManager.Wait(r.profileFetchInterval) {
				return
			}
		}
	}()
}

// fetchProfileTasksOnce pulls and handles the pending profile task commands of
// one polling round.
func (r *gRPCReporter) fetchProfileTasksOnce() {
	// Construct the request
	req := &profilev3.ProfileTaskCommandQuery{
		Service:         r.entity.ServiceName,
		ServiceInstance: r.entity.ServiceInstanceName,
		LastCommandTime: r.lastProfileCommandTime,
	}

	// Pull tasks
	resp, err := r.profileTaskClient.GetProfileTaskCommands(context.Background(), req)
	if err != nil {
		r.logger.Errorf("fetch profile task error: %v", err)
		return
	}

	// Handle all returned commands
	for _, cmd := range resp.Commands {
		nt := r.handleProfileTask(cmd, r.lastProfileCommandTime)
		if nt > r.lastProfileCommandTime {
			r.lastProfileCommandTime = nt
		}
	}

	// Remove completed tasks
	r.profileTaskManager.RemoveProfileTask()
}

func (r *gRPCReporter) AddProfileTaskManager(p reporter.ProfileTaskManager) {
	r.profileTaskManager = p
}

func (r *gRPCReporter) handleProfileTask(cmd *common.Command, t int64) int64 {
	if cmd.Command != "ProfileTaskQuery" {
		return t
	}
	nt := r.profileTaskManager.AddProfileTask(cmd.Args, t)
	return nt
}
