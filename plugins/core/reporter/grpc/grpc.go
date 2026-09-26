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
	"io"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-go/plugins/core/operator"
	"github.com/apache/skywalking-go/plugins/core/reporter"
	common "github.com/apache/skywalking-go/protocols/collect/common/v3"
	agentv3 "github.com/apache/skywalking-go/protocols/collect/language/agent/v3"
	profilev3 "github.com/apache/skywalking-go/protocols/collect/language/profile/v3"
	logv3 "github.com/apache/skywalking-go/protocols/collect/logging/v3"
	managementv3 "github.com/apache/skywalking-go/protocols/collect/management/v3"
)

const (
	maxSendQueueSize int32 = 30000
)

// NewGRPCReporter create a new reporter to send data to gRPC oap server.
// backend_service may be a single host:port or a comma-separated list; multiple
// addresses are published to gRPC (pick_first) via a static multi-backend resolver.
func NewGRPCReporter(logger operator.LogOperator,
	serverAddr string,
	checkInterval time.Duration,
	profileFetchInterval time.Duration,
	connManager *reporter.ConnectionManager,
	cdsManager *reporter.CDSManager,
	pprofTaskManager *reporter.PprofTaskManager,
	opts ...ReporterOption,
) (reporter.Reporter, error) {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	r := &gRPCReporter{
		shutdownCtx:          shutdownCtx,
		shutdownCancel:       shutdownCancel,
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
		shutdownCancel()
		return nil, err
	}
	r.serviceClients.Store(newGrpcServiceClients(conn))
	return r, nil
}

// grpcServiceClients is an immutable snapshot of generated stubs for one ClientConn.
// Readers load the atomic pointer and use the local copy so bind/recreate cannot
// race with in-flight RPCs (Codex P2).
type grpcServiceClients struct {
	conn       *grpc.ClientConn
	trace      agentv3.TraceSegmentReportServiceClient
	metrics    agentv3.MeterReportServiceClient
	log        logv3.LogReportServiceClient
	management managementv3.ManagementServiceClient
	profile    profilev3.ProfileTaskClient
}

func newGrpcServiceClients(conn *grpc.ClientConn) *grpcServiceClients {
	return &grpcServiceClients{
		conn:       conn,
		trace:      agentv3.NewTraceSegmentReportServiceClient(conn),
		metrics:    agentv3.NewMeterReportServiceClient(conn),
		log:        logv3.NewLogReportServiceClient(conn),
		management: managementv3.NewManagementServiceClient(conn),
		profile:    profilev3.NewProfileTaskClient(conn),
	}
}

type gRPCReporter struct {
	entity               *reporter.Entity
	serverAddr           string
	logger               operator.LogOperator
	tracingSendCh        chan *agentv3.SegmentObject
	metricsSendCh        chan []*agentv3.MeterData
	logSendCh            chan *logv3.LogData
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
	// serviceClients holds the latest stub bundle; published atomically.
	serviceClients atomic.Pointer[grpcServiceClients]
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

func (r *gRPCReporter) clients() *grpcServiceClients {
	return r.serviceClients.Load()
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
	if r.connManager.IsMultiBackend() && r.shutdownCancel != nil {
		r.shutdownCancel()
	}
	if r.bootFlag {
		if r.tracingSendCh != nil {
			close(r.tracingSendCh)
		}
		if r.metricsSendCh != nil {
			close(r.metricsSendCh)
		}
		if r.connManager.IsMultiBackend() && r.logSendCh != nil {
			close(r.logSendCh)
		}
	} else {
		r.closeGRPCConn()
	}
}

func (r *gRPCReporter) closeGRPCConn() {
	if err := r.connManager.ReleaseConnection(r.serverAddr); err != nil {
		r.logger.Error(err)
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

// pipelineSend wraps stream Send. Multi-backend paths bound Send duration so a
// half-open connection after docker kill cannot stall the Collect loop forever.
func (r *gRPCReporter) pipelineSend(cancel context.CancelFunc, send func() error) (recovered bool, err error) {
	return r.sendWithRecover(func() error {
		if r.connManager.IsMultiBackend() {
			return reporter.MultiBackendSend(cancel, send, 0)
		}
		return send()
	})
}

func (r *gRPCReporter) kickMultiBackendConnect() {
	if !r.connManager.IsMultiBackend() {
		return
	}
	if conn := r.connManager.PeekConnection(r.serverAddr); conn != nil {
		state := conn.GetState()
		if state == connectivity.Idle || state == connectivity.TransientFailure {
			conn.Connect()
		}
	}
}

// reportMultiBackendError leaves transport recovery to gRPC. An RPC failure
// does not mean the active backend is unreachable, and closing the shared
// channel would also interrupt healthy telemetry streams.
func (r *gRPCReporter) reportMultiBackendError(operation string, err error) {
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		// The connection interceptor already emits a throttled diagnostic.
		return
	default:
		r.logger.Errorf("%s: %v", operation, err)
	}
	r.kickMultiBackendConnect()
}

// Wait before taking more telemetry out of the queue during a transport outage.
// RPC errors on a Ready channel do not enter this wait or change backend order.
func (r *gRPCReporter) waitForMultiBackendReady() bool {
	ctx := r.shutdownCtx
	if ctx == nil {
		ctx = context.Background()
	}
	conn := r.connManager.PeekConnection(r.serverAddr)
	if conn == nil {
		return false
	}
	for ctx.Err() == nil {
		state := conn.GetState()
		switch state {
		case connectivity.Ready:
			return true
		case connectivity.Shutdown:
			return false
		case connectivity.Idle:
			conn.Connect()
		default:
		}
		if !conn.WaitForStateChange(ctx, state) {
			return false
		}
	}
	return false
}

// multiBackendTraceSendLoop sends a snapshot of the queue on each Collect
// stream. Batching amortizes acknowledgements without replaying a failed batch.
func (r *gRPCReporter) multiBackendTraceSendLoop() {
	defer r.closeGRPCConn()
	for r.waitForMultiBackendReady() {
		s, ok := <-r.tracingSendCh
		if !ok {
			return
		}
		if !r.waitForMultiBackendReady() {
			return
		}
		queued := len(r.tracingSendCh)
		segments := make([]*agentv3.SegmentObject, 1, queued+1)
		segments[0] = s
		for i := 0; i < queued; i++ {
			segments = append(segments, <-r.tracingSendCh)
		}
		if err := r.sendTraceSegmentsMulti(segments); err != nil {
			// A failed acknowledgement may follow successful ingestion. Do not
			// replay this segment: OAP does not deduplicate Collect requests.
			r.reportMultiBackendError("send segment error", err)
		}
	}
}

func (r *gRPCReporter) sendTraceSegmentsMulti(segments []*agentv3.SegmentObject) error {
	c := r.clients()
	if c == nil {
		return io.ErrUnexpectedEOF
	}
	ctx, cancel, stopOpen := reporter.BackendStreamContext(r.serverAddr, r.checkInterval)
	defer cancel()
	stream, err := c.trace.Collect(metadata.NewOutgoingContext(ctx, r.connManager.GetMD()))
	if timedOut := stopOpen(); err != nil || timedOut {
		if err == nil {
			err = ctx.Err()
		}
		return err
	}
	for _, segment := range segments {
		recovered, sendErr := r.pipelineSend(cancel, func() error { return stream.Send(segment) })
		if recovered {
			continue
		}
		if sendErr != nil {
			return sendErr
		}
	}
	// CloseAndRecv must not block forever on a half-open peer after docker kill.
	closeErr := reporter.MultiBackendSend(cancel, func() error {
		_, err := stream.CloseAndRecv()
		if err == io.EOF {
			return nil
		}
		return err
	}, 0)
	return closeErr
}

// nolint
func (r *gRPCReporter) initSendPipeline() {
	if r.clients() == nil {
		return
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				r.logger.Errorf("gRPCReporter initSendPipeline trace client Collect panic err %v", err)
			}
		}()
		if r.connManager.IsMultiBackend() {
			r.multiBackendTraceSendLoop()
			return
		}
	StreamLoop:
		for {
			if r.connManager.IsMultiBackend() && !r.waitForMultiBackendReady() {
				return
			}
			switch r.connManager.GetConnectionStatus(r.serverAddr) {
			case reporter.ConnectionStatusShutdown:
				return
			case reporter.ConnectionStatusDisconnect:
				time.Sleep(5 * time.Second)
				continue StreamLoop
			}

			c := r.clients()
			if c == nil {
				time.Sleep(5 * time.Second)
				continue StreamLoop
			}
			var (
				stream agentv3.TraceSegmentReportService_CollectClient
				err    error
			)
			stream, err = c.trace.Collect(metadata.NewOutgoingContext(context.Background(), r.connManager.GetMD()))
			if err != nil {
				r.logger.Errorf("open stream error %v", err)
				time.Sleep(5 * time.Second)
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
			r.closeGRPCConn()
			break
		}
	}()
	go func() {
		defer func() {
			if err := recover(); err != nil {
				r.logger.Errorf("gRPCReporter initSendPipeline metrics client CollectBatch panic err %v", err)
			}
		}()
	StreamLoop:
		for {
			if r.connManager.IsMultiBackend() && !r.waitForMultiBackendReady() {
				return
			}
			switch r.connManager.GetConnectionStatus(r.serverAddr) {
			case reporter.ConnectionStatusShutdown:
				return
			case reporter.ConnectionStatusDisconnect:
				time.Sleep(5 * time.Second)
				continue StreamLoop
			}

			c := r.clients()
			if c == nil {
				time.Sleep(5 * time.Second)
				continue StreamLoop
			}
			var (
				stream agentv3.MeterReportService_CollectBatchClient
				err    error
				cancel = func() {}
			)
			if r.connManager.IsMultiBackend() {
				var ctx context.Context
				var stopOpen func() bool
				ctx, cancel, stopOpen = reporter.BackendStreamContext(r.serverAddr, r.checkInterval)
				stream, err = c.metrics.CollectBatch(metadata.NewOutgoingContext(ctx, r.connManager.GetMD()))
				if timedOut := stopOpen(); err != nil || timedOut {
					cancel()
					if err == nil {
						err = ctx.Err()
					}
					r.reportMultiBackendError("open stream error", err)
					time.Sleep(5 * time.Second)
					continue StreamLoop
				}
				go reporter.WatchConnCancelOnUnready(ctx, cancel, c.conn)
			} else {
				stream, err = c.metrics.CollectBatch(metadata.NewOutgoingContext(context.Background(), r.connManager.GetMD()))
				if err != nil {
					r.logger.Errorf("open stream error %v", err)
					time.Sleep(5 * time.Second)
					continue StreamLoop
				}
			}
			for s := range r.metricsSendCh {
				recovered, sendErr := r.pipelineSend(cancel, func() error {
					return stream.Send(&agentv3.MeterDataCollection{MeterData: s})
				})
				if recovered {
					continue
				}
				if sendErr != nil {
					// Cancel before CloseAndRecv: on multi-backend a half-open peer
					// can hang CloseAndRecv forever and block reconnect.
					cancel()
					if r.connManager.IsMultiBackend() {
						r.reportMultiBackendError("send metrics error", sendErr)
					} else {
						r.logger.Errorf("send metrics error %v", sendErr)
						r.closeMetricsStream(stream)
					}
					continue StreamLoop
				}
			}
			cancel()
			if !r.connManager.IsMultiBackend() {
				r.closeMetricsStream(stream)
			}
			break
		}
	}()
	go func() {
		defer func() {
			if err := recover(); err != nil {
				r.logger.Errorf("gRPCReporter initSendPipeline log client Collect panic err %v", err)
			}
		}()
	StreamLoop:
		for {
			if r.connManager.IsMultiBackend() && !r.waitForMultiBackendReady() {
				return
			}
			switch r.connManager.GetConnectionStatus(r.serverAddr) {
			case reporter.ConnectionStatusShutdown:
				return
			case reporter.ConnectionStatusDisconnect:
				time.Sleep(5 * time.Second)
				continue StreamLoop
			}

			c := r.clients()
			if c == nil {
				time.Sleep(5 * time.Second)
				continue StreamLoop
			}
			var (
				stream logv3.LogReportService_CollectClient
				err    error
				cancel = func() {}
			)
			if r.connManager.IsMultiBackend() {
				var ctx context.Context
				var stopOpen func() bool
				ctx, cancel, stopOpen = reporter.BackendStreamContext(r.serverAddr, r.checkInterval)
				stream, err = c.log.Collect(metadata.NewOutgoingContext(ctx, r.connManager.GetMD()))
				if timedOut := stopOpen(); err != nil || timedOut {
					cancel()
					if err == nil {
						err = ctx.Err()
					}
					r.reportMultiBackendError("open stream error", err)
					time.Sleep(5 * time.Second)
					continue StreamLoop
				}
				go reporter.WatchConnCancelOnUnready(ctx, cancel, c.conn)
			} else {
				stream, err = c.log.Collect(metadata.NewOutgoingContext(context.Background(), r.connManager.GetMD()))
				if err != nil {
					r.logger.Errorf("open stream error %v", err)
					time.Sleep(5 * time.Second)
					continue StreamLoop
				}
			}
			for s := range r.logSendCh {
				recovered, sendErr := r.pipelineSend(cancel, func() error { return stream.Send(s) })
				if recovered {
					continue
				}
				if sendErr != nil {
					cancel()
					if r.connManager.IsMultiBackend() {
						r.reportMultiBackendError("send log error", sendErr)
					} else {
						r.logger.Errorf("send log error %v", sendErr)
						r.closeLogStream(stream)
					}
					continue StreamLoop
				}
			}
			cancel()
			if !r.connManager.IsMultiBackend() {
				r.closeLogStream(stream)
			}
			break
		}
	}()
	go func() {
		defer func() {
			if err := recover(); err != nil {
				r.logger.Errorf("gRPCReporter reportProfileResult panic err %v", err)
			}
		}()

	StreamLoop:
		for {
			if r.connManager.IsMultiBackend() && !r.waitForMultiBackendReady() {
				return
			}
			switch r.connManager.GetConnectionStatus(r.serverAddr) {
			case reporter.ConnectionStatusShutdown:
				return
			case reporter.ConnectionStatusDisconnect:
				time.Sleep(5 * time.Second)
				continue StreamLoop
			}

			c := r.clients()
			if c == nil {
				time.Sleep(5 * time.Second)
				continue StreamLoop
			}
			var (
				stream profilev3.ProfileTask_GoProfileReportClient
				err    error
				cancel = func() {}
			)
			if r.connManager.IsMultiBackend() {
				var ctx context.Context
				var stopOpen func() bool
				ctx, cancel, stopOpen = reporter.BackendStreamContext(r.serverAddr, r.checkInterval)
				stream, err = c.profile.GoProfileReport(metadata.NewOutgoingContext(ctx, r.connManager.GetMD()))
				if timedOut := stopOpen(); err != nil || timedOut {
					cancel()
					if err == nil {
						err = ctx.Err()
					}
					r.reportMultiBackendError("open profile stream error", err)
					time.Sleep(5 * time.Second)
					continue StreamLoop
				}
				go reporter.WatchConnCancelOnUnready(ctx, cancel, c.conn)
			} else {
				stream, err = c.profile.GoProfileReport(metadata.NewOutgoingContext(context.Background(), r.connManager.GetMD()))
				if err != nil {
					r.logger.Errorf("open profile stream error %v", err)
					time.Sleep(5 * time.Second)
					continue StreamLoop
				}
			}
			re := r.profileTaskManager.GetProfileResults()

			for task := range re {
				profileData := &profilev3.GoProfileData{
					TaskId:  task.TaskID,
					Payload: task.Payload,
					IsLast:  task.IsLast,
				}
				r.logger.Infof("Sending profile task: TaskID='%s', PayloadSize=%d, IsLast=%v",
					task.TaskID, len(task.Payload), task.IsLast)
				recovered, sendErr := r.pipelineSend(cancel, func() error { return stream.Send(profileData) })
				if recovered {
					continue
				}
				if sendErr != nil {
					cancel()
					if r.connManager.IsMultiBackend() {
						r.reportMultiBackendError("send profile data error", sendErr)
					} else {
						r.logger.Errorf("send profile data error %v", sendErr)
						r.closeProfileStream(stream)
					}
					continue StreamLoop
				}
				if task.IsLast {
					r.profileTaskManager.ProfileFinish()
					var report = profilev3.ProfileTaskFinishReport{
						TaskId:          task.TaskID,
						Service:         r.entity.ServiceName,
						ServiceInstance: r.entity.ServiceInstanceName,
					}
					pc := r.clients()
					if pc == nil {
						pc = c
					}
					if r.connManager.IsMultiBackend() {
						finishCtx, finishCancel := reporter.BackendRPCContext(r.serverAddr, r.checkInterval)
						_, err = pc.profile.ReportTaskFinish(
							metadata.NewOutgoingContext(finishCtx, r.connManager.GetMD()), &report)
						finishCancel()
					} else {
						_, err = pc.profile.ReportTaskFinish(metadata.NewOutgoingContext(context.Background(), r.connManager.GetMD()), &report)
					}
					if err != nil {
						r.logger.Errorf("report profile task finish error %v", err)
					}
				}
			}
			cancel()
			if !r.connManager.IsMultiBackend() {
				r.closeProfileStream(stream)
			}
			break
		}
	}()
}

func (r *gRPCReporter) closeTracingStream(stream agentv3.TraceSegmentReportService_CollectClient) {
	_, err := stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		r.logger.Errorf("send closing error %v", err)
	}
}

func (r *gRPCReporter) closeMetricsStream(stream agentv3.MeterReportService_CollectBatchClient) {
	_, err := stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		r.logger.Errorf("send closing error %v", err)
	}
}

func (r *gRPCReporter) closeLogStream(stream logv3.LogReportService_CollectClient) {
	_, err := stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		r.logger.Errorf("send closing error %v", err)
	}
}
func (r *gRPCReporter) closeProfileStream(stream profilev3.ProfileTask_GoProfileReportClient) {
	_, err := stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		r.logger.Errorf("send profile closing error %v", err)
	}
}
func (r *gRPCReporter) reportInstanceProperties() (err error) {
	c := r.clients()
	if c == nil {
		return io.ErrUnexpectedEOF
	}
	ctx := context.Background()
	cancel := func() {}
	if r.connManager.IsMultiBackend() {
		ctx, cancel = reporter.BackendRPCContext(r.serverAddr, r.checkInterval)
	}
	_, err = c.management.ReportInstanceProperties(
		metadata.NewOutgoingContext(ctx, r.connManager.GetMD()),
		&managementv3.InstanceProperties{
			Service:         r.entity.ServiceName,
			ServiceInstance: r.entity.ServiceInstanceName,
			Properties:      r.entity.Props,
		})
	cancel()
	return err
}

func (r *gRPCReporter) sendKeepAlive() error {
	c := r.clients()
	if c == nil {
		return io.ErrUnexpectedEOF
	}
	ctx := context.Background()
	cancel := func() {}
	if r.connManager.IsMultiBackend() {
		ctx, cancel = reporter.BackendRPCContext(r.serverAddr, r.checkInterval)
	}
	defer cancel()
	_, err := c.management.KeepAlive(
		metadata.NewOutgoingContext(ctx, r.connManager.GetMD()),
		&managementv3.InstancePingPkg{
			Service:         r.entity.ServiceName,
			ServiceInstance: r.entity.ServiceInstanceName,
		})
	return err
}

func (r *gRPCReporter) check() {
	if r.checkInterval < 0 || r.clients() == nil {
		return
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				r.logger.Errorf("gRPCReporter check panic err %v", err)
			}
		}()
		instancePropertiesSubmitted := false
		propertyRefreshHeartbeats := 0
		for {
			switch r.connManager.GetConnectionStatus(r.serverAddr) {
			case reporter.ConnectionStatusShutdown:
				return
			case reporter.ConnectionStatusDisconnect:
				if r.connManager.IsMultiBackend() {
					instancePropertiesSubmitted = false
				}
				time.Sleep(r.checkInterval)
				continue
			}

			if !instancePropertiesSubmitted || (r.connManager.IsMultiBackend() && propertyRefreshHeartbeats >= 10) {
				err := r.reportInstanceProperties()
				if err != nil {
					if r.connManager.IsMultiBackend() {
						r.reportMultiBackendError("report serviceInstance properties error", err)
					} else {
						r.logger.Errorf("report serviceInstance properties error %v", err)
						time.Sleep(r.checkInterval)
						continue
					}
				} else {
					instancePropertiesSubmitted = true
					propertyRefreshHeartbeats = 0
				}
			}

			if err := r.sendKeepAlive(); err != nil {
				if r.connManager.IsMultiBackend() {
					r.reportMultiBackendError("send keep alive signal error", err)
				} else {
					r.logger.Errorf("send keep alive signal error %v", err)
				}
			}
			propertyRefreshHeartbeats++
			time.Sleep(r.checkInterval)
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
			time.Sleep(r.profileFetchInterval)
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
	ctx := context.Background()
	cancel := func() {}
	if r.connManager.IsMultiBackend() {
		ctx, cancel = reporter.BackendRPCContext(r.serverAddr, r.profileFetchInterval)
	}
	c := r.clients()
	if c == nil {
		return
	}
	resp, err := c.profile.GetProfileTaskCommands(ctx, req)
	cancel()
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
