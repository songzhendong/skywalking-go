// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package core

import (
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-go/plugins/core/operator"
	"github.com/apache/skywalking-go/plugins/core/reporter"
	common "github.com/apache/skywalking-go/protocols/collect/common/v3"
)

type profileLabels struct {
	labels *LabelSet
}

const (
	maxSendQueueSize int32         = 8192
	timeOut          time.Duration = 2 * time.Minute
	ChunkSize                      = 1024 * 1024
	TraceLabel                     = "traceID"
	SegmentLabel                   = "traceSegmentID"
	MinDurationLabel               = "minDurationThreshold"
	SpanLabel                      = "spanID"
	// Align with gRPCReporter sendPipelineDrainWait so IsLast enqueue can wait
	// for shutdown flush before force-close.
	sendPipelineAlignedWait = 15 * time.Second
	// maxOverflowResults bounds results buffered outside FinalReportResults
	// before enqueueProfileResult switches to backpressure.
	maxOverflowResults = 1024
	// Bound Close-path drains so a stuck reporter consumer cannot hang forever.
	profileCloseFlushTimeout = 5 * time.Second
)

type currentTask struct {
	serialNumber         string // uuid
	taskID               string
	minDurationThreshold int64
	endpointName         string
	endTime              time.Time
	duration             int
}

type ProfileManager struct {
	mu                 sync.Mutex
	TraceProfileTask   *reporter.TraceProfileTask
	ProfileTaskQueue   []*reporter.TraceProfileTask
	rawCh              chan profileRawData
	FinalReportResults chan reporter.ProfileResult
	profilingWriter    *ProfilingWriter
	profileEvents      *TraceProfilingEventManager
	currentTask        *currentTask
	Log                operator.LogOperator
	closeOnce          sync.Once
	sendMu             sync.Mutex
	closed             atomic.Bool
	resultsClosed      bool // true after FinalReportResults is closed; under sendMu
	reportWG           sync.WaitGroup
	monitorWG          sync.WaitGroup
	profileFlushOnce   sync.Once
	monitorStop        chan struct{} // closed in Close to unblock monitor timer wait
	pendingTimers      []*time.Timer
	cpuProfileOwned    bool                     // true only if this manager started runtime/pprof CPU profiling
	overflow           []reporter.ProfileResult // when FinalReportResults is full
}

func (m *ProfileManager) initReportChannel() {
	// Original channel for receiving raw data chunks sent by the Writer
	rawCh := make(chan profileRawData, maxSendQueueSize)
	m.rawCh = rawCh
	// Start a goroutine to supplement each data chunk with business information
	m.reportWG.Add(1)
	go func() {
		defer m.reportWG.Done()
		for rawResult := range rawCh {
			// The recover wraps a single chunk: this goroutine has no other
			// protection and a panic here would kill the whole process. The
			// locked sections below use deferred unlocks so a panic can never
			// leak a held mutex into the next iteration.
			func() {
				defer func() {
					if err := recover(); err != nil {
						m.Log.Errorf("profile report panic: %v, stack: %s", err, debug.Stack())
					}
				}()
				// Get business information from currentTask
				var task *currentTask
				func() {
					m.mu.Lock()
					defer m.mu.Unlock()
					task = m.currentTask
				}()
				if task == nil {
					m.Log.Info("no task")
					return // Task has ended, ignore
				}

				result := reporter.ProfileResult{
					TaskID:  task.taskID,
					Payload: rawResult.data,
					IsLast:  rawResult.isLast,
				}
				m.enqueueProfileResult(result)
				if rawResult.isLast {
					func() {
						m.mu.Lock()
						defer m.mu.Unlock()
						if m.TraceProfileTask == nil {
							m.Log.Warn("no TraceProfileTask before finish profile")
						} else {
							m.TraceProfileTask.Status = reporter.Finished
						}
						m.currentTask = nil
					}()
					if err := m.profileEvents.UpdateBaseEventStatus(CurTaskExist, false); err != nil {
						m.Log.Errorf("update profile event error:%v", err)
					}
				}
			}()
		}
	}()
	// Pump overflow into FinalReportResults whenever space appears so consumers
	// are not stuck waiting while results sit only in overflow.
	go m.overflowPump()
}

func (m *ProfileManager) overflowPump() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		m.sendMu.Lock()
		if m.resultsClosed {
			m.sendMu.Unlock()
			return
		}
		m.drainOverflowLocked()
		m.sendMu.Unlock()
	}
}

// enqueueProfileResult buffers a result for the reporter. Once overflow reaches
// maxOverflowResults it applies backpressure (a bounded wait on the consumer)
// instead of growing the buffer, so a stalled reporter cannot balloon memory.
//
// Only the rawCh report goroutine may call this: Close joins that goroutine
// (reportWG) before closing FinalReportResults, so the unlocked send below can
// never race the channel close.
func (m *ProfileManager) enqueueProfileResult(result reporter.ProfileResult) {
	m.sendMu.Lock()
	if m.resultsClosed || m.FinalReportResults == nil {
		m.sendMu.Unlock()
		return
	}
	m.overflow = append(m.overflow, result)
	m.drainOverflowLocked()
	if len(m.overflow) <= maxOverflowResults {
		m.sendMu.Unlock()
		return
	}
	next := m.overflow[0]
	m.overflow = m.overflow[1:]
	ch := m.FinalReportResults
	m.sendMu.Unlock()

	timer := time.NewTimer(sendPipelineAlignedWait)
	defer timer.Stop()
	select {
	case ch <- next:
	case <-timer.C:
		// Consumer still stalled: keep the result rather than dropping it.
		m.sendMu.Lock()
		if !m.resultsClosed {
			m.overflow = append([]reporter.ProfileResult{next}, m.overflow...)
		}
		m.sendMu.Unlock()
	}
}

// drainOverflowLocked pushes overflow into FinalReportResults without blocking.
// Caller must hold sendMu.
func (m *ProfileManager) drainOverflowLocked() {
	for len(m.overflow) > 0 {
		select {
		case m.FinalReportResults <- m.overflow[0]:
			m.overflow = m.overflow[1:]
		default:
			return
		}
	}
}

// flushOverflowBlocking delivers any overflow before the results channel is closed.
// Each send is bounded so a stuck reporter consumer cannot hang ProfileManager.Close.
func (m *ProfileManager) flushOverflowBlocking() {
	deadline := time.Now().Add(profileCloseFlushTimeout)
	for {
		m.sendMu.Lock()
		if m.resultsClosed || m.FinalReportResults == nil || len(m.overflow) == 0 {
			m.sendMu.Unlock()
			return
		}
		next := m.overflow[0]
		m.overflow = m.overflow[1:]
		ch := m.FinalReportResults
		leftAfter := len(m.overflow)
		m.sendMu.Unlock()

		remain := time.Until(deadline)
		if remain <= 0 {
			m.logCloseFlushTimeout("overflow", leftAfter+1)
			m.sendMu.Lock()
			m.overflow = append([]reporter.ProfileResult{next}, m.overflow...)
			m.sendMu.Unlock()
			return
		}
		timer := time.NewTimer(remain)
		select {
		case ch <- next:
			timer.Stop()
		case <-timer.C:
			m.sendMu.Lock()
			m.overflow = append([]reporter.ProfileResult{next}, m.overflow...)
			left := len(m.overflow)
			m.sendMu.Unlock()
			m.logCloseFlushTimeout("overflow", left)
			return
		}
	}
}

func (m *ProfileManager) logCloseFlushTimeout(what string, remaining int) {
	if m.Log != nil {
		m.Log.Errorf("ProfileManager Close: timed out flushing %s after %s, dropping %d remaining",
			what, profileCloseFlushTimeout, remaining)
	}
}

// flushProfileOutputOnce sends IsLast / pending exactly once (Close or monitor).
func (m *ProfileManager) flushProfileOutputOnce() {
	m.profileFlushOnce.Do(func() {
		if m.profilingWriter == nil {
			return
		}
		m.profilingWriter.Flush()
		m.profilingWriter.DrainPendingBlocking()
	})
}

// Close stops the profile producer and closes the results channel so the
// reporter can drain remaining buffered results and exit cleanly.
//
// Order matters: mark closed first (reject new tasks), join monitor, Flush while
// currentTask is still set so IsLast is attributed and enqueued, then clear
// currentTask and close FinalReportResults.
func (m *ProfileManager) Close() {
	m.closeOnce.Do(func() {
		m.closed.Store(true)
		// Unblock monitor before Wait so Close cannot stall for task duration.
		select {
		case <-m.monitorStop:
		default:
			close(m.monitorStop)
		}

		m.mu.Lock()
		for _, t := range m.pendingTimers {
			t.Stop()
		}
		m.pendingTimers = nil
		owned := m.cpuProfileOwned
		m.cpuProfileOwned = false
		hasTask := m.currentTask != nil
		m.mu.Unlock()

		// Only stop CPU profiling if this manager started it; otherwise a host
		// or pprof-task profiler could be torn down incorrectly.
		if owned {
			pprof.StopCPUProfile()
			releaseCPUProfiling()
		}
		// Wait for monitor so it cannot Flush/Drain after we close rawCh.
		m.monitorWG.Wait()

		if m.profilingWriter != nil {
			if hasTask {
				m.flushProfileOutputOnce()
			}
			m.profilingWriter.Close()
		}

		m.mu.Lock()
		if m.rawCh != nil {
			close(m.rawCh)
			m.rawCh = nil
		}
		m.mu.Unlock()
		m.reportWG.Wait()

		// Deliver any results that did not fit in FinalReportResults during report.
		m.flushOverflowBlocking()

		m.mu.Lock()
		m.currentTask = nil
		m.mu.Unlock()

		m.sendMu.Lock()
		m.resultsClosed = true
		m.overflow = nil
		if m.FinalReportResults != nil {
			close(m.FinalReportResults)
		}
		m.sendMu.Unlock()
	})
}

func NewProfileManager(log operator.LogOperator) *ProfileManager {
	pm := &ProfileManager{
		FinalReportResults: make(chan reporter.ProfileResult, maxSendQueueSize),
		profileEvents:      NewEventManager(),
		ProfileTaskQueue:   make([]*reporter.TraceProfileTask, 0),
		monitorStop:        make(chan struct{}),
	}
	pm.RegisterProfileEvents()

	if log == nil {
		log = newDefaultLogger()
	}
	pm.Log = log
	pm.initReportChannel()
	pm.profilingWriter = NewProfilingWriter(
		ChunkSize,
		pm.rawCh,
	)
	return pm
}

func (m *ProfileManager) AddProfileTask(args []*common.KeyStringValuePair, t int64) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Load() {
		return t
	}
	var task reporter.TraceProfileTask
	for _, arg := range args {
		switch arg.Key {
		case "TaskId":
			task.TaskID = arg.Value
		case "EndpointName":
			task.EndpointName = arg.Value
		case "Duration":
			// Duration min
			task.Duration = parseInt(arg.Value)
		case "MinDurationThreshold":
			task.MinDurationThreshold = parseInt64(arg.Value)
		case "DumpPeriod":
			task.DumpPeriod = parseInt(arg.Value)
		case "MaxSamplingCount":
			task.MaxSamplingCount = parseInt(arg.Value)
		case "StartTime":
			task.StartTime = time.UnixMilli(parseInt64(arg.Value))
		case "CreateTime":
			temp := parseInt64(arg.Value)
			task.CreateTime = time.UnixMilli(temp)
			if temp > t {
				t = temp
			}
		case "SerialNumber":
			task.SerialNumber = arg.Value
		}
	}
	m.Log.Info("adding profile task:", task)
	endTime := task.StartTime.Add(time.Duration(task.Duration) * time.Minute)
	task.EndTime = endTime
	task.Status = reporter.Pending
	m.addTask(&task)
	return t
}

func (m *ProfileManager) RemoveProfileTask() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.TraceProfileTask == nil {
		return
	}
	if m.TraceProfileTask.Status == reporter.Reported ||
		time.Now().After(m.TraceProfileTask.EndTime.Add(timeOut)) {
		m.TraceProfileTask = nil
	}
}

func (m *ProfileManager) addTask(task *reporter.TraceProfileTask) {
	if task == nil {
		return
	}
	for _, t := range m.ProfileTaskQueue {
		if task.EndTime.After(t.StartTime) && task.StartTime.Before(t.EndTime) {
			return
		}
	}
	m.ProfileTaskQueue = append(m.ProfileTaskQueue, task)

	delay := time.Until(task.StartTime)
	if delay < 0 {
		delay = 0
	}

	var timer *time.Timer
	timer = time.AfterFunc(delay, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.removePendingTimerLocked(timer)
		if m.closed.Load() {
			return
		}
		if m.TraceProfileTask != nil {
			return
		}
		m.TraceProfileTask = task
		m.trySetCurrentTaskAndStartProfile(task)
	})
	m.pendingTimers = append(m.pendingTimers, timer)
}

func (m *ProfileManager) removePendingTimerLocked(timer *time.Timer) {
	if timer == nil {
		return
	}
	for i, t := range m.pendingTimers {
		if t == timer {
			m.pendingTimers = append(m.pendingTimers[:i], m.pendingTimers[i+1:]...)
			return
		}
	}
}

// tryStartCPUProfiling must be called with m.mu held: Close also takes m.mu, so
// the lock is what guarantees monitorWG.Add happens before Close's monitorWG.Wait.
func (m *ProfileManager) tryStartCPUProfiling() {
	if m.closed.Load() {
		return
	}
	ok, err := m.profileEvents.ExecuteComplexEvent(CouldProfile)
	if err != nil {
		m.Log.Errorf("profile event error:%v", err)
		return
	}
	t := m.TraceProfileTask
	if ok && t != nil && t.Status == reporter.Pending {
		if !tryAcquireCPUProfiling() {
			m.Log.Info("CPU profiling already running, skip trace profile start")
			return
		}
		err := pprof.StartCPUProfile(m.profilingWriter)
		if err != nil {
			releaseCPUProfiling()
			m.Log.Info("failed to start cpu profiling", err)
			return
		}
		m.cpuProfileOwned = true
		err = m.profileEvents.UpdateBaseEventStatus(IfProfiling, true)
		if err != nil {
			m.Log.Errorf("update profile event error:%v", err)
		}
		t.Status = reporter.Running
		m.monitorWG.Add(1)
		go func() {
			defer m.monitorWG.Done()
			m.monitor()
		}()
	}
}

func (m *ProfileManager) CheckIfProfileTarget(endpoint string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.currentTask == nil {
		return false
	}
	return m.currentTask.endpointName == endpoint
}

func (m *ProfileManager) IfProfiling() bool {
	ok, err := m.profileEvents.GetBaseEventStatus(IfProfiling)
	if err != nil {
		m.Log.Errorf("get profile event error:%v", err)
		return false
	}
	return ok
}

func (m *ProfileManager) trySetCurrentTaskAndStartProfile(task *reporter.TraceProfileTask) {
	if m.currentTask != nil && time.Now().Before(m.currentTask.endTime.Add(timeOut)) {
		return
	}
	ok, err := m.profileEvents.ExecuteComplexEvent(CouldSetCurTask)
	if err != nil {
		m.Log.Errorf("profile event error:%v", err)
	}
	if ok {
		m.generateCurrentTask(task)
		m.tryStartCPUProfiling()
	}
}

func (m *ProfileManager) generateProfileLabels(traceSegmentID string, minDurationThreshold int64) profileLabels {
	var l = LabelSet{}
	l = UpdateTraceLabels(l, SegmentLabel, traceSegmentID, MinDurationLabel, strconv.FormatInt(minDurationThreshold, 10))
	return profileLabels{
		labels: &l,
	}
}

func (m *ProfileManager) generateCurrentTask(t *reporter.TraceProfileTask) {
	var c = currentTask{
		serialNumber:         t.SerialNumber,
		taskID:               t.TaskID,
		minDurationThreshold: t.MinDurationThreshold,
		duration:             t.Duration,
		endpointName:         t.EndpointName,
		endTime:              t.EndTime,
	}
	m.currentTask = &c
	err := m.profileEvents.UpdateBaseEventStatus(CurTaskExist, true)
	if err != nil {
		m.Log.Errorf("profile event error:%v", err)
	}
}

func (m *ProfileManager) TryToAddSegmentLabelSet(traceSegmentID string) {
	m.mu.Lock()
	task := m.currentTask
	m.mu.Unlock()
	if task == nil {
		return
	}
	c := m.generateProfileLabels(traceSegmentID, task.minDurationThreshold)
	SetGoroutineLabels(c.labels)
}

func (m *ProfileManager) monitor() {
	m.mu.Lock()
	if m.closed.Load() || m.currentTask == nil {
		m.mu.Unlock()
		return
	}
	duration := m.currentTask.duration
	m.mu.Unlock()

	timer := time.NewTimer(time.Duration(duration) * time.Minute)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-m.monitorStop:
		// Close owns StopCPUProfile + Flush.
		return
	}

	if m.closed.Load() {
		return
	}
	m.mu.Lock()
	owned := m.cpuProfileOwned
	if owned {
		m.cpuProfileOwned = false
	}
	m.mu.Unlock()
	if owned {
		pprof.StopCPUProfile()
		releaseCPUProfiling()
	}
	err := m.profileEvents.UpdateBaseEventStatus(IfProfiling, false)
	if err != nil {
		m.Log.Errorf("profile event error:%v", err)
	}
	if m.closed.Load() {
		return
	}
	m.flushProfileOutputOnce()
}

func (m *ProfileManager) AddSpanID(traceID, segmentID string, spanID int32) {
	l := m.AddSkyLabels(traceID, segmentID, spanID).(*LabelSet)
	SetGoroutineLabels(l)
}

func (m *ProfileManager) GetProfileResults() chan reporter.ProfileResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.FinalReportResults
}

func (m *ProfileManager) ProfileFinish() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.TraceProfileTask == nil {
		return
	}
	m.TraceProfileTask.Status = reporter.Reported
}

func parseInt64(value string) int64 {
	v, _ := strconv.ParseInt(value, 10, 64)
	return v
}

func parseInt(value string) int {
	v, _ := strconv.Atoi(value)
	return v
}
func parseString(value int32) string {
	str := strconv.Itoa(int(value))
	return str
}
