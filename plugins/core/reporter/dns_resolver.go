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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reporter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/resolver"

	"github.com/apache/skywalking-go/plugins/core/operator"
)

const (
	periodicDNSScheme = "skywalking-periodic-dns"

	// Cap a single DNS lookup so Build/Close cannot hang on a stuck resolver.
	dnsLookupTimeout = 5 * time.Second

	updateStateMinBackoff = 1 * time.Second

	dnsErrKeyNoUsableAddresses = "no usable addresses"
	dnsErrKeyTimeout           = "dns timeout"
	dnsErrKeyCanceled          = "dns canceled"
	dnsErrKeyNotFound          = "dns not found"
	dnsErrKeyTemporary         = "dns temporary"
	dnsErrKeyGeneric           = "dns error"
	dnsErrKeyNetwork           = "dns network"
	dnsErrKeyLookupFailed      = "dns lookup failed"
	dnsErrKeyInvalidBackend    = "invalid backend address"
	updateStateErrKey          = "update_state_failed"
)

type dnsLookupFunc func(ctx context.Context, host string) ([]string, error)

func netLookupHost(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
}

type periodicDNSResolverBuilder struct {
	logger      operator.LogOperator
	serverAddr  string
	interval    time.Duration
	lookup      dnsLookupFunc
	targetValue string
}

func newPeriodicDNSResolverBuilder(logger operator.LogOperator, serverAddr string,
	interval time.Duration, lookup dnsLookupFunc) (*periodicDNSResolverBuilder, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("periodic DNS resolver interval must be greater than 0")
	}
	if lookup == nil {
		lookup = netLookupHost
	}
	if _, _, err := splitBackendServiceAddress(serverAddr); err != nil {
		return nil, err
	}
	serverAddr = strings.TrimSpace(serverAddr)
	return &periodicDNSResolverBuilder{
		logger:      logger,
		serverAddr:  serverAddr,
		interval:    interval,
		lookup:      lookup,
		targetValue: fmt.Sprintf("%s:///%s", periodicDNSScheme, serverAddr),
	}, nil
}

//nolint:gocritic // resolver.Builder requires resolver.Target by value.
func (b *periodicDNSResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn,
	opts resolver.BuildOptions) (resolver.Resolver, error) {
	lookupCtx, cancel := context.WithCancel(context.Background())
	r := &periodicDNSResolver{
		logger:     b.logger,
		serverAddr: b.serverAddr,
		interval:   b.interval,
		lookup:     b.lookup,
		cc:         cc,
		done:       make(chan struct{}),
		trigger:    make(chan struct{}, 1),
		lookupCtx:  lookupCtx,
		cancel:     cancel,
	}
	// Seed a hostname address without DNS so Dial/Build is not blocked on lookup.
	// The watch loop performs the first real DNS resolve immediately.
	if err := r.seedHostnameState(); err != nil {
		cancel()
		return nil, err
	}
	r.watchDone = make(chan struct{})
	go func() {
		defer close(r.watchDone)
		defer func() {
			if rec := recover(); rec != nil {
				if r.logger != nil {
					r.logger.Errorf("periodic DNS resolver watch panic, resolver stopped: %v\n%s",
						rec, debug.Stack())
				}
			}
		}()
		r.watch()
	}()
	return r, nil
}

func (b *periodicDNSResolverBuilder) Scheme() string {
	return periodicDNSScheme
}

func (b *periodicDNSResolverBuilder) target() string {
	return b.targetValue
}

type periodicDNSResolver struct {
	logger     operator.LogOperator
	serverAddr string
	interval   time.Duration
	lookup     dnsLookupFunc
	cc         resolver.ClientConn
	done       chan struct{}
	trigger    chan struct{}
	closeOnce  sync.Once
	lookupCtx  context.Context
	cancel     context.CancelFunc
	// watchDone is closed when the watch goroutine exits; nil if watch never started.
	watchDone chan struct{}

	resolveMu         sync.Mutex
	lastGoodAddrs     []resolver.Address
	lastReported      []resolver.Address
	updateFailBackoff time.Duration
	// updateWG tracks UpdateState calls so Close can wait without holding
	// resolveMu across ClientConn.UpdateState (avoids deadlock with Conn.Close).
	updateWG sync.WaitGroup

	// DNS health logging: emit on first failure / error-type change / recovery only.
	lookupUnhealthy       bool
	lookupFailures        int
	lastLoggedLookupErr   string
	lastLoggedUpdateState string
}

// ResolveNow is only a hint from gRPC and may be called concurrently. Coalesce
// signals onto the watch loop so slow DNS cannot spawn unbounded goroutines.
func (r *periodicDNSResolver) ResolveNow(opts resolver.ResolveNowOptions) {
	select {
	case <-r.done:
	case r.trigger <- struct{}{}:
	default:
	}
}

func (r *periodicDNSResolver) Close() {
	r.closeOnce.Do(func() {
		// Cancel in-flight DNS first (lookup does not hold resolveMu).
		if r.cancel != nil {
			r.cancel()
		}
		close(r.done)
		// Join watch before waiting UpdateState: watch is the only publisher,
		// and UpdateState runs outside resolveMu so Conn.Close cannot deadlock.
		if r.watchDone != nil {
			<-r.watchDone
		}
		r.updateWG.Wait()
	})
}

func (r *periodicDNSResolver) watch() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	var backoffTimer *time.Timer
	var backoffC <-chan time.Time
	stopBackoff := func() {
		if backoffTimer == nil {
			return
		}
		if !backoffTimer.Stop() {
			select {
			case <-backoffTimer.C:
			default:
			}
		}
		backoffTimer = nil
		backoffC = nil
	}
	defer stopBackoff()

	scheduleBackoff := func(d time.Duration) {
		stopBackoff()
		if d <= 0 {
			return
		}
		backoffTimer = time.NewTimer(d)
		backoffC = backoffTimer.C
	}

	// Resolve immediately so addresses refresh without waiting for the first tick.
	scheduleBackoff(r.resolveAndUpdate())

	for {
		select {
		case <-ticker.C:
			scheduleBackoff(r.resolveAndUpdate())
		case <-r.trigger:
			scheduleBackoff(r.resolveAndUpdate())
		case <-backoffC:
			scheduleBackoff(r.resolveAndUpdate())
		case <-r.done:
			return
		}
	}
}

// seedHostnameState publishes a non-DNS address so ClientConn can start dialing
// while the first lookup runs asynchronously in watch.
func (r *periodicDNSResolver) seedHostnameState() error {
	var addresses []resolver.Address
	host, port, err := splitBackendServiceAddress(r.serverAddr)
	if err != nil {
		addr := strings.TrimSpace(r.serverAddr)
		addresses = []resolver.Address{{Addr: addr, ServerName: addr}}
	} else {
		addresses = []resolver.Address{newResolverAddress(host, host, port)}
	}
	if err := r.cc.UpdateState(resolver.State{Addresses: addresses}); err != nil {
		return fmt.Errorf("periodic DNS resolver initial UpdateState rejected by ClientConn: %w", err)
	}
	r.lastReported = cloneResolverAddresses(addresses)
	return nil
}

// resolveAndUpdate refreshes addresses. When UpdateState fails it returns a
// backoff duration so watch can retry sooner than the full resolve interval.
func (r *periodicDNSResolver) resolveAndUpdate() time.Duration {
	select {
	case <-r.done:
		return 0
	default:
	}

	// DNS lookup intentionally runs without resolveMu so Close can cancel and
	// return without waiting on a stuck resolver beyond the lookup timeout.
	host, port, ips, lookupErr := r.lookupBackendServiceIPs()

	r.resolveMu.Lock()
	select {
	case <-r.done:
		r.resolveMu.Unlock()
		return 0
	default:
	}

	canceled := lookupErr != nil && r.lookupCtx != nil && r.lookupCtx.Err() != nil
	usable := uniqueResolverAddresses(ips, host, port)
	dnsHealthy := !canceled && lookupErr == nil && host != "" && len(usable) > 0

	addresses := r.addressesFromLookupLocked(host, port, ips, lookupErr)
	r.noteDNSLookupOutcomeLocked(host, canceled, dnsHealthy, lookupErr)
	if sameResolverAddresses(addresses, r.lastReported) {
		r.updateFailBackoff = 0
		r.resolveMu.Unlock()
		return 0
	}
	toPublish := cloneResolverAddresses(addresses)
	r.updateWG.Add(1)
	r.resolveMu.Unlock()

	// UpdateState must not run under resolveMu: ClientConn.Close may call
	// resolver.Close which needs to join watch without waiting on this lock.
	var backoff time.Duration
	func() {
		defer r.updateWG.Done()
		err := r.cc.UpdateState(resolver.State{Addresses: toPublish})

		r.resolveMu.Lock()
		defer r.resolveMu.Unlock()
		select {
		case <-r.done:
			return
		default:
		}
		if err != nil {
			if r.logger != nil && r.lastLoggedUpdateState != updateStateErrKey {
				r.lastLoggedUpdateState = updateStateErrKey
				r.logger.Errorf("update periodic DNS resolver state error: %v, will retry with backoff", err)
			}
			// Do not ReportError: retry with local backoff (and gRPC may also ResolveNow).
			r.updateFailBackoff = nextUpdateStateBackoff(r.updateFailBackoff, r.interval)
			backoff = r.updateFailBackoff
			return
		}
		r.lastLoggedUpdateState = ""
		if r.logger != nil {
			added, removed := addressSetDelta(r.lastReported, toPublish)
			r.logger.Infof("periodic DNS resolver updated backend address set: count=%d added=%d removed=%d",
				len(toPublish), added, removed)
		}
		r.lastReported = toPublish
		r.updateFailBackoff = 0
		backoff = 0
	}()
	return backoff
}

func (r *periodicDNSResolver) noteDNSLookupOutcomeLocked(host string, canceled, dnsHealthy bool, lookupErr error) {
	if canceled {
		return
	}
	if dnsHealthy {
		r.noteDNSLookupRecoveredLocked(host)
		return
	}
	switch {
	case host == "" && lookupErr != nil:
		r.noteDNSLookupFailureLocked(host, dnsErrKeyInvalidBackend, lookupErr)
	case lookupErr != nil:
		r.noteDNSLookupFailureLocked(host, dnsLookupErrorKey(lookupErr), lookupErr)
	default:
		r.noteDNSLookupFailureLocked(host, dnsErrKeyNoUsableAddresses, nil)
	}
}

func (r *periodicDNSResolver) noteDNSLookupFailureLocked(host, errKey string, lookupErr error) {
	r.lookupFailures++
	r.lookupUnhealthy = true
	if errKey == r.lastLoggedLookupErr {
		return
	}
	r.lastLoggedLookupErr = errKey
	if r.logger == nil {
		return
	}
	fallback := "fallback to hostname"
	if len(r.lastGoodAddrs) > 0 {
		fallback = "keep last-good"
	}
	switch {
	case errKey == dnsErrKeyInvalidBackend:
		r.logger.Errorf("backend service address format error: %v, %s (failures=%d, period=%s)",
			lookupErr, fallback, r.lookupFailures, r.interval)
	case lookupErr != nil:
		r.logger.Errorf("failed to resolve %s of backend service: %v, %s (failures=%d, period=%s)",
			host, lookupErr, fallback, r.lookupFailures, r.interval)
	default:
		r.logger.Errorf("resolved %s of backend service returned no usable addresses, %s (failures=%d, period=%s)",
			host, fallback, r.lookupFailures, r.interval)
	}
}

func (r *periodicDNSResolver) noteDNSLookupRecoveredLocked(host string) {
	if !r.lookupUnhealthy {
		return
	}
	failures := r.lookupFailures
	r.lookupUnhealthy = false
	r.lookupFailures = 0
	r.lastLoggedLookupErr = ""
	if r.logger != nil {
		r.logger.Infof("periodic DNS resolver recovered for %s after %d failures (period=%s)",
			host, failures, r.interval)
	}
}

// dnsLookupErrorKey classifies lookup failures into stable buckets so ephemeral
// details in Error() (ports, resolver IDs) do not defeat log rate-limiting.
func dnsLookupErrorKey(err error) string {
	if err == nil {
		return dnsErrKeyNoUsableAddresses
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return dnsErrKeyTimeout
	}
	if errors.Is(err, context.Canceled) {
		return dnsErrKeyCanceled
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		switch {
		case dnsErr.IsNotFound:
			return dnsErrKeyNotFound
		case dnsErr.IsTimeout:
			return dnsErrKeyTimeout
		case dnsErr.IsTemporary:
			return dnsErrKeyTemporary
		default:
			return dnsErrKeyGeneric
		}
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Timeout() {
			return dnsErrKeyTimeout
		}
		return dnsErrKeyNetwork
	}
	return dnsErrKeyLookupFailed
}

func nextUpdateStateBackoff(current, interval time.Duration) time.Duration {
	if current <= 0 {
		current = updateStateMinBackoff
	} else {
		current *= 2
	}
	if interval > 0 && current > interval {
		current = interval
	}
	return current
}

func (r *periodicDNSResolver) lookupBackendServiceIPs() (host, port string, ips []string, err error) {
	host, port, err = splitBackendServiceAddress(r.serverAddr)
	if err != nil {
		return "", "", nil, err
	}

	ctx := r.lookupCtx
	if ctx == nil {
		ctx = context.Background()
	}
	lookupCtx, cancel := context.WithTimeout(ctx, r.lookupTimeout())
	defer cancel()

	ips, err = r.lookup(lookupCtx, host)
	return host, port, ips, err
}

// lookupTimeout caps a single DNS query. It never exceeds dnsLookupTimeout and
// shrinks to the resolve interval when that interval is shorter, so a 1s refresh
// period cannot be blocked by a 5s lookup.
func (r *periodicDNSResolver) lookupTimeout() time.Duration {
	timeout := dnsLookupTimeout
	if r.interval > 0 && r.interval < timeout {
		return r.interval
	}
	return timeout
}

func (r *periodicDNSResolver) addressesFromLookupLocked(host, port string, ips []string, lookupErr error) []resolver.Address {
	if host == "" && lookupErr != nil {
		return r.addressesOnInvalidServerAddrLocked()
	}
	if lookupErr != nil {
		return r.lastGoodOrHostnameLocked(host, port)
	}
	addresses := uniqueResolverAddresses(ips, host, port)
	if len(addresses) == 0 {
		return r.lastGoodOrHostnameLocked(host, port)
	}
	// Preserve DNS response order so ClientConn can use RR / first-listed preference.
	r.lastGoodAddrs = cloneResolverAddresses(addresses)
	return addresses
}

func (r *periodicDNSResolver) addressesOnInvalidServerAddrLocked() []resolver.Address {
	if len(r.lastGoodAddrs) > 0 {
		return cloneResolverAddresses(r.lastGoodAddrs)
	}
	return []resolver.Address{{
		Addr:       strings.TrimSpace(r.serverAddr),
		ServerName: strings.TrimSpace(r.serverAddr),
	}}
}

func (r *periodicDNSResolver) lastGoodOrHostnameLocked(host, port string) []resolver.Address {
	if len(r.lastGoodAddrs) > 0 {
		return cloneResolverAddresses(r.lastGoodAddrs)
	}
	return []resolver.Address{newResolverAddress(host, host, port)}
}

func uniqueResolverAddresses(ips []string, host, port string) []resolver.Address {
	seen := make(map[string]struct{}, len(ips))
	addresses := make([]resolver.Address, 0, len(ips))
	for _, ip := range ips {
		ip = strings.TrimSpace(ip)
		if ip == "" {
			continue
		}
		addr := net.JoinHostPort(ip, port)
		if _, exists := seen[addr]; exists {
			continue
		}
		seen[addr] = struct{}{}
		addresses = append(addresses, newResolverAddress(ip, host, port))
	}
	return addresses
}

// resolveBackendServiceAddressesLocked is used by unit tests that construct a
// resolver without going through lookupBackendServiceIPs.
func (r *periodicDNSResolver) resolveBackendServiceAddressesLocked() []resolver.Address {
	host, port, ips, err := r.lookupBackendServiceIPs()
	return r.addressesFromLookupLocked(host, port, ips, err)
}

func newResolverAddress(addressHost, serverName, port string) resolver.Address {
	return resolver.Address{
		Addr:       net.JoinHostPort(addressHost, port),
		ServerName: serverName,
	}
}

func splitBackendServiceAddress(serverAddr string) (host, port string, err error) {
	serverAddr = strings.TrimSpace(serverAddr)
	if serverAddr == "" {
		return "", "", fmt.Errorf("backend service address is empty")
	}
	host, port, err = net.SplitHostPort(serverAddr)
	if err != nil {
		return "", "", fmt.Errorf("expected backend service address format host:port, got %q: %w", serverAddr, err)
	}
	if strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return "", "", fmt.Errorf("expected backend service address format host:port, got %q", serverAddr)
	}
	return host, port, nil
}

func sameResolverAddresses(a, b []resolver.Address) bool {
	if len(a) != len(b) {
		return false
	}
	// Compare as an order-independent set so DNS RR reshuffles do not UpdateState,
	// while still publishing the original DNS order to ClientConn.
	ac := cloneResolverAddresses(a)
	bc := cloneResolverAddresses(b)
	sortResolverAddresses(ac)
	sortResolverAddresses(bc)
	for i := range ac {
		if ac[i].Addr != bc[i].Addr || ac[i].ServerName != bc[i].ServerName {
			return false
		}
	}
	return true
}

func sortResolverAddresses(addrs []resolver.Address) {
	sort.Slice(addrs, func(i, j int) bool {
		if addrs[i].Addr != addrs[j].Addr {
			return addrs[i].Addr < addrs[j].Addr
		}
		return addrs[i].ServerName < addrs[j].ServerName
	})
}

func cloneResolverAddresses(in []resolver.Address) []resolver.Address {
	if len(in) == 0 {
		return nil
	}
	out := make([]resolver.Address, len(in))
	copy(out, in)
	return out
}

func addressSetDelta(previous, next []resolver.Address) (added, removed int) {
	prevSet := make(map[string]struct{}, len(previous))
	for _, addr := range previous {
		prevSet[addr.Addr] = struct{}{}
	}
	nextSet := make(map[string]struct{}, len(next))
	for _, addr := range next {
		nextSet[addr.Addr] = struct{}{}
		if _, ok := prevSet[addr.Addr]; !ok {
			added++
		}
	}
	for addr := range prevSet {
		if _, ok := nextSet[addr]; !ok {
			removed++
		}
	}
	return added, removed
}
