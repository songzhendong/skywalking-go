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

// DNS mock for gRPC e2e: oap-backend initially resolves to a blackhole IP, then
// flips to the real OAP address so the agent must recover via periodic DNS.
package main

import (
	"encoding/binary"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Blackhole window (blackholeFor) starts at the first backend query so slow
// image/startup still exercises mid-lifecycle DNS change.
const (
	backendName    = "oap-backend"
	realTarget     = "oap"
	blackholeIP    = "203.0.113.1"
	forwardDNS     = "127.0.0.11:53"
	forwardTimeout = 2 * time.Second
	blackholeFor   = 20 * time.Second
)

func main() {
	var (
		firstBackendOnce sync.Once
		firstBackendAt   atomic.Value // time.Time
	)

	pc, err := net.ListenPacket("udp", ":53")
	if err != nil {
		log.Fatalf("listen dns: %v", err)
	}
	defer pc.Close()
	log.Printf("dns-mock listening on :53 for %s* (blackhole %s for %s after first query, then %s)",
		backendName, blackholeIP, blackholeFor, realTarget)

	buf := make([]byte, 512)
	for {
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			log.Printf("read: %v", err)
			continue
		}
		req := make([]byte, n)
		copy(req, buf[:n])
		go handleQuery(pc, addr, req, &firstBackendOnce, &firstBackendAt)
	}
}

func handleQuery(pc net.PacketConn, addr net.Addr, req []byte,
	firstBackendOnce *sync.Once, firstBackendAt *atomic.Value) {
	name, err := readQName(req)
	if err != nil || name == "" {
		return
	}
	name = strings.TrimSuffix(strings.ToLower(name), ".")

	if isBackendName(name) {
		firstBackendOnce.Do(func() {
			firstBackendAt.Store(time.Now())
			log.Printf("first backend query %q; blackhole for %s", name, blackholeFor)
		})
		var ip net.IP
		if inBlackholeWindow(firstBackendAt) {
			ip = net.ParseIP(blackholeIP).To4()
		} else {
			ip = lookupA(realTarget)
			if ip == nil {
				ip = net.ParseIP(blackholeIP).To4()
			}
		}
		if resp := buildAResponse(req, ip); resp != nil {
			_, _ = pc.WriteTo(resp, addr)
		}
		return
	}

	forward(pc, addr, req)
}

func isBackendName(name string) bool {
	return name == backendName || strings.HasPrefix(name, backendName+".")
}

func inBlackholeWindow(firstBackendAt *atomic.Value) bool {
	v := firstBackendAt.Load()
	if v == nil {
		return true
	}
	return time.Since(v.(time.Time)) < blackholeFor
}

func lookupA(host string) net.IP {
	ips, err := net.LookupIP(host)
	if err != nil {
		log.Printf("lookup %s: %v", host, err)
		return nil
	}
	for _, ip := range ips {
		if v4 := ip.To4(); v4 != nil {
			return v4
		}
	}
	return nil
}

func forward(pc net.PacketConn, client net.Addr, req []byte) {
	conn, err := net.Dial("udp", forwardDNS)
	if err != nil {
		log.Printf("forward dial: %v", err)
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(forwardTimeout))
	if _, writeErr := conn.Write(req); writeErr != nil {
		return
	}
	resp := make([]byte, 512)
	n, readErr := conn.Read(resp)
	if readErr != nil {
		return
	}
	_, _ = pc.WriteTo(resp[:n], client)
}

func readQName(msg []byte) (string, error) {
	if len(msg) < 12 {
		return "", os.ErrInvalid
	}
	i := 12
	var labels []string
	for i < len(msg) {
		l := int(msg[i])
		if l == 0 {
			return strings.Join(labels, "."), nil
		}
		if l&0xC0 == 0xC0 {
			return "", os.ErrInvalid
		}
		i++
		if i+l > len(msg) {
			return "", os.ErrInvalid
		}
		labels = append(labels, string(msg[i:i+l]))
		i += l
	}
	return "", os.ErrInvalid
}

func buildAResponse(req []byte, ip net.IP) []byte {
	if len(req) < 12 || ip == nil || ip.To4() == nil {
		return nil
	}
	ip = ip.To4()

	i := 12
	for i < len(req) {
		l := int(req[i])
		if l == 0 {
			i++
			break
		}
		if l&0xC0 == 0xC0 {
			return nil
		}
		i += 1 + l
	}
	if i+4 > len(req) {
		return nil
	}
	questionEnd := i + 4

	resp := make([]byte, 0, questionEnd+16)
	resp = append(resp, req[:questionEnd]...)
	resp[2] = 0x84
	resp[3] = 0x80
	binary.BigEndian.PutUint16(resp[6:8], 1)
	binary.BigEndian.PutUint16(resp[8:10], 0)
	binary.BigEndian.PutUint16(resp[10:12], 0)

	resp = append(resp,
		0xC0, 0x0C,
		0x00, 0x01,
		0x00, 0x01,
		0x00, 0x00, 0x00, 0x1E,
		0x00, 0x04,
	)
	resp = append(resp, ip...)
	return resp
}
