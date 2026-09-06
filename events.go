/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type eventDebouncer struct {
	name   string
	timer  *time.Timer
	mu     sync.Mutex
	events []frame

	callback func([]frame)
	quit     chan struct{}
	// done is closed when the flusher goroutine exits, allowing stop()
	// to wait until callback dispatch has actually ceased. Without this,
	// a Session.Close → stop() could return before a pending timer tick
	// fired one last callback against already-stopped components.
	done chan struct{}

	logger StructuredLogger
}

func newEventDebouncer(name string, eventHandler func([]frame), logger StructuredLogger) *eventDebouncer {
	e := &eventDebouncer{
		name:     name,
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
		timer:    time.NewTimer(eventDebounceTime),
		callback: eventHandler,
		logger:   logger,
	}
	e.timer.Stop()
	go e.flusher()

	return e
}

func (e *eventDebouncer) stop() {
	// Close-only signal: the flusher's `case <-e.quit` returns whether
	// quit is closed or sent-to. Using close instead of send+close means
	// stop() does not deadlock if the flusher exited early (e.g. via a
	// recovered panic). The <-e.done wait then restores the original
	// synchronization guarantee — stop() returns only after the flusher
	// goroutine has actually exited, so a pending timer cannot fire a
	// late callback against already-stopped components.
	close(e.quit)
	<-e.done
}

func (e *eventDebouncer) flusher() {
	defer close(e.done)
	defer recoverGoroutine(e.logger, "eventDebouncer.flusher", nil)
	for {
		select {
		case <-e.timer.C:
			e.mu.Lock()
			e.flush()
			e.mu.Unlock()
		case <-e.quit:
			return
		}
	}
}

const (
	eventBufferSize   = 1000
	eventDebounceTime = 1 * time.Second
)

// flush must be called with mu locked
func (e *eventDebouncer) flush() {
	if len(e.events) == 0 {
		return
	}

	// if the flush interval is faster than the callback then we will end up calling
	// the callback multiple times, probably a bad idea. In this case we could drop
	// frames?
	events := e.events
	logger := e.logger
	go func() {
		defer recoverGoroutine(logger, "eventDebouncer.callback", nil)
		e.callback(events)
	}()
	e.events = make([]frame, 0, eventBufferSize)
}

func (e *eventDebouncer) debounce(frame frame) {
	e.mu.Lock()
	e.timer.Reset(eventDebounceTime)

	// TODO: probably need a warning to track if this threshold is too low
	if len(e.events) < eventBufferSize {
		e.events = append(e.events, frame)
	} else {
		e.logger.Warning("Event buffer full, dropping event frame.",
			NewLogFieldString("event_name", e.name), NewLogFieldStringer("frame", frame))
	}

	e.mu.Unlock()
}

func (s *Session) handleEvent(framer *framer) {
	defer framer.release() // Framer was read in processFrame, needs release here
	frame, err := framer.parseFrame()
	if err != nil {
		s.logger.Error("Unable to parse event frame.", NewLogFieldError("err", err))
		return
	}

	s.logger.Debug("Handling event frame.", NewLogFieldStringer("frame", frame))

	switch f := frame.(type) {
	case *schemaChangeKeyspace, *schemaChangeFunction,
		*schemaChangeTable, *schemaChangeAggregate, *schemaChangeType:
		s.schemaDescriber.debounceRefreshSchemaMetadata()
	case *topologyChangeEventFrame, *statusChangeEventFrame:
		s.nodeEvents.debounce(frame)
	default:
		s.logger.Error("Invalid event frame.",
			NewLogFieldString("frame_type", fmt.Sprintf("%T", f)), NewLogFieldStringer("frame", f))
	}
}

// handleNodeEvent handles inbound status and topology change events.
//
// Status events are debounced by host IP; only the latest event is processed.
//
// Topology events are debounced by performing a single full topology refresh
// whenever any topology event comes in.
//
// Processing topology change events before status change events ensures
// that a NEW_NODE event is not dropped in favor of a newer UP event (which
// would itself be dropped/ignored, as the node is not yet known).
func (s *Session) handleNodeEvent(frames []frame) {
	type nodeEvent struct {
		change string
		host   net.IP
		port   int
	}

	topologyEventReceived := false
	// status change events
	sEvents := make(map[string]*nodeEvent)

	for _, frame := range frames {
		switch f := frame.(type) {
		case *topologyChangeEventFrame:
			s.logger.Info("Received topology change event.",
				NewLogFieldString("frame", strings.Join([]string{f.change, "->", f.host.String(), ":", strconv.Itoa(f.port)}, "")))
			topologyEventReceived = true
		case *statusChangeEventFrame:
			event, ok := sEvents[f.host.String()]
			if !ok {
				event = &nodeEvent{change: f.change, host: f.host, port: f.port}
				sEvents[f.host.String()] = event
			}
			event.change = f.change
		}
	}

	if topologyEventReceived && !s.cfg.Events.DisableTopologyEvents {
		s.debounceRingRefresh()
	}

	for _, f := range sEvents {
		s.logger.Info("Dispatching status change event.",
			NewLogFieldString("frame", strings.Join([]string{f.change, "->", f.host.String(), ":", strconv.Itoa(f.port)}, "")))

		// ignore events we received if they were disabled
		// see https://github.com/apache/cassandra-gocql-driver/issues/1591
		switch f.change {
		case nodeStateChangeUp:
			if !s.cfg.Events.DisableNodeStatusEvents {
				s.handleNodeUp(f.host, f.port)
			}
		case nodeStateChangeDown:
			if !s.cfg.Events.DisableNodeStatusEvents {
				s.handleNodeDown(f.host, f.port)
			}
		}
	}
}

func (s *Session) handleNodeUp(eventIp net.IP, eventPort int) {
	s.logger.Info("Node is UP.",
		NewLogFieldStringer("event_ip", eventIp), NewLogFieldInt("event_port", eventPort))

	host, ok := s.ring.getHostByIP(eventIp.String())
	if !ok {
		s.debounceRingRefresh()
		return
	}

	if s.cfg.filterHost(host) {
		return
	}

	if d := host.Version().nodeUpDelay(); d > 0 {
		time.Sleep(d)
	}
	s.startPoolFill(host)
}

// startPoolFill admits host's pool and publishes host to the selection policy.
//
// Both steps follow ring ownership: the pool admission checks it under the
// pool's own lock, and the publication runs under hostPublishMu with a fresh
// check, so a caller that still holds an object refreshRing has since replaced
// neither registers a pool for it nor puts it back into the policy.
//
// Parameters:
//   - host: the ring object to fill and publish
func (s *Session) startPoolFill(host *HostInfo) {
	if s.cfg.testStartPoolFillStart != nil {
		s.cfg.testStartPoolFillStart(host)
	}
	// we let the pool call handleNodeConnected to change the host state
	s.pool.addHost(host)
	s.withOwnedHost(host, func() bool {
		s.policy.AddHost(host)
		return true
	})
	if s.cfg.testStartPoolFillDone != nil {
		s.cfg.testStartPoolFillDone(host)
	}
}

func (s *Session) handleNodeConnected(host *HostInfo) {
	if s.testAfterNodeConnected != nil {
		defer s.testAfterNodeConnected(host)
	}
	if !s.ring.owns(host) {
		return
	}
	if s.testAfterOwnsConnected != nil {
		s.testAfterOwnsConnected()
	}

	// The ownership and pool checks, the state change and the policy
	// publication form one transition under hostPublishMu,
	// so a removal or a DOWN of the same host cannot interleave with it.
	if s.withOwnedHost(host, func() bool {
		if _, ok := s.pool.getPoolFor(host); !ok {
			// The pool was removed or replaced after the fill succeeded; the
			// host stays in its current state and the replacement's own fill
			// (or reconnectDownedHosts) owns recovery.
			return false
		}

		s.logger.Debug("Pool connected to node.",
			NewLogFieldIP("host_addr", host.ConnectAddress()), NewLogFieldInt("port", host.Port()), NewLogFieldString("host_id", host.HostID()))

		host.setState(NodeUp)

		if s.cfg.filterHost(host) {
			return false
		}
		s.policy.HostUp(host)
		return true
	}) {
		s.hostListeners.OnHostUp(HostUpEvent{Host: host})
	}
}

func (s *Session) handleNodeDown(ip net.IP, port int) {
	s.logger.Warning("Node is DOWN.",
		NewLogFieldIP("host_addr", ip), NewLogFieldInt("port", port))

	host, ok := s.ring.getHostByIP(ip.String())
	if ok {
		s.markHostDown(host)
	}
}

// handleHostDown marks host DOWN by identity.
//
// The *HostInfo must be the ring's current object for its host ID (ring.owns):
// contact-point objects without an ID and objects replaced by refreshRing under the same ID are ignored.
// Unlike handleNodeDown, which is keyed by the broadcast address a server event carries,
// this path is used by driver-side failure detection (pool fill failure, control dial failure)
// that holds the object it failed on,
// so it works behind port mapping, NAT and an AddressTranslator where the address lookup would miss.
//
// Parameters:
//   - host: the ring object to mark DOWN; nil is ignored
func (s *Session) handleHostDown(host *HostInfo) {
	if !s.ring.owns(host) {
		if host != nil {
			s.logger.Debug("Ignoring DOWN for a host that is not the current ring entry.",
				NewLogFieldIP("host_addr", host.ConnectAddress()), NewLogFieldInt("port", host.Port()), NewLogFieldString("host_id", host.HostID()))
		}
		return
	}
	if s.testAfterOwnsDown != nil {
		s.testAfterOwnsDown()
	}

	s.logger.Warning("Node is DOWN.",
		NewLogFieldIP("host_addr", host.ConnectAddress()), NewLogFieldInt("port", host.Port()), NewLogFieldString("host_id", host.HostID()))
	s.markHostDown(host)
}

// markHostDown applies the DOWN transition to a host the caller has already
// resolved: state, policy, pool removal (pointer-checked) and listeners.
//
// Parameters:
//   - host: the resolved ring object
func (s *Session) markHostDown(host *HostInfo) {
	// State, policy and pool change as one transition under hostPublishMu,
	// so a late fill success for the same host cannot leave it UP with no pool.
	// An object the ring no longer owns is left untouched: its state no longer
	// matters, and reporting it DOWN to a policy that keys by address could
	// evict a replacement that took the same address.
	if s.withOwnedHost(host, func() bool {
		host.setState(NodeDown)
		if s.cfg.filterHost(host) {
			return false
		}

		s.policy.HostDown(host)
		s.pool.removeHost(host)
		return true
	}) {
		s.hostListeners.OnHostDown(HostDownEvent{Host: host})
	}
}

const (
	nodeStateChangeUp   = "UP"
	nodeStateChangeDown = "DOWN"
)
