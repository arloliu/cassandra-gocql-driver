//go:build ccm
// +build ccm

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

package ccm

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

func execCmd(args ...string) (*bytes.Buffer, error) {
	execName := "ccm"
	if runtime.GOOS == "windows" {
		args = append([]string{"/c", execName}, args...)
		execName = "cmd.exe"
	}
	cmd := exec.Command(execName, args...)
	stdout := &bytes.Buffer{}
	cmd.Stdout = stdout
	cmd.Stderr = &bytes.Buffer{}
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("Failed to execute command: [ %s ], err: %w, stderr: %s", cmd.String(), err, cmd.Stderr.(*bytes.Buffer).String())
	}

	return stdout, nil
}

// Starts nodes that are not up, and waits for them to be up before returning
func AllUp() error {
	status, err := Status()
	if err != nil {
		return err
	}

	for _, host := range status {
		if !host.State.IsUp() {
			if err := NodeUp(host.Name); err != nil {
				return err
			}
		}
	}

	return nil
}

// Runs ccm start --wait-for-binary-proto
func StartAll() error {
	_, err := execCmd("start", "--wait-for-binary-proto")
	return err
}

func NodeUp(node string) error {
	args := []string{node, "start", "--wait-for-binary-proto"}
	if runtime.GOOS == "windows" {
		args = append(args, "--quiet-windows")
	}
	_, err := execCmd(args...)
	return err
}

func NodeDown(node string) error {
	_, err := execCmd(node, "stop")
	return err
}

// Pause suspends the node's Cassandra process with SIGSTOP.
//
// The node keeps its listening sockets, so clients still complete TCP
// handshakes against it, but no request, TLS handshake or STARTUP exchange
// is served until Resume is called.
//
// Parameters:
//   - node: ccm node name, e.g. "node1"
//
// Returns:
//   - error: If the ccm command failed
//
// Example:
//
//	if err := ccm.Pause("node1"); err != nil { ... }
//	defer ccm.Resume("node1")
func Pause(node string) error {
	_, err := execCmd(node, "pause")
	return err
}

// Resume continues a paused node's Cassandra process with SIGCONT.
//
// Parameters:
//   - node: ccm node name, e.g. "node1"
//
// Returns:
//   - error: If the ccm command failed
func Resume(node string) error {
	_, err := execCmd(node, "resume")
	return err
}

// configDir returns the ccm configuration directory.
//
// Returns:
//   - string: $CCM_CONFIG_DIR when set, otherwise ~/.ccm
func configDir() (string, error) {
	if dir := os.Getenv("CCM_CONFIG_DIR"); dir != "" {
		return dir, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locate home directory: %w", err)
	}
	return filepath.Join(home, ".ccm"), nil
}

// nodeDir returns the directory ccm keeps node's configuration in.
//
// Returns:
//   - string: <config dir>/<active cluster>/<node>
func nodeDir(node string) (string, error) {
	dir, err := configDir()
	if err != nil {
		return "", err
	}

	// ccm records the active cluster's name in CURRENT.
	current, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		return "", fmt.Errorf("read the active ccm cluster: %w", err)
	}

	return filepath.Join(dir, strings.TrimSpace(string(current)), node), nil
}

// SetNodeAddress moves node to ip and restarts it.
//
// The data directory is left alone, so the node keeps its host_id and its
// tokens and rejoins as itself at a new address - the shape a pod recycled onto
// a new IP has, and the one issue #1884 is about.
//
// ccm has no command for this: the address lives in the node's cassandra.yaml,
// which Cassandra reads, and again in ccm's own node.conf, which ccm reads to
// find the node afterwards. Both have to move together or the next ccm command
// talks to the old address.
//
// Parameters:
//   - node: ccm node name, e.g. "node3"
//   - ip: the address to move it to, e.g. "127.0.0.4"
//
// Returns:
//   - error: if the node could not be stopped, rewritten, or started again
func SetNodeAddress(node, ip string) error {
	dir, err := nodeDir(node)
	if err != nil {
		return err
	}

	yamlPath := filepath.Join(dir, "conf", "cassandra.yaml")
	oldIP, err := listenAddress(yamlPath)
	if err != nil {
		return err
	}
	if oldIP == ip {
		return nil
	}

	if err := NodeDown(node); err != nil {
		return fmt.Errorf("stop %s: %w", node, err)
	}

	if err := replaceInFile(yamlPath, func(line string) string {
		for _, key := range []string{"listen_address", "rpc_address"} {
			if strings.HasPrefix(line, key+":") {
				return key + ": " + ip
			}
		}
		return line
	}); err != nil {
		return fmt.Errorf("rewrite %s: %w", yamlPath, err)
	}

	// node.conf lists the address twice, under interfaces.binary and
	// interfaces.storage, each as a bare sequence entry.
	confPath := filepath.Join(dir, "node.conf")
	if err := replaceInFile(confPath, func(line string) string {
		if strings.TrimSpace(line) == "- "+oldIP {
			return strings.Replace(line, oldIP, ip, 1)
		}
		return line
	}); err != nil {
		return fmt.Errorf("rewrite %s: %w", confPath, err)
	}

	if err := NodeUp(node); err != nil {
		return fmt.Errorf("start %s at %s: %w", node, ip, err)
	}
	return nil
}

// listenAddress reads listen_address out of a cassandra.yaml.
//
// Returns:
//   - string: the configured address
func listenAddress(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}

	for _, line := range strings.Split(string(content), "\n") {
		if strings.HasPrefix(line, "listen_address:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "listen_address:")), nil
		}
	}
	return "", fmt.Errorf("no listen_address in %s", path)
}

// replaceInFile rewrites path, passing every line through replace.
func replaceInFile(path string, replace func(string) string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	lines := strings.Split(string(content), "\n")
	for i, line := range lines {
		lines[i] = replace(line)
	}

	return os.WriteFile(path, []byte(strings.Join(lines, "\n")), info.Mode())
}

func AddNode(name, ip string, jmxPort int) error {
	_, err := execCmd("add", name, "-i", ip, "-j", strconv.Itoa(jmxPort), "-d", "datacenter1")
	return err
}

func DecommissionNode(node string) error {
	_, err := execCmd(node, "decommission")
	return err
}

func RemoveNode(node string) error {
	_, err := execCmd(node, "remove")
	return err
}

type Host struct {
	State NodeState
	Addr  string
	Name  string
}

type NodeState int

func (n NodeState) String() string {
	if n == NodeStateUp {
		return "UP"
	} else if n == NodeStateDown {
		return "DOWN"
	} else {
		return fmt.Sprintf("UNKNOWN_STATE_%d", n)
	}
}

func (n NodeState) IsUp() bool {
	return n == NodeStateUp
}

const (
	NodeStateUp NodeState = iota
	NodeStateDown
)

func Status() (map[string]Host, error) {
	// TODO: parse into struct to manipulate
	out, err := execCmd("status", "-v")
	if err != nil {
		return nil, err
	}

	const (
		stateCluster = iota
		stateCommas
		stateNode
		stateOption
	)

	nodes := make(map[string]Host)
	// didnt really want to write a full state machine parser
	state := stateCluster
	sc := bufio.NewScanner(out)

	var host Host

	for sc.Scan() {
		switch state {
		case stateCluster:
			text := sc.Text()
			if !strings.HasPrefix(text, "Cluster:") {
				return nil, fmt.Errorf("expected 'Cluster:' got %q", text)
			}
			state = stateCommas
		case stateCommas:
			text := sc.Text()
			if !strings.HasPrefix(text, "-") {
				return nil, fmt.Errorf("expected commas got %q", text)
			}
			state = stateNode
		case stateNode:
			// assume nodes start with node
			text := sc.Text()
			if !strings.HasPrefix(text, "node") {
				return nil, fmt.Errorf("expected 'node' got %q", text)
			}
			line := strings.Split(text, ":")
			host.Name = line[0]

			nodeState := strings.TrimSpace(line[1])
			switch nodeState {
			case "UP":
				host.State = NodeStateUp
			case "DOWN":
				host.State = NodeStateDown
			case "DOWN (Not initialized)":
				host.State = NodeStateDown
				// could be more specific and have a separate state for this, but for our purposes its just down
				// and this is the only other state we know of that ccm produces
			default:
				return nil, fmt.Errorf("unknown node state from ccm: %q", nodeState)
			}

			state = stateOption
		case stateOption:
			text := sc.Text()
			if text == "" {
				state = stateNode
				nodes[host.Name] = host
				host = Host{}
				continue
			}

			line := strings.Split(strings.TrimSpace(text), "=")
			k, v := line[0], line[1]
			if k == "binary" {
				// could check errors
				// ('127.0.0.1', 9042)
				v = v[2:] // (''
				if i := strings.IndexByte(v, '\''); i < 0 {
					return nil, fmt.Errorf("invalid binary v=%q", v)
				} else {
					host.Addr = v[:i]
					// dont need port
				}
			}
		default:
			return nil, fmt.Errorf("unexpected state: %q", state)
		}
	}

	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("unable to parse ccm status: %v", err)
	}

	return nodes, nil
}

func Hosts() ([]Host, error) {
	status, err := Status()
	if err != nil {
		return nil, err
	}

	hosts := make([]Host, 0, len(status))
	for _, host := range status {
		hosts = append(hosts, host)
	}

	return hosts, nil
}

type ClusterInfo struct {
	Hosts []Host
}

func (c *ClusterInfo) HostAddrs() []string {
	addrs := make([]string, 0, len(c.Hosts))
	for _, host := range c.Hosts {
		addrs = append(addrs, host.Addr)
	}
	return addrs
}

// CurrentClusterInfo returns the current cluster information by running ccm status -v.
// It assumes that name of each node in the cluster starts with "node" prefix (e.g. node1, node2, etc)
func CurrentClusterInfo() (*ClusterInfo, error) {
	hosts, err := Hosts()
	if err != nil {
		return nil, err
	}

	if len(hosts) < 1 {
		return nil, fmt.Errorf("no nodes in cluster")
	}

	cluster := &ClusterInfo{
		Hosts: hosts,
	}

	return cluster, nil
}
