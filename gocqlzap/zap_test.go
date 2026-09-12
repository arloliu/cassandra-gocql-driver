//go:build all || unit
// +build all unit

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

package gocqlzap

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const logLineEnding = "%%%\n%%%"

func NewCustomLogger(pipeTo io.Writer) zapcore.Core {
	cfg := zap.NewProductionEncoderConfig()
	cfg.LineEnding = logLineEnding
	return zapcore.NewCore(
		zapcore.NewConsoleEncoder(cfg),
		zapcore.AddSync(pipeTo),
		zapcore.DebugLevel,
	)
}

func TestGocqlZapLog(t *testing.T) {
	b := &bytes.Buffer{}
	logger := zap.New(NewCustomLogger(b))
	clusterCfg := gocql.NewCluster("0.0.0.1")
	clusterCfg.Logger = NewZapLogger(logger)
	clusterCfg.ProtoVersion = 4
	// 0.0.0.1 never answers, so the dial has to fail before the log line this
	// test reads can exist. The default ConnectTimeout is 11 seconds and the
	// dial is what spends it; a strictly positive millisecond keeps the same
	// failure and the same log line without the wait. It must stay positive:
	// zero means "no deadline" to net.Dialer, not "give up at once".
	clusterCfg.ConnectTimeout = time.Millisecond
	session, err := clusterCfg.CreateSession()
	if err == nil {
		session.Close()
		t.Fatal("expected error creating session")
	}
	err = logger.Sync()
	if err != nil {
		t.Fatal("logger sync failed")
	}
	logOutput := strings.Split(b.String(), logLineEnding)
	found := false
	for _, logEntry := range logOutput {
		if len(logEntry) == 0 {
			continue
		}
		if !strings.Contains(logEntry, "info\tgocql\tControl connection failed to establish a connection to host.\t{\"host_addr\": "+
			"\"0.0.0.1\", \"port\": 9042, \"host_id\": \"\", \"err\": \"dial tcp 0.0.0.1:9042:") {
			continue
		} else {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("log output didn't match expectations: ", strings.Join(logOutput, "\n"))
	}
}
