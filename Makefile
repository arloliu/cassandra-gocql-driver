SHELL := bash
MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
KEY_PATH = ${MAKEFILE_PATH}/testdata/pki

CASSANDRA_VERSION ?= 4.1.6
TEST_CQL_PROTOCOL ?= 4
TEST_COMPRESSOR ?= no-compression
TEST_INTEGRATION_TAGS ?= integration
# Integration runs drive real Cassandra nodes through ccm; stopping and starting
# one costs tens of seconds, so the ccm-tagged suite needs more than the Go
# default. Override for a longer sweep.
TEST_TIMEOUT ?= 10m

# TEST_OPTS is forwarded verbatim to `go test`, ahead of the custom test-binary
# flags. It may carry only flags `go test` itself recognises - -run, -count, -v,
# -timeout and the like.
#
# It must NOT carry -tags, or any other option that changes which packages or
# files are selected. -tags is a flag `go test` recognises, so "only go's own
# flags" does not exclude it on its own: TEST_OPTS lands after each recipe's
# own -tags, would override it, and check-test-selection would then be auditing
# a different set of tags from the one actually run.

CCM_VERSION ?= 39b8222b31a6c7afe8fe845d16981088a5a735ad
GOLANGCI_VERSION = v2.1.6
JVM_EXTRA_OPTS ?= -Dcassandra.test.fail_writes_ks=test -Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler
ifeq (${CCM_CONFIG_DIR},)
	CCM_CONFIG_DIR = ~/.ccm
endif
CCM_CONFIG_DIR := $(shell readlink --canonicalize ${CCM_CONFIG_DIR})

# Project-local CCM install. Keeps Python deps out of the system site-packages
# and sidesteps PEP 668 ("externally-managed-environment") on Debian/Ubuntu.
CCM_VENV := ${MAKEFILE_PATH}/.ccm-venv
CCM_PYTHON := python3
CCM_PIP := ${CCM_VENV}/bin/pip
CCM_STAMP := ${CCM_VENV}/.installed-${CCM_VERSION}

# Make the venv's `ccm` visible to every recipe (including the $(ccm liveset)
# subshells used by test-integration). Targets that need CCM still depend on
# .prepare-ccm so the venv is created before first use.
export PATH := ${CCM_VENV}/bin:${PATH}

CASSANDRA_CONFIG ?= "client_encryption_options.enabled: true" \
"client_encryption_options.keystore: ${KEY_PATH}/.keystore" \
"client_encryption_options.keystore_password: cassandra" \
"client_encryption_options.require_client_auth: true" \
"client_encryption_options.truststore: ${KEY_PATH}/.truststore" \
"client_encryption_options.truststore_password: cassandra" \
"concurrent_reads: 2" \
"concurrent_writes: 2" \
"write_request_timeout_in_ms: 5000" \
"read_request_timeout_in_ms: 5000"

ifeq ($(shell echo "${CASSANDRA_VERSION}" | grep -oP "3\.[0-9]+\.[0-9]+" ),${CASSANDRA_VERSION})
	CASSANDRA_CONFIG += "rpc_server_type: sync" \
"rpc_min_threads: 2" \
"rpc_max_threads: 2" \
"enable_user_defined_functions: true" \
"enable_materialized_views: true"
else ifeq ($(shell echo "${CASSANDRA_VERSION}" | grep -oP "4\.0\.[0-9]+" ),${CASSANDRA_VERSION})
	CASSANDRA_CONFIG +=	"enable_user_defined_functions: true" \
"enable_materialized_views: true"
else
	CASSANDRA_CONFIG += "user_defined_functions_enabled: true" \
"materialized_views_enabled: true"
endif

ifneq (${JAVA11_HOME},)
	export JAVA11_HOME
endif

ifneq (${JAVA17_HOME},)
	export JAVA17_HOME
endif

# Derive JAVA_HOME from `java` on PATH if the caller didn't provide one. Only
# accept versions Cassandra 3.x–5.x supports (11 / 17); anything else falls
# through to the SDKMAN-based install-java target.
ifeq ($(strip ${JAVA_HOME}),)
JAVA_HOME := $(shell command -v java >/dev/null 2>&1 && java -version 2>&1 | grep -qE '"(11|17)\.' && readlink -f $$(command -v java) 2>/dev/null | sed 's:/bin/java$$::')
endif

ifneq (${JAVA_HOME},)
	export JAVA_HOME
endif

export JVM_EXTRA_OPTS

# CHECK_INTEGRATION_TAGS rejects a TEST_INTEGRATION_TAGS value outside the sets
# check_test_selection.sh audits: an unaudited combination could select a file or
# package no lane covers, which is the failure that check exists to prevent.
#
# It is inlined as the first line of every recipe that can act on the value
# rather than made a prerequisite, because `make -j` runs prerequisites in
# parallel - a prerequisite would let cluster preparation start alongside it.
CHECK_INTEGRATION_TAGS = shopt -u nocasematch; \
	case "$(strip ${TEST_INTEGRATION_TAGS})" in \
		"integration"|"cassandra"|"ccm"|"ccm ccmtopology") ;; \
		*) \
			echo "TEST_INTEGRATION_TAGS=\"${TEST_INTEGRATION_TAGS}\" is not one of the audited tag sets." >&2; \
			echo "Supported: integration | cassandra | ccm | \"ccm ccmtopology\"" >&2; \
			echo "To add one, extend LANES in test_lanes.sh and this list together." >&2; \
			exit 1 ;; \
	esac

.prepare-cassandra-cluster: .prepare-ccm .prepare-java
	@$(CHECK_INTEGRATION_TAGS)
	@if [ -d ${CCM_CONFIG_DIR}/gocql_integration_test ] && ccm switch gocql_integration_test 2>/dev/null 1>&2 && ccm status | grep UP 2>/dev/null 1>&2; then \
		echo "Cassandra cluster is already started"; \
  	else \
		echo "Start cassandra ${CASSANDRA_VERSION} cluster"; \
		ccm stop gocql_integration_test 2>/dev/null 1>&2 || true; \
		ccm remove gocql_integration_test 2>/dev/null 1>&2 || true; \
		rm -rf ${CCM_CONFIG_DIR}/gocql_integration_test || true; \
		ccm create gocql_integration_test -v ${CASSANDRA_VERSION} -n 3 -d --vnodes --jvm_arg="-Xmx256m -XX:NewSize=100m" && \
		ccm updateconf ${CASSANDRA_CONFIG} && \
		ccm start --wait-for-binary-proto --verbose && \
		ccm status && \
		ccm node1 nodetool status; \
	fi

cassandra-start: .prepare-ccm .prepare-java
	@echo "Start cassandra ${CASSANDRA_VERSION} cluster"
	@ccm stop gocql_integration_test 2>/dev/null 1>&2 || true
	@ccm remove gocql_integration_test 2>/dev/null 1>&2 || true
	@rm -rf ${CCM_CONFIG_DIR}/gocql_integration_test || true
	ccm create gocql_integration_test -v ${CASSANDRA_VERSION} -n 3 -d --vnodes --jvm_arg="-Xmx256m -XX:NewSize=100m"
	@ccm updateconf ${CASSANDRA_CONFIG}
	@ccm start --wait-for-binary-proto --verbose
	@ccm status
	@ccm node1 nodetool status

cassandra-stop: .prepare-ccm
	@echo "Stop cassandra cluster"
	@ccm stop gocql_integration_test 2>/dev/null 1>&2 || true

cassandra-remove: .prepare-ccm
	@echo "Remove cassandra cluster"
	@ccm remove gocql_integration_test 2>/dev/null 1>&2 || true
	@rm -rf ${CCM_CONFIG_DIR}/gocql_integration_test || true

# The integration recipes name the package "." before any custom test-binary
# flag, and both positions matter.
#
# `go test`'s grammar is [flags] [packages] [flags & test-binary flags]: once an
# unknown flag such as -proto appears, everything after it is handed to the test
# binary, so a package pattern written after those flags is silently treated as a
# test-binary argument and ignored. That is what these recipes used to do - the
# trailing ./... never selected anything, a mistyped path was accepted in
# silence, and only the current directory was ever compiled.
#
# "." rather than "./..." keeps that de facto behaviour, deliberately:
# internal/ccm does not register this binary's custom flags, so a working ./...
# would fail with "flag provided but not defined: -proto" before running any of
# that package's tests. check-test-selection guards the assumption that nothing
# else needs selecting.
test-integration: .prepare-cassandra-cluster
	@$(CHECK_INTEGRATION_TAGS)
	@echo "Run integration tests for proto ${TEST_CQL_PROTOCOL} on cassandra ${CASSANDRA_VERSION}"
	go test -v -tags "${TEST_INTEGRATION_TAGS} gocql_debug" -timeout=${TEST_TIMEOUT} . ${TEST_OPTS} -proto=${TEST_CQL_PROTOCOL} -gocql.timeout=60s -runssl -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=${CASSANDRA_VERSION} -cluster=$$(ccm liveset)

test-integration-auth: .prepare-cassandra-cluster
	@$(CHECK_INTEGRATION_TAGS)
	@echo "Run auth integration tests for proto ${TEST_CQL_PROTOCOL} on cassandra ${CASSANDRA_VERSION}"
	go test -v -run=TestAuthentication -tags "${TEST_INTEGRATION_TAGS} gocql_debug" -timeout=${TEST_TIMEOUT} . -proto=${TEST_CQL_PROTOCOL} -gocql.timeout=60s -runssl -runauth -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=${CASSANDRA_VERSION} -cluster=$$(ccm liveset)

test-cassandra: .prepare-cassandra-cluster
	@echo "Run cassandra-tagged tests for proto ${TEST_CQL_PROTOCOL} on cassandra ${CASSANDRA_VERSION}"
	go test -v -tags "cassandra gocql_debug" -timeout=${TEST_TIMEOUT} . ${TEST_OPTS} -proto=${TEST_CQL_PROTOCOL} -gocql.timeout=60s -runssl -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=${CASSANDRA_VERSION} -cluster=$$(ccm liveset)

# test-ccm and test-ccmtopology are aliases over the parameterised
# test-integration target: TEST_INTEGRATION_TAGS is what selects a lane, and the
# ccm-tagged tests already run in CI through that variable. The aliases exist so
# a local run does not have to remember the tag spelling.
test-ccm:
	@$(MAKE) test-integration TEST_INTEGRATION_TAGS="ccm"

# ccmtopology restarts a node under a new address. It is destructive to the
# shared local cluster and an interrupted run leaves the node moved, so confirm
# the cluster can be rebuilt before running it. See the invocation notes in
# rejoin_new_ip_ccm_test.go.
#
# TEST_OPTS replaces the default -run filter rather than adding to it, so pass
# the filter back yourself if you override it:
#   make test-ccmtopology TEST_OPTS="-run TestRejoinWithNewAddress -count=2"
test-ccmtopology:
	@$(MAKE) test-integration TEST_INTEGRATION_TAGS="ccm ccmtopology" \
		TEST_OPTS="$(or ${TEST_OPTS},-run TestRejoinWithNewAddress)"

test-unit:
	@echo "Run unit tests"
	@go clean -testcache
	go test -v -tags unit -timeout=5m -race ./...

# test-unit-fast is the inner-loop lane: same selection as test-unit without the
# race detector. CI keeps running test-unit, so -race coverage is not lost.
test-unit-fast:
	@echo "Run unit tests without the race detector"
	@go clean -testcache
	go test -v -tags unit -timeout=5m ./...

# check-test-selection proves two things about test *selection*, and nothing
# about whether the selected tests assert the right things:
#
#   A. every *_test.go in this module is selected by at least one lane, so a
#      misspelt or wrong build tag cannot hide a file from every lane at once;
#   B. internal/ccm is still the only non-root package holding integration-tagged
#      tests, so nothing new compiles into a lane that would never run it.
#
# `go vet -tags <tag> ./...` cannot stand in for A: vet only checks the files a
# lane already selected, so a file no tag selects is invisible to all of them.
check-test-selection:
	@./check_test_selection.sh

# check-vet-lanes runs `go vet` once per lane, over ./... . It proves the
# narrower thing vet can prove - that the files each lane SELECTS still compile
# and pass vet - and nothing about whether a selected test executes or asserts
# the right thing. It does not replace check-test-selection, which is what
# catches a file no lane selects at all; vet cannot see one.
#
# It is a gate rather than a note telling a human to run it because the
# `ccm ccmtopology gocql_debug` lane was compiled nowhere in CI, and because a
# compile error under `ccm` would otherwise surface only after the integration
# job has built a cluster, minutes into a 15-minute timeout, instead of in the
# build job's first second. Running in the build job, it now compiles the
# ccmtopology lane in CI; nothing executes it.
#
# The whole sweep takes under half a second.
check-vet-lanes:
	@./check_vet_lanes.sh

# check-test-selection-cases pins the escapes earlier versions of that check let
# through. It creates fixtures in the working tree and removes them again, so it
# is deliberately not part of `check`, which must not mutate anything. Run it on
# a clean tree after changing check_test_selection.sh.
check-test-selection-cases:
	@./check_test_selection_cases.sh

check: .prepare-golangci check-test-selection check-vet-lanes
	@echo "Build"
	@go build -tags all .
	@echo "Check linting"
	@golangci-lint run

fix: .prepare-golangci
	@echo "Fix linting"
	golangci-lint run --fix

.prepare-java:
ifneq (${JAVA_HOME},)
	@echo "Using JAVA_HOME=${JAVA_HOME}"
else ifeq ($(shell if [ -f ~/.sdkman/bin/sdkman-init.sh ]; then echo "installed"; else echo "not-installed"; fi), not-installed)
	@$(MAKE) install-java
	@echo "Java installed via SDKMAN. Re-source your shell (or set JAVA_HOME) before re-running make."
	@exit 1
else
	@echo "JAVA_HOME is not set and could not be auto-detected. Source ~/.sdkman/bin/sdkman-init.sh or set JAVA_HOME explicitly."
	@exit 1
endif

install-java:
	@echo "Installing SDKMAN..."
	@curl -s "https://get.sdkman.io" | bash
	@echo "sdkman_auto_answer=true" >> ~/.sdkman/etc/config
	@( \
		source ~/.sdkman/bin/sdkman-init.sh; \
		export PATH=${PATH}:~/.sdkman/bin; \
		echo "Installing Java versions..."; \
		sdk install java 11.0.24-zulu; \
		sdk install java 17.0.12-zulu; \
		sdk default java 11.0.24-zulu; \
		sdk use java 11.0.24-zulu; \
		if [[ -n "${GITHUB_ENV}" ]]; then \
			echo "JAVA11_HOME=$$JAVA_HOME_11_X64" >> ${GITHUB_ENV}; \
    		echo "JAVA17_HOME=$$JAVA_HOME_17_X64" >> ${GITHUB_ENV}; \
    		echo "JAVA_HOME=$$JAVA_HOME_11_X64" >> ${GITHUB_ENV}; \
    		echo "$$PATH" > ${GITHUB_PATH}; \
		fi; \
	)

.prepare-ccm: ${CCM_STAMP}

${CCM_STAMP}:
	@if [ ! -x ${CCM_VENV}/bin/python ]; then \
		echo "Creating CCM venv at ${CCM_VENV}"; \
		${CCM_PYTHON} -m venv ${CCM_VENV}; \
	fi
	@echo "Installing CCM ${CCM_VERSION}"
	@rm -f ${CCM_VENV}/.installed-*
	@${CCM_PIP} install -q --upgrade pip
	@${CCM_PIP} install -q "setuptools<81"
	@${CCM_PIP} install -q "git+https://github.com/riptano/ccm.git@${CCM_VERSION}"
	@mkdir -p ${CCM_CONFIG_DIR}
	@echo ${CCM_VERSION} > ${CCM_CONFIG_DIR}/ccm-version
	@touch ${CCM_STAMP}

install-ccm:
	@rm -rf ${CCM_VENV}
	@$(MAKE) .prepare-ccm

.prepare-golangci:
	@if ! golangci-lint --version 2>/dev/null | grep ${GOLANGCI_VERSION} >/dev/null; then \
  		echo "Installing golangci-ling ${GOLANGCI_VERSION}"; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_VERSION}; \
  	fi

# Every target here is a command, not a file. Without this a file named e.g.
# `check-test-selection` in the working tree would make `make check` consider the
# check up to date and skip it silently.
.PHONY: check check-test-selection check-test-selection-cases check-vet-lanes fix test-unit test-unit-fast test-integration \
	test-integration-auth test-cassandra test-ccm test-ccmtopology \
	cassandra-start cassandra-stop cassandra-remove install-java install-ccm \
	.prepare-ccm .prepare-java .prepare-cassandra-cluster .prepare-golangci
