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

.prepare-cassandra-cluster: .prepare-ccm .prepare-java
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

test-integration: .prepare-cassandra-cluster
	@echo "Run integration tests for proto ${TEST_CQL_PROTOCOL} on cassandra ${CASSANDRA_VERSION}"
	go test -v ${TEST_OPTS} -tags "${TEST_INTEGRATION_TAGS} gocql_debug" -timeout=${TEST_TIMEOUT} -proto=${TEST_CQL_PROTOCOL} -gocql.timeout=60s -runssl -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=${CASSANDRA_VERSION} -cluster=$$(ccm liveset) ./...

test-integration-auth: .prepare-cassandra-cluster
	@echo "Run auth integration tests for proto ${TEST_CQL_PROTOCOL} on cassandra ${CASSANDRA_VERSION}"
	go test -v -run=TestAuthentication -tags "${TEST_INTEGRATION_TAGS} gocql_debug" -timeout=${TEST_TIMEOUT} -proto=${TEST_CQL_PROTOCOL} -gocql.timeout=60s -runssl -runauth -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=${CASSANDRA_VERSION} -cluster=$$(ccm liveset) ./...

test-cassandra: .prepare-cassandra-cluster
	@echo "Run cassandra-tagged tests for proto ${TEST_CQL_PROTOCOL} on cassandra ${CASSANDRA_VERSION}"
	go test -v ${TEST_OPTS} -tags "cassandra gocql_debug" -timeout=${TEST_TIMEOUT} -proto=${TEST_CQL_PROTOCOL} -gocql.timeout=60s -runssl -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=${CASSANDRA_VERSION} -cluster=$$(ccm liveset) ./...

test-unit:
	@echo "Run unit tests"
	@go clean -testcache
	go test -v -tags unit -timeout=5m -race ./...

check: .prepare-golangci
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

