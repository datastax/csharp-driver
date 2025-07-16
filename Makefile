SHELL := bash

MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
SCYLLA_VERSION ?= release:2025.2

CCM_CASSANDRA_REPO ?= github.com/apache/cassandra-ccm
CCM_CASSANDRA_VERSION ?= d3225ac6565242b231129e0c4f8f0b7a041219cf

CCM_SCYLLA_REPO ?= github.com/scylladb/scylla-ccm
CCM_SCYLLA_VERSION ?= master

SCYLLA_EXT_OPTS ?= --smp=2 --memory=4G
SIMULACRON_PATH ?= ${MAKEFILE_PATH}/ci/simulacron-standalone-0.12.0.jar

ifeq (${CCM_CONFIG_DIR},)
	CCM_CONFIG_DIR = ~/.ccm
endif
CCM_CONFIG_DIR := $(shell readlink --canonicalize ${CCM_CONFIG_DIR})

export SCYLLA_EXT_OPTS
export SIMULACRON_PATH
export SCYLLA_VERSION

check: .prepare-mono
	dotnet format --verify-no-changes --severity warn --verbosity diagnostic src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj
	dotnet format --verify-no-changes --severity warn --verbosity diagnostic src/Cassandra/Cassandra.csproj
	dotnet format --verify-no-changes --severity warn --verbosity diagnostic src/Cassandra.Tests/Cassandra.Tests.csproj

fix: .prepare-mono
	dotnet format --severity warn --verbosity diagnostic src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj
	dotnet format --severity warn --verbosity diagnostic src/Cassandra/Cassandra.csproj
	dotnet format --severity warn --verbosity diagnostic src/Cassandra.Tests/Cassandra.Tests.csproj

test-unit: .use-development-snk .prepare-mono
	dotnet test src/Cassandra.Tests/Cassandra.Tests.csproj

test-integration-scylla: .use-development-snk .prepare-mono .prepare-scylla-ccm
	CCM_DISTRIBUTION=scylla dotnet test src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj -f net8 -l "console;verbosity=detailed" --filter "(FullyQualifiedName!~ClientWarningsTests & FullyQualifiedName!~CustomPayloadTests & FullyQualifiedName!~Connect_With_Ssl_Test & FullyQualifiedName!~Should_UpdateHosts_When_HostIpChanges & FullyQualifiedName!~Should_UseNewHostInQueryPlans_When_HostIsDecommissionedAndJoinsAgain & FullyQualifiedName!~Should_RemoveNodeMetricsAndDisposeMetricsContext_When_HostIsRemoved & FullyQualifiedName!~Virtual_Keyspaces_Are_Included & FullyQualifiedName!~Virtual_Table_Metadata_Test & FullyQualifiedName!~SessionAuthenticationTests & FullyQualifiedName!~TypeSerializersTests & FullyQualifiedName!~Custom_MetadataTest & FullyQualifiedName!~LinqWhere_WithVectors & FullyQualifiedName!~SimpleStatement_With_No_Compact_Enabled_Should_Reveal_Non_Schema_Columns & FullyQualifiedName!~SimpleStatement_With_No_Compact_Disabled_Should_Not_Reveal_Non_Schema_Columns & FullyQualifiedName!~ColumnClusteringOrderReversedTest & FullyQualifiedName!~GetMaterializedView_Should_Refresh_View_Metadata_Via_Events & FullyQualifiedName!~MaterializedView_Base_Table_Column_Addition & FullyQualifiedName!~MultipleSecondaryIndexTest & FullyQualifiedName!~RaiseErrorOnInvalidMultipleSecondaryIndexTest & FullyQualifiedName!~TableMetadataAllTypesTest & FullyQualifiedName!~TableMetadataClusteringOrderTest & FullyQualifiedName!~TableMetadataCollectionsSecondaryIndexTest & FullyQualifiedName!~TableMetadataCompositePartitionKeyTest & FullyQualifiedName!~TupleMetadataTest & FullyQualifiedName!~Udt_Case_Sensitive_Metadata_Test & FullyQualifiedName!~UdtMetadataTest & FullyQualifiedName!~Should_Retrieve_Table_Metadata & FullyQualifiedName!~CreateTable_With_Frozen_Key & FullyQualifiedName!~CreateTable_With_Frozen_Udt & FullyQualifiedName!~CreateTable_With_Frozen_Value & FullyQualifiedName!~Should_AllMetricsHaveValidValues_When_AllNodesAreUp & FullyQualifiedName!~SimpleStatement_Dictionary_Parameters_CaseInsensitivity_ExcessOfParams & FullyQualifiedName!~SimpleStatement_Dictionary_Parameters_CaseInsensitivity_NoOverload & FullyQualifiedName!~TokenAware_TransientReplication_NoHopsAndOnlyFullReplicas & FullyQualifiedName!~GetFunction_Should_Return_Most_Up_To_Date_Metadata_Via_Events & FullyQualifiedName!~LargeDataTests & FullyQualifiedName!~MetadataTests & FullyQualifiedName!~MultiThreadingTests & FullyQualifiedName!~PoolTests & FullyQualifiedName!~PrepareLongTests & FullyQualifiedName!~SpeculativeExecutionLongTests & FullyQualifiedName!~StressTests & FullyQualifiedName!~TransitionalAuthenticationTests & FullyQualifiedName!~ProxyAuthenticationTests & FullyQualifiedName!~SessionDseAuthenticationTests & FullyQualifiedName!~CloudIntegrationTests & FullyQualifiedName!~CoreGraphTests & FullyQualifiedName!~GraphTests & FullyQualifiedName!~InsightsIntegrationTests & FullyQualifiedName!~DateRangeTests & FullyQualifiedName!~FoundBugTests & FullyQualifiedName!~GeometryTests & FullyQualifiedName!~LoadBalancingPolicyTests & FullyQualifiedName!~ConsistencyTests & FullyQualifiedName!~LoadBalancingPolicyTests & FullyQualifiedName!~ReconnectionPolicyTests & FullyQualifiedName!~RetryPolicyTests)"

test-integration-cassandra: .use-development-snk .prepare-mono .prepare-cassandra-ccm
	CCM_DISTRIBUTION=cassandra dotnet test src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj -f net8 -l "console;verbosity=detailed"

.prepare-cassandra-ccm:
	@ccm --help 2>/dev/null 1>&2; if [[ $$? -lt 127 ]] && grep CASSANDRA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 && grep ${CCM_CASSANDRA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev//null  1>&2; then \
		echo "Cassandra CCM ${CCM_CASSANDRA_VERSION} is already installed"; \
  	else \
		echo "Installing Cassandra CCM ${CCM_CASSANDRA_VERSION}"; \
		pip install "git+https://${CCM_CASSANDRA_REPO}.git@${CCM_CASSANDRA_VERSION}"; \
		mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true; \
		echo CASSANDRA > ${CCM_CONFIG_DIR}/ccm-type; \
		echo ${CCM_CASSANDRA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version; \
  	fi

install-cassandra-ccm:
	@echo "Install CCM ${CCM_CASSANDRA_VERSION}"
	@pip install "git+https://${CCM_CASSANDRA_REPO}.git@${CCM_CASSANDRA_VERSION}"
	@mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	@echo CASSANDRA > ${CCM_CONFIG_DIR}/ccm-type
	@echo ${CCM_CASSANDRA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

.prepare-scylla-ccm:
	@ccm --help 2>/dev/null 1>&2; if [[ $$? -lt 127 ]] && grep SCYLLA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 && grep ${CCM_SCYLLA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev//null  1>&2; then \
		echo "Scylla CCM ${CCM_SCYLLA_VERSION} is already installed"; \
  	else \
		echo "Installing Scylla CCM ${CCM_SCYLLA_VERSION}"; \
		pip install "git+https://${CCM_SCYLLA_REPO}.git@${CCM_SCYLLA_VERSION}"; \
		mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true; \
		echo SCYLLA > ${CCM_CONFIG_DIR}/ccm-type; \
		echo ${CCM_SCYLLA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version; \
  	fi

install-scylla-ccm:
	@echo "Installing Scylla CCM ${CCM_SCYLLA_VERSION}"
	@pip install "git+https://${CCM_SCYLLA_REPO}.git@${CCM_SCYLLA_VERSION}"
	@mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	@echo SCYLLA > ${CCM_CONFIG_DIR}/ccm-type
	@echo ${CCM_SCYLLA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

.prepare-mono:
	@if mono --version 2>/dev/null 1>&2; then \
		echo "Mono is already installed"; \
	else \
		echo "Installing Mono"; \
		sudo apt update; \
		sudo apt install -y mono-complete; \
		mono --version; \
	fi

install-mono:
	sudo apt update
	sudo apt install -y mono-complete
	mono --version

.use-development-snk:
	@[ -f build/scylladb.snk ] || ( cp build/scylladb-dev.snk build/scylladb.snk )
