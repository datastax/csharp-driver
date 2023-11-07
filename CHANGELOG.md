# ChangeLog - DataStax C# Driver

## 3.19.4

2023-11-07

### Bug fixes

*   [[CSHARP-1000](https://datastax-oss.atlassian.net/browse/CSHARP-1000)] Fix support for Astra custom domains

## 3.19.3

2023-08-31

### Bug fixes

*   [[CSHARP-995](https://datastax-oss.atlassian.net/browse/CSHARP-995)] Some Statement properties are not being applied in LINQ statements

## 3.19.2

2023-03-31

### Bug fixes

*   [[CSHARP-992](https://datastax-oss.atlassian.net/browse/CSHARP-992)] Support null writetime values in LINQ

## 3.19.1

2023-01-19

### Bug fixes

*   [[CSHARP-989](https://datastax-oss.atlassian.net/browse/CSHARP-989)] Fix writetime support in LINQ

## 3.19.0

2022-12-22

### Bug fixes

*   [[CSHARP-974](https://datastax-oss.atlassian.net/browse/CSHARP-974)] Race condition in Hashed Wheel Timer causes unhandled exception
*   [[CSHARP-981](https://datastax-oss.atlassian.net/browse/CSHARP-981)] Memory leak - Diposed Sessions are still referenced by the Cassandra.Cluster that created them
*   [[CSHARP-987](https://datastax-oss.atlassian.net/browse/CSHARP-987)] Race condition in control connection reconnection

### Improvements

*   [[CSHARP-977](https://datastax-oss.atlassian.net/browse/CSHARP-977)] Reduce memory allocations on BeConverter by renting arrays from ArrayPool
*   [[CSHARP-979](https://datastax-oss.atlassian.net/browse/CSHARP-979)] Improve  data reading performance by inlining hot methods in RecyclableMemoryStream
*   [[CSHARP-980](https://datastax-oss.atlassian.net/browse/CSHARP-980)] Improve data reading performance by avoiding using ThreadLocal<T> in the hot loop

### New Features

*   [[CSHARP-986](https://datastax-oss.atlassian.net/browse/CSHARP-986)] LINQ support for writetime\(\) function

## 3.18.0

2022-07-04

### Bug fixes

*   [[CSHARP-954](https://datastax-oss.atlassian.net/browse/CSHARP-954)] Nodes disconnecting can cause Next is null but it is not the tail exception
*   [[CSHARP-967](https://datastax-oss.atlassian.net/browse/CSHARP-967)] Driver does not work in an application published as a single file

### New Features

*   [[CSHARP-968](https://datastax-oss.atlassian.net/browse/CSHARP-968)] Provide easier way to deserialize ElementMap graph results

## 3.18.0-alpha1

2022-06-22

### Bug fixes

*   [[CSHARP-954](https://datastax-oss.atlassian.net/browse/CSHARP-954)] Nodes disconnecting can cause Next is null but it is not the tail exception

### New Features

*   [[CSHARP-968](https://datastax-oss.atlassian.net/browse/CSHARP-968)] Provide easier way to deserialize ElementMap graph results

## 3.17.1

2021-11-25

### Bug fixes

*   [[CSHARP-960](https://datastax-oss.atlassian.net/browse/CSHARP-960)] - Create table generation fails with turkish culture

## 3.17.0

2021-10-25

### Improvements

*   [[CSHARP-956](https://datastax-oss.atlassian.net/browse/CSHARP-956)] - Retrieve new data from the Astra metadata service when refreshing contact points
*   [[CSHARP-946](https://datastax-oss.atlassian.net/browse/CSHARP-946)] - Remove System.Threading.Tasks.Dataflow version restriction

### Bug fixes

*   [[CSHARP-958](https://datastax-oss.atlassian.net/browse/CSHARP-958)] - Invalid keyspace in cluster.Connect\(string\) causes leaked connections
*   [[CSHARP-957](https://datastax-oss.atlassian.net/browse/CSHARP-957)] - Unitialized cluster shutdown can leak a connection
*   [[CSHARP-945](https://datastax-oss.atlassian.net/browse/CSHARP-945)] - The driver isn't rejecting the DseV1 protocol version
*   [[CSHARP-879](https://datastax-oss.atlassian.net/browse/CSHARP-879)] - Control Connection doesn't reconnect

## 3.16.3

2021-03-12

### Improvements

*   [[CSHARP-949](https://datastax-oss.atlassian.net/browse/CSHARP-949)] - Expose UdtMap.AddPropertyMapping(PropertyInfo, string)

### Bug fixes

*   [[CSHARP-950](https://datastax-oss.atlassian.net/browse/CSHARP-950)] - AuthenticationException when connecting to Astra serverless

## 3.16.2

2021-01-20

### Bug fixes

*   [[CSHARP-947](https://datastax-oss.atlassian.net/browse/CSHARP-947)] - The driver should read broadcast_address instead of peer for system.local queries

## 3.16.1

2020-10-22

### Improvements

*   [[CSHARP-942](https://datastax-oss.atlassian.net/browse/CSHARP-942)] - Log control connection init failures

### Bug fixes

*   [[CSHARP-943](https://datastax-oss.atlassian.net/browse/CSHARP-943)] - Cluster.Connect() throws "ArgumentException 'The source argument contains duplicate keys.'"

## 3.16.0

2020-09-10

### Features

*   [[CSHARP-750](https://datastax-oss.atlassian.net/browse/CSHARP-750)] - Initial DataStax Graph (DSE 6.8) support on the unified C# driver

### Improvements

*   [[CSHARP-898](https://datastax-oss.atlassian.net/browse/CSHARP-898)] - Replace lz4net with K4os.Compression.LZ4 on net452
*   [[CSHARP-925](https://datastax-oss.atlassian.net/browse/CSHARP-925)] - Ignore credentials in secure connect bundle [DataStax Astra]
*   [[CSHARP-934](https://datastax-oss.atlassian.net/browse/CSHARP-934)] - Optimize TimeoutItem cleanup

### Bug fixes

*   [[CSHARP-902](https://datastax-oss.atlassian.net/browse/CSHARP-902)] - ProtocolEventDebouncer calls refresh keyspace for the same keyspace multiple times
*   [[CSHARP-906](https://datastax-oss.atlassian.net/browse/CSHARP-906)] - C# driver doesn't support DSE transitional authentication
*   [[CSHARP-907](https://datastax-oss.atlassian.net/browse/CSHARP-907)] - Insights CPU info not available in netcoreapp3.1 on Windows
*   [[CSHARP-908](https://datastax-oss.atlassian.net/browse/CSHARP-908)] - DateRangeSerializer precision issue
*   [[CSHARP-913](https://datastax-oss.atlassian.net/browse/CSHARP-913)] - Misleading log message: "contact point resolved to multiple addresses"

### Documentation

*   [[CSHARP-927](https://datastax-oss.atlassian.net/browse/CSHARP-927)] - Document Duration type

## 3.15.1

2020-07-27

### Improvements

*   [[CSHARP-926](https://datastax-oss.atlassian.net/browse/CSHARP-926)] - Remove explicit System.Net.Http reference

## 3.15.0

2020-05-12

### Features

*   [[CSHARP-871](https://datastax-oss.atlassian.net/browse/CSHARP-871)] - Add support for system.peers_v2
*   [[CSHARP-886](https://datastax-oss.atlassian.net/browse/CSHARP-886)] - Add beta version native protocol flag and ensure we have test coverage of v5
*   [[CSHARP-887](https://datastax-oss.atlassian.net/browse/CSHARP-887)] - Transient Replication Support
*   [[CSHARP-895](https://datastax-oss.atlassian.net/browse/CSHARP-895)] - Add Table.CreateAsync and Table.CreateIfNotExistsAsync
*   [[CSHARP-719](https://datastax-oss.atlassian.net/browse/CSHARP-719)] - Support LZ4 compression in .NET Core

### Improvements

*   [[CSHARP-664](https://datastax-oss.atlassian.net/browse/CSHARP-664)] - Use prepared statement result_metadata for execute row responses

### Bug fixes

*   [[CSHARP-659](https://datastax-oss.atlassian.net/browse/CSHARP-659)] - Query trace retrieval fails when started_at is null
*   [[CSHARP-884](https://datastax-oss.atlassian.net/browse/CSHARP-884)] - Race condition in the ControlConnection Dispose method can leak connections
*   [[CSHARP-894](https://datastax-oss.atlassian.net/browse/CSHARP-894)] - Batch Statements cause a warning on TokenMap due to null keyspace
*   [[CSHARP-896](https://datastax-oss.atlassian.net/browse/CSHARP-896)] - Invalid deserialization when paging a rowset and a schema change happens

### Documentation

*   [[CSHARP-812](https://datastax-oss.atlassian.net/browse/CSHARP-812)] - Add documentation about batch support for LINQ and Mapper
*   [[CSHARP-833](https://datastax-oss.atlassian.net/browse/CSHARP-833)] - KeyspaceMetadata.ExportAsString API docs incorrectly state that it exports the table creation statements
*   [[CSHARP-881](https://datastax-oss.atlassian.net/browse/CSHARP-881)] - Add section about ServerName and certificate name mismatches to SSL docs
*   [[CSHARP-882](https://datastax-oss.atlassian.net/browse/CSHARP-882)] - Add a section to the driver manual with information on Statements

## 3.14.0

2020-03-24

### Features

*   [[CSHARP-405](https://datastax-oss.atlassian.net/browse/CSHARP-405)] - Log exception when Cluster.Init() can not recover from
*   [[CSHARP-806](https://datastax-oss.atlassian.net/browse/CSHARP-806)] - Drop support for .NET Standard 1.5 and bump net45 to net452
*   [[CSHARP-819](https://datastax-oss.atlassian.net/browse/CSHARP-819)] - Add option to keep contact points unresolved and always re-resolve when there's total connectivity loss
*   [[CSHARP-829](https://datastax-oss.atlassian.net/browse/CSHARP-829)] - Refine connection errors for connecting to cloud instance that may have been parked
*   [[CSHARP-841](https://datastax-oss.atlassian.net/browse/CSHARP-841)] - Gracefully handle TCP backpressure
*   [[CSHARP-846](https://datastax-oss.atlassian.net/browse/CSHARP-846)] - Implement EverywhereReplicationStrategy and LocalReplicationStrategy
*   [[CSHARP-850](https://datastax-oss.atlassian.net/browse/CSHARP-850)] - Host distance should be a computed aggregate of all configured LBPs
*   [[CSHARP-876](https://datastax-oss.atlassian.net/browse/CSHARP-876)] - The builder should fail fast when no credentials are provided for Astra clusters
*   [[CSHARP-862](https://datastax-oss.atlassian.net/browse/CSHARP-862)] - Update System.Net.Http to fix vulnerabilities
*   [[CSHARP-863](https://datastax-oss.atlassian.net/browse/CSHARP-863)] - Update System.Net.Security to fix vulnerabilities

#### AppMetrics Extension

*   [[CSHARP-851](https://datastax-oss.atlassian.net/browse/CSHARP-851)] - HistogramReservoir values should reset periodically (Timer metrics)

### Bug fixes

*   [[CSHARP-696](https://datastax-oss.atlassian.net/browse/CSHARP-696)] - HostConnectionPool incorrectly logs reconnection message after warmup
*   [[CSHARP-697](https://datastax-oss.atlassian.net/browse/CSHARP-697)] - When socket.ConnectAsync() throws an error synchronously, SocketAsyncEventArgs is not disposed
*   [[CSHARP-710](https://datastax-oss.atlassian.net/browse/CSHARP-710)] - Mapper cannot deserialize IList properties
*   [[CSHARP-711](https://datastax-oss.atlassian.net/browse/CSHARP-711)] - Race condition: re-prepare on UP might not use the current keyspace
*   [[CSHARP-786](https://datastax-oss.atlassian.net/browse/CSHARP-786)] - Support NULL in collection serializer
*   [[CSHARP-798](https://datastax-oss.atlassian.net/browse/CSHARP-798)] - Handle prepared id mismatch when repreparing on the fly
*   [[CSHARP-801](https://datastax-oss.atlassian.net/browse/CSHARP-801)] - Exception on UDT => CLR mapping of collection property which has null value
*   [[CSHARP-816](https://datastax-oss.atlassian.net/browse/CSHARP-816)] - "Local datacenter is not specified" message is logged if user specifies it with the default execution profile
*   [[CSHARP-839](https://datastax-oss.atlassian.net/browse/CSHARP-839)] - Mapper and Linq2Cql causes re-prepare warnings in scenarios with high concurrency
*   [[CSHARP-845](https://datastax-oss.atlassian.net/browse/CSHARP-845)] - When socket.ConnectAsync returns synchronously, driver assumes that it is connected
*   [[CSHARP-875](https://datastax-oss.atlassian.net/browse/CSHARP-875)] - Duplicate contact points cause cluster initialization failure
*   [[CSHARP-877](https://datastax-oss.atlassian.net/browse/CSHARP-877)] - NodeMetric.DefaultNodeMetrics and SessionMetric.DefaultSessionMetrics contain null values
*   [[CSHARP-878](https://datastax-oss.atlassian.net/browse/CSHARP-878)] - ControlConnection attempts to connect to DOWN nodes

### Documentation

*   [[CSHARP-489](https://datastax-oss.atlassian.net/browse/CSHARP-489)] - Use docfx or doxygen to generate API docs
*   [[CSHARP-847](https://datastax-oss.atlassian.net/browse/CSHARP-847)] - Doc: include a note about concurrent schema modifications

## 3.13.0

2020-01-15

### Features

*   [[CSHARP-791](https://datastax-oss.atlassian.net/browse/CSHARP-791)] - Unified Driver
*   [[CSHARP-840](https://datastax-oss.atlassian.net/browse/CSHARP-840)] - Linq2Cql and Mapper should generate CQL statements in a deterministic way

### Bug fixes

*   [[CSHARP-837](https://datastax-oss.atlassian.net/browse/CSHARP-837)] - Serializer protocol version is changed after init, causing current active connections to fail
*   [[CSHARP-835](https://datastax-oss.atlassian.net/browse/CSHARP-835)] - BatchStatement error message uses short.MaxValue instead of ushort.MaxValue
*   [[CSHARP-832](https://datastax-oss.atlassian.net/browse/CSHARP-832)] - Some graph related classes depend on the current culture

## CassandraCSharpDriver.AppMetrics 1.0.1

2019-11-11

### Bug fixes

*   [[CSHARP-817](https://datastax-oss.atlassian.net/browse/CSHARP-817)] - App.Metrics snapshot fails when timer metrics don't have any recorded values

## 3.12.0

2019-10-30

### Features

*   [[CSHARP-685](https://datastax-oss.atlassian.net/browse/CSHARP-685)] - Expose metrics
*   [[CSHARP-754](https://datastax-oss.atlassian.net/browse/CSHARP-754)] - DataStax Astra support
*   [[CSHARP-595](https://datastax-oss.atlassian.net/browse/CSHARP-595)] - DcAwareLoadBalancingPolicy: Warn when the local datacenter is not specified
*   [[CSHARP-788](https://datastax-oss.atlassian.net/browse/CSHARP-788)] - Add list of reserved keywords and add double quotes when they are used as identifiers
*   [[CSHARP-802](https://datastax-oss.atlassian.net/browse/CSHARP-802)] - Session.Warmup should mark host as down if no connection can be opened to that host

### Bug fixes

*   [[CSHARP-807](https://datastax-oss.atlassian.net/browse/CSHARP-807)] - Exception isn't logged when an exception is thrown while parsing a host token

## 3.11.0

2019-07-22

### Features

- [CSHARP-756] - ControlConnection init: Defer host map creation until system tables have been queried
- [CSHARP-757] - Include host_id in host metadata
- [CSHARP-779] - Separate socket endpoint from the host address using an endpoint resolver

### Bug Fixes

- [CSHARP-778] - NullReferenceException at Cassandra.Data.Linq.CqlExpressionVisitor.GetPropertyValue(MemberExpression node)
- [CSHARP-781] - Linq2Cql GetTable() generates invalid create table when more than one option is specified
- [CSHARP-784] - Driver is unable to correctly reestablish connection with previously decommissioned node

## 3.10.1

2019-06-17

### Bug Fixes

- [CSHARP-772] - Remove internal conversion of timestmap to DateTimeOffset
- [CSHARP-777] - Invalid or unsupported protocol version (0)

## 3.10.0

2019-05-21

### Features

- [CSHARP-275] - Prevent duplicate metadata fetches from control connection and allow disabling schema metadata fetching
- [CSHARP-763] - Introduce Execution Profiles in Mapper and LINQ APIs
- [CSHARP-678] - Introduce Execution Profiles at Session Level
- [CSHARP-424] - Allow UDT mappings to be defined for different keyspaces

### Bug Fixes

- [CSHARP-744] - LINQ and Mapper don't support empty column names
- [CSHARP-766] - Cassandra Date is not mapped correct to LocalDate with some specfic values

## 3.9.0

2019-04-02

### Features

- [CSHARP-538] - Expose Metadata.CheckSchemaAgreement() and ExecutionInfo.IsSchemaInAgreement()
- [CSHARP-618] - Add client configuration information to STARTUP message
- [CSHARP-725] - Deprecate usedHostsPerRemoteDc in DCAwareRoundRobinPolicy

### Bug Fixes

- [CSHARP-708] - Retry on current host should be made on a different connection
- [CSHARP-715] - LocalTime Parse fails for valid LocalTime strings on certain cultures
- [CSHARP-752] - Memory leak in ControlConnection

## 3.8.0

2019-02-11

### Features

- [CSHARP-726] - Improvements to token map building process

## 3.7.0

2018-11-26

### Features

- [CSHARP-705] - Provide a means of sending query to a specific node to facilitate virtual table queries
- [CSHARP-706] - Parse Virtual Keyspace Metadata

### Bug Fixes

- [CSHARP-709] - Rarely occurring concurrency bug in the HashedWheelTimer class

## 3.6.0

2018-06-18

### Features

- [CSHARP-591] - EC2 multi-region address resolution policy
- [CSHARP-625] - Mark DowngradingConsistencyRetryPolicy as deprecated
- [CSHARP-634] - Use system.peers in protocol negotiation
- [CSHARP-669] - Support pool warmup on initialization and enable it by default
- [CSHARP-680] - Use OPTIONS message for heartbeats instead of 'select key from system.local'
- [CSHARP-335] - RowSet: Support concurrent asynchronous calls to fetch next page
- [CSHARP-681] - Log driver version on Connect

### Bug Fixes

- [CSHARP-631] - BatchStatement: Use routing key from first statement
- [CSHARP-660] - Linq: StatementFactory shouldn't cache failed prepared statements
- [CSHARP-667] - Mapper: Statement factory cache should use keyspace to uniquely identify the query
- [CSHARP-691] - Sync completion of socket.ConnectAsync() is not considered

## 3.5.0

2018-04-17

### Features

- [CSHARP-636] - Add NO_COMPACT startup option
- [CSHARP-649] - Limit the write queue size at connection level

## 3.4.1

2018-02-26

### Bug Fixes

- [CSHARP-498] - Linq: short and sbyte parameters fail for constant on Where expressions
- [CSHARP-611] - QueryOptions.GetSerialConsistencyLevel() is not being used
- [CSHARP-635] - Linq: Table creation containing a static counter column not supported
- [CSHARP-640] - Exception using nullable long in a UDT
- [CSHARP-641] - ReadFailureException does not log number of failures
- [CSHARP-643] - Responses with warnings and/or custom payloads are incorrectly parsed for non-results
- [CSHARP-650] - Building of Cluster fails when single contact point DNS entry cannot be resolved

## 3.4.0

2017-11-13

### Features

- [CSHARP-604] - UdtMappingDefinitions.Define needs async counterpart
- [CSHARP-317] - Linq: Support IN with tuple notation for composite clustering keys
- [CSHARP-326] - Mapper: Enum support in collections.
- [CSHARP-370] - Add Cluster.ConnectAsync() and Cluster.ShutdownAsync to the API
- [CSHARP-381] - Support conversion for UDT fields mapping
- [CSHARP-478] - Provide simple way to override a single setting in PoolingOptions
- [CSHARP-506] - Allow prepared statements to be prepared on all nodes
- [CSHARP-524] - UPDATE ... IF EXISTS support for linq
- [CSHARP-528] - Provide more information in the NoHostAvailableException message
- [CSHARP-556] - Add max and min uuid methods to TimeUuid structure
- [CSHARP-590] - Modify the message for batch log write failures
- [CSHARP-592] - Expose TimeUuid.Parse() method
- [CSHARP-598] - Use ExceptionDispatchInfo for preserving original stack trace
- [CSHARP-606] - Expose information on the state of connection pools

### Bug Fixes

- [CSHARP-364] - FrameWriter.WriteShort() should encode ushorts
- [CSHARP-512] - Linq: Boolean expressions without equality operators are not generated correctly
- [CSHARP-515] - Linq: Chained methods after CqlInsert.IfNotExists() are not considered
- [CSHARP-522] - Linq: using CqlOperator.SubstractAssign to remove an item from a map fails
- [CSHARP-547] - PreparedStatement instances created from empty constructor can not bind values
- [CSHARP-558] - Mapper: Table creation error when PartitionKey or ClusteringKey have no Name attribute
- [CSHARP-574] - UnobservedTaskException in Connection class
- [CSHARP-578] - Linq: CqlQuery<T>.QueryTrace always null
- [CSHARP-614] - BatchRequests dont include generated timestamps

## 3.3.2

2017-07-13

### Features

- [CSHARP-581] - Linq Batch: allow setting the batch type
- [CSHARP-582] - Linq: Add Allow filtering on scalar Count() method
- [CSHARP-587] - Support LocalDate and LocalTime parsing

### Bug Fixes

- [CSHARP-577] - InvalidCastException in Cassandra.Data.Linq
- [CSHARP-586] - WithDefaultTimestamp flag is not on when using timestamp generator

## 3.3.1

2017-06-19

### Bug Fixes

- [CSHARP-579] - SynchronizationContext-based deadlock on Connect()

## 3.3.0

2017-05-22

### Notable Changes

- Timestamp generation: client-side timestamps are generated and sent in the request by default when the server 
supports it.
- Enhanced retry policies: handle client timeouts, connection closed and other errors.

### Features

- [CSHARP-205] - Add client-side timestamp generator
- [CSHARP-449] - Expose OnRequestError() method in a new extended retry policy interface
- [CSHARP-529] - Make protocol negotiation more resilient
- [CSHARP-543] - Linq: Evaluation of closures can be done more efficiently
- [CSHARP-496] - Linq: Support Cassandra 3.10 GROUP BY

### Bug Fixes

- [CSHARP-540] - Table metadata can not read custom types column info
- [CSHARP-555] - Cluster.Init: C# driver appears to be leaking on TimeoutException
- [CSHARP-559] - Mapper.ExecuteAsync doesn't allow ConsistencyLevel setting on the BatchStatement
- [CSHARP-563] - TokenAwarePolicy does not take statement keyspace into account

## 3.2.1

2017-02-15

### Bug Fixes

- [CSHARP-503] - TimeUuid values are deserialized as Guid causing problems
- [CSHARP-530] - Row.GetValue() does not support Nullable<DateTimeOffset>
- [CSHARP-531] - Mapper: Avoid using ConcurrentDictionary.Count for Statement cache

## 3.2.0

2016-12-20

### Features

- [CSHARP-274] - Don't mark host down while one connection is active
- [CSHARP-171] - Alternate ILoggerProvider-based logging API

### Bug Fixes

- [CSHARP-504] - Table metadata fetching for case-sensitive UDT name fails
- [CSHARP-507] - Mapping attributes: ClusteringKeyAttribute.Name is not used
- [CSHARP-511] - Timeuuid collections are not converted from Guid[]
- [CSHARP-513] - Use constant number for AssemblyVersion attribute

## 3.1.0

2016-10-17

### Merged from 3.0 branch:

#### Bug Fixes

- [CSHARP-483] - Request timeout but driver log and net-trace shows the server responds promptly
- [CSHARP-492] - Mapper: Include keyspace name when defined in mappings
- [CSHARP-493] - Connection: Reading while cancelling all in-flight requests can result in NullReferenceException
- [CSHARP-500] - Concurrent calls to OperationState.SetCompleted() can result in deadlock

## 3.0.9

2016-09-29

### Bug Fixes

- [CSHARP-483] - Request timeout but driver log and net-trace shows the server responds promptly
- [CSHARP-492] - Mapper: Include keyspace name when defined in mappings
- [CSHARP-493] - Connection: Reading while cancelling all in-flight requests can result in NullReferenceException
- [CSHARP-500] - Concurrent calls to OperationState.SetCompleted() can result in deadlock

## 3.1.0-beta1

2016-08-17

### Notable Changes

- Added .NET Core support and dropped support for .NET Framework 4.0 [#240](https://github.com/datastax/csharp-driver/pull/240)

### Features

- [CSHARP-384] - .NET Core Support
- [CSHARP-426] - Drop support for .NET Framework 4.0

## 3.0.8

2016-08-04

### Bug Fixes

- [CSHARP-467] - Linq query generation only supports single increments and decrements
- [CSHARP-475] - cluster.Metadata.GetTable(KS,TB).TableColumns KeyType returns None instead of Partition.
- [CSHARP-476] - KeyNotFoundException from Cassandra Error Response

## 3.0.7

2016-06-27

### Features

- [CSHARP-444] - Expose AddressTranslator instance in Configuration
- [CSHARP-461] - Include Idempotence-aware retry policy

### Bug Fixes

- [CSHARP-331] - TokenMap#ComputeTokenToReplicaNetwork does not account for multiple racks in a DC
- [CSHARP-372] - ProtocolErrorException are incorrectly reported
- [CSHARP-465] - OperationTimedOut exceptions are not included in NoHostAvailableException

## 3.0.6

2016-05-12

### Bug Fixes

- [CSHARP-447] - TimeUuid instances: Cannot order by underlying date
- [CSHARP-448] - Support big-endian systems in TimeUuid struct
- [CSHARP-451] - Mapper: Invalid CQL if first pocoValue is null and a later value is not
- [CSHARP-453] - Decimal serializer: support negative scale

## 3.0.5

2016-04-20

### Features

- [CSHARP-409] - Mapper: Support timestamp at CqlQueryOptions level
- [CSHARP-415] - Allow setting read timeout at statement level

### Bug Fixes

- [CSHARP-414] - Linq projections not handled correctly for select query.
- [CSHARP-417] - Query trace: introduce delay between retries and max retries
- [CSHARP-428] - Use QueryAbortTimeout for metadata sync queries
- [CSHARP-439] - Race condition on read timeout
- [CSHARP-445] - PrepareAsync() blocks the calling thread when fetching table metadata

## 3.0.4

2016-03-28

### Features

- [CSHARP-393] - Pass the authenticator name from the server to the auth provider [#196](https://github.com/datastax/csharp-driver/pull/196)
- [CSHARP-395] - Mapper: Support TTL for Inserts [#192](https://github.com/datastax/csharp-driver/pull/192)
- [CSHARP-402] - Support custom type serializers [#194](https://github.com/datastax/csharp-driver/pull/194)
- [CSHARP-406] - Support Dictionary as named parameters in SimpleStatement [#199](https://github.com/datastax/csharp-driver/pull/199)
- [CSHARP-410] - Mapper: Allow setting the BatchType for CreateBatch() [#195](https://github.com/datastax/csharp-driver/pull/195)

## 3.0.3

2016-03-03

### Bug Fixes

- [CSHARP-408] - Serial not allowed as Statement consistency level

## 3.0.2

2016-02-11

### Bug Fixes

- [CSHARP-403] - LZ4 decompression buffer pooling bug

## 3.0.1

2016-01-14

### Features

- [CSHARP-388] - Expose Builder.WithMaxProtocolVersion()

### Bug Fixes

- [CSHARP-385] - TokenMap evaluates all the ring when a replication factor contains a non-existent DC
- [CSHARP-386] - Following reconnection attempts may be not be scheduled depending on timer precision
- [CSHARP-391] - RequestExecution retry counter should be volatile

## 3.0.0

2015-12-21

### Notable Changes

- Default consistency changed back to `LOCAL_ONE`.

### Features

- [CSHARP-299] - Mapper and Linq: handle NULLs efficiently at the client level
- [CSHARP-378] - Change default consistency level to `LOCAL_ONE`
- [CSHARP-374] - Use a private field for inFlight counting
- [CSHARP-375] - Enable heartbeat by default
- [CSHARP-377] - Reduce allocations inside RowSet class for void results

### Bug Fixes

- [CSHARP-313] - DCAwareRoundRobinPolicy incorrect detection of local datacenter, connects to wrong datacenter
- [CSHARP-336] - Connection with SSL settings to a C* host without ssl enabled causes the driver to hang
- [CSHARP-351] - Linq CreateTable(): support frozen keyword
- [CSHARP-366] - ControlConnection: reconnection attempt after Cluster.Shutdown() may cause ObjectDisposedException
- [CSHARP-376] - HashedWheelTimer should remove cancelled timeouts on each tick to allow GC


## 3.0.0-beta2

2015-11-19

### Notable Changes

- Support for Cassandra 3.0
- _Breaking_: Changed default consistency level to LOCAL_QUORUM [#158](https://github.com/datastax/csharp-driver/pull/158)
- _Breaking_: `AggregateMetadata.InitialCondition` member now returns the string representation of the value
[#157](https://github.com/datastax/csharp-driver/pull/157)
- Changed read timeout to 12 secs [#158](https://github.com/datastax/csharp-driver/pull/158)
- Linq: Select expressions or lambdas that do not specify fields generate a query with explicit columns names.
ie: `SELECT a, b, c, ...` [#94](https://github.com/datastax/csharp-driver/pull/94)

### Features

- [CSHARP-361] - Update schema type representation to CQL
- [CSHARP-353] - Change default consistency level to LOCAL_QUORUM
- [CSHARP-259] - Enable TCP NoDelay by Default
- [CSHARP-356] - Updated default behavior unbound values in prepared statements
- [CSHARP-362] - Set default read timeout to 12 secs

### Bug Fixes

- [CSHARP-308] - Linq: Avoid using SELECT * when the fields are not specified
- [CSHARP-365] - Mono: SocketAsyncEventArgs BufferList must implement indexer
- [CSHARP-371] - Mapper: Allow automatic conversion to structs for null values

## 3.0.0-beta1

2015-10-19

### Notable Changes

- Support for Cassandra 3.0-rc2

### Features

- [CSHARP-213] - Retrieve Cassandra Version with the Host Metadata
- [CSHARP-286] - Process Modernized Schema Tables for 3.0
- [CSHARP-348] - Process materialized view metadata
- [CSHARP-359] - Updated Clustering Order Representation in Schema Metadata

## 2.8.0-alpha1

2015-10-29

### Features

- [CSHARP-270] - Use a pool of buffers
- [CSHARP-244] - Add driver-side write batching of native frames

## 2.7.3

2015-11-13

### Notable Changes

- **Mapper** and **Linq**: Fixed regression introduced in v2.7.2 that failed to map columns with null values to structs.

### Bug Fixes

- [CSHARP-371] - Mapper: Allow default(T) for null values

## 2.7.2

2015-10-06

### Features

- [CSHARP-337] - Make PreparedStatement mockable

### Bug Fixes

- [CSHARP-342] - Custom type converters do not work
- [CSHARP-345] - Regression: Support server error in STARTUP response for C* 2.1
- [CSHARP-346] - Mapper: Support anonymous type containing a nullable column for value types
- [CSHARP-347] - Improve exception message for null values on collections

## 2.7.1

2015-09-17

### Bug Fixes

- [CSHARP-344] - Calling Socket.ConnectAsync() can result in uncaught exception

## 2.7.0

2015-09-10

### Notable Changes

- **Cluster**: All requests use a client read timeout that can be configured using `SocketOptions.SetReadTimeoutMillis(int)`, disabled by default
- **Cluster**: Added support for _speculative query executions_

### Features

- [CSHARP-273] - New Retry Policy Decision - try next host
- [CSHARP-311] - Cluster-level reusable timer: Hashed Wheel Timer
- [CSHARP-314] - Use Immutable collections for Hosts and the Connection pool
- [CSHARP-243] - Speculative query retries
- [CSHARP-279] - Make connection and pool creation non blocking
- [CSHARP-280] - Schedule reconnections using Timers
- [CSHARP-328] - Linq support for StartsWith()
- [CSHARP-332] - Per-Host Request Timeout

### Bug Fixes

- [CSHARP-260] - Default QueryAbortTimeout value should not be Infinite
- [CSHARP-296] - Driver hangs when system.peers is mucked up.
- [CSHARP-316] - RequestHandler retry should not use a new query plan
- [CSHARP-325] - Enum column mapping to int is not supported in Linq Update()
- [CSHARP-333] - Allow Heartbeat interval be set to zero to disable it
- [CSHARP-338] - TcpSocket.ReceiveAsync() raises ObjectDisposedException that faults the user Task
- [CSHARP-339] - Host reconnection delay Read operation and isUp Write operation are not thread-safe
- [CSHARP-340] - Switching keyspace at level after disposing connection results in ObjectDisposedException

## 2.6.0

2015-08-10

### Notable Changes

- Added support for Cassandra 2.2 types and features

### Features

- [CSHARP-282] - Support UDF and Aggregate Function Schema Meta
- [CSHARP-283] - Add client address to query trace
- [CSHARP-285] - Support protocol v4 exceptions
- [CSHARP-287] - Use PK columns from v4 prepared responses
- [CSHARP-290] - Key-value payloads in native protocol v4
- [CSHARP-291] - Small int and byte types for C* 2.2
- [CSHARP-293] - Support new date and time types
- [CSHARP-303] - Add support for client warnings
- [CSHARP-304] - Distinguish between NULL and UNSET values in Prepared Statements
- [CSHARP-305] - Support server error in STARTUP response for C* 2.1

## 2.5.2

2015-04-08

### Bug Fixes

- [CSHARP-255] - Use active connection to wait for schema agreement
- [CSHARP-268] - Linq Select() projections for Update() do not support fields
- [CSHARP-269] - Mapper always sets automatic paging to false

## 2.5.1

2015-03-23

### Features

- [CSHARP-146] - Manual paging
- [CSHARP-211] - Make Statement.SetRoutingKey() api more friendly
- [CSHARP-240] - Log additional information on RequestHandler and Connection classes
- [CSHARP-242] - Make pool creation, after node considered back up, less eager
- [CSHARP-245] - Use specific TaskScheduler for calling callbacks from the Connection
- [CSHARP-251] - Support Lightweight Transactions in the Mapper API
- [CSHARP-258] - Add TCP NoDelay socket option
- [CSHARP-261] - Linq: Manual paging
- [CSHARP-262] - Mapper: Manual paging

### Bug Fixes

- [CSHARP-246] - ObjectDisposedException in Connection.WriteCompletedHandler()
- [CSHARP-252] - Metadata.HostEvent is not firing when a node is Down, it does fire when a node is Up
- [CSHARP-253] - RPTokenFactory produces negative hashes
- [CSHARP-254] - TokenMap doesn't handle adjacent ranges owned by the same host
- [CSHARP-264] - Builder.WithPort() option not considered
- [CSHARP-266] - Connection: Serialization of bad frames can result in NullReferenceException

## 2.5.0

2015-02-05

### Features

- [CSHARP-234] - Support Freezing and Nested Collections
- [CSHARP-174] - Deprecate SimpleStatement.Bind and add constructor params
- [CSHARP-176] - Mark Session.WaitForSchemaAgreement() as Obsolete
- [CSHARP-235] - Mapper & Linq: Issue a log warning when the number of prepared statements is large
- [CSHARP-239] - Make RowSet.GetEnumerator() and Row.GetValue() methods mockable

### Bug Fixes

- [CSHARP-227] - ReplicationStrategies now internal?
- [CSHARP-237] - Linq Table<T>.Create() does not use provided keyspace when using constructor that specifies it
- [CSHARP-241] - TaskHelper.Continue() calls to SynchronizationContext.Post can cause deadlocks
- [CSHARP-247] - Support for COM single threaded apartment model
- [CSHARP-248] - QueryOptions PageSize setting not being respected

## 2.5.0-rc1

2015-01-15

### Notable Changes

- Integrated [CqlPoco](https://github.com/LukeTillman/cqlpoco) into the driver codebase

### Features

- [CSHARP-199] - Integrate CqlPoco
- [CSHARP-165] - Add an address translator
- [CSHARP-173] - Linq Counter column support
- [CSHARP-81] - Cannot add item to collection column using linq
- [CSHARP-89] - Ignore attribute for object properties would be really useful
- [CSHARP-92] - Makes it possible to provide Table and Keyspace creation options to Linq
- [CSHARP-123] - Linq: Make identifier quoting optional
- [CSHARP-128] - Linq CreateTablesIfNotExist: varint support
- [CSHARP-143] - Strong Name the NuGet Assemblies
- [CSHARP-144] - Linq: CQL functions support
- [CSHARP-153] - Performance optimization for adapt Rows to entities
- [CSHARP-214] - Use InvalidTypeException with a clear message when conversion is not valid
- [CSHARP-215] - Make mapping Fetch<T>() lazy
- [CSHARP-233] - Support SortedSet and HashSet as valid sets and arrays as valid List or Set values

### Bug Fixes

- [CSHARP-170] - When Changing Key Space after a Prepare, the driver goes into a recursive exception pattern causing the application to be blocked
- [CSHARP-229] - Internal class Cassandra.TcpSocket leaks memory that can cause long running multiple/many C* sessions with large records to run OOM
- [CSHARP-231] - Batch ExecutionInfo always returns consistency Any as achieved consistency

