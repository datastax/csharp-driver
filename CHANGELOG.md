# ChangeLog - DataStax C# Driver

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

