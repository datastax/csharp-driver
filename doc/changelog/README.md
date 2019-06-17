# Changelog

Changelog for the DataStax Enterprise C# Driver.

## 2.7.1

2019-06-17

### Bug Fixes

- [CSHARP-772] - Remove internal conversion of timestmap to DateTimeOffset
- [CSHARP-777] - Invalid or unsupported protocol version (0)

## 2.7.0

2019-05-21

### Features

- [CSHARP-275] - Prevent duplicate metadata fetches from control connection and allow disabling schema metadata fetching
- [CSHARP-763] - Introduce Execution Profiles in Mapper and LINQ APIs
- [CSHARP-678] - Introduce Execution Profiles at Session Level
- [CSHARP-761] - Implement GraphExecutionProfile
- [CSHARP-765] - Integrate Insights with Execution Profiles
- [CSHARP-424] - Allow UDT mappings to be defined for different keyspaces

### Bug Fixes

- [CSHARP-687] - DseCluster.WithCredentials() method should use DsePlainTextAuthProvider
- [CSHARP-744] - LINQ and Mapper don't support empty column names
- [CSHARP-766] - Cassandra Date is not mapped correct to LocalDate with some specfic values

## 2.6.0

2019-04-02

### Features

- [CSHARP-538] - Expose Metadata.CheckSchemaAgreement() and ExecutionInfo.IsSchemaInAgreement()
- [CSHARP-618] - Add client configuration information to STARTUP message
- [CSHARP-725] - Deprecate usedHostsPerRemoteDc in DCAwareRoundRobinPolicy
- [CSHARP-727] - Send startup message and invoke Insights RPCs periodically
- [CSHARP-734] - Add DSE specific client configuration information to STARTUP message

### Bug Fixes

- [CSHARP-708] - Retry on current host should be made on a different connection
- [CSHARP-715] - LocalTime Parse fails for valid LocalTime strings on certain cultures
- [CSHARP-752] - Memory leak in ControlConnection

## 2.5.0

2019-02-11

### Features

- [CSHARP-731] - Add .NET Standard 2.0 Windows only support for Kerberos authentication
- [CSHARP-726] - Improvements to token map building process

## 2.5.0-alpha1

2019-01-22

### Features

- [CSHARP-731] - Add .NET Standard 2.0 Windows only support for Kerberos authentication

## 2.4.0

2018-11-26

### Features

- [CSHARP-705] - Provide a means of sending query to a specific node to facilitate virtual table queries
- [CSHARP-706] - Parse Virtual Keyspace Metadata

###  Bug Fixes

- [CSHARP-709] - Rarely occurring concurrency bug in the HashedWheelTimer class

## 2.3.0

2018-06-18

### Features

- [CSHARP-335] - RowSet: Support concurrent asynchronous calls to fetch next page
- [CSHARP-591] - EC2 multi-region address resolution policy
- [CSHARP-625] - Mark DowngradingConsistencyRetryPolicy as deprecated
- [CSHARP-634] - Use system.peers in protocol negotiation
- [CSHARP-669] - Support pool warmup on initialization and enable it by default
- [CSHARP-680] - Use OPTIONS message for heartbeats instead of 'select key from system.local'
- [CSHARP-681] - Log driver version on Connect

###  Bug Fixes

- [CSHARP-631] - BatchStatement: Use routing key from first statement
- [CSHARP-660] - Linq: StatementFactory shouldn't cache failed prepared statements
- [CSHARP-667] - Mapper: Statement factory cache should use keyspace to uniquely identify the query
- [CSHARP-691] - Sync completion of socket.ConnectAsync() is not considered

## 2.2.0

2018-04-17

### Notable Changes

- DSE 6.0 Support

### Features

- [CSHARP-620] - Include hash of result set metadata in prepared statement id
- [CSHARP-621] - Per-query (and per-batch) keyspace support
- [CSHARP-622] - Handle bulked results in Graph
- [CSHARP-636] - Add NO_COMPACT startup option
- [CSHARP-638] - Support new 'nodesync' option in table metadata
- [CSHARP-649] - Limit the write queue size at connection level
- [CSHARP-670] - DETERMINISTIC and MONOTONIC Clauses for Function and Aggregate

## 2.1.1

2018-02-26

###  Bug Fixes

- [CSHARP-498] - Linq: short and sbyte parameters fail for constant on Where expressions
- [CSHARP-611] - QueryOptions.GetSerialConsistencyLevel() is not being used
- [CSHARP-633] - Graph types de/serializarion Culture Variant issue
- [CSHARP-635] - Linq: Table creation containing a static counter column not supported
- [CSHARP-640] - Exception using nullable long in a UDT
- [CSHARP-641] - ReadFailureException does not log number of failures
- [CSHARP-643] - Responses with warnings and/or custom payloads are incorrectly parsed for non-results
- [CSHARP-650] - Building of Cluster fails when single contact point DNS entry cannot be resolved

## 2.1.0

2017-11-13

### Notable Changes

- Add user-friendly methods to Graph elements provide to access its properties.
- Introduced `PrepareOnAllHosts` and `ReprepareOnUp` settings to control driver behaviour when preparing queries.
- Linq improvements and fixes.

### Features

- [CSHARP-434] - Graph: Support friendly deserialization of properties
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
- [CSHARP-607] - Add ConnectAsync() and ShutdownAsync() to DseCluster

###  Bug Fixes

- [CSHARP-364] - FrameWriter.WriteShort() should encode ushorts
- [CSHARP-512] - Linq: Boolean expressions without equality operators are not generated correctly
- [CSHARP-515] - Linq: Chained methods after CqlInsert.IfNotExists() are not considered
- [CSHARP-522] - Linq: using CqlOperator.SubstractAssign to remove an item from a map fails
- [CSHARP-547] - PreparedStatement instances created from empty constructor can not bind values
- [CSHARP-558] - Mapper: Table creation error when PartitionKey or ClusteringKey have no Name attribute
- [CSHARP-574] - UnobservedTaskException in Connection class
- [CSHARP-578] - Linq: CqlQuery<T>.QueryTrace always null
- [CSHARP-614] - BatchRequests dont include generated timestamps

## 2.0.3

2017-07-13

###  Bug Fixes

- [CSHARP-570] - GraphNode.ToEdge fails when no properties are defined
- [CSHARP-577] - InvalidCastException in Cassandra.Data.Linq
- [CSHARP-586] - WithDefaultTimestamp flag is not on when using timestamp generator


### Features

- [CSHARP-581] - Linq Batch: allow setting the batch type
- [CSHARP-582] - Linq: Add Allow filtering on scalar Count() method
- [CSHARP-584] - Support non generic overload for GraphNode.To()
- [CSHARP-585] - GraphSON2 Deserialization on the DSE driver
- [CSHARP-587] - Support LocalDate and LocalTime parsing

## 2.0.2

2017-06-19

###  Bug Fixes

- [CSHARP-579] - SynchronizationContext-based deadlock on Connect()

## 2.0.1

2017-05-22

###  Bug Fixes

- [CSHARP-555] - Cluster.Init: C# driver appears to be leaking on TimeoutException
- [CSHARP-559] - Mapper.ExecuteAsync doesn't allow ConsistencyLevel setting on the BatchStatement
- [CSHARP-563] - TokenAwarePolicy does not take statement keyspace into account
- [CSHARP-568] - SSPI usage is not MIT Kerberos compatible

## 2.0.0

2017-04-18

### Notable Changes

- Timestamp generation: client-side timestamps are generated and sent in the request by default when the server 
supports it.
- Enhanced retry policies: handle client timeouts, connection closed and other errors.

### Features

- [CSHARP-205] - Add client-side timestamp generator
- [CSHARP-449] - Expose OnRequestError() method in a new extended retry policy interface
- [CSHARP-496] - Linq: Support Cassandra 3.10 GROUP BY
- [CSHARP-484] - Add support for Duration graph datatype
- [CSHARP-543] - Linq: Evaluation of closures can be done more efficiently

### Bug Fixes

- [CSHARP-544] - Geometry instances as graph parameters should be serialized into WKT

## 2.0.0-beta1

2017-03-15

### Notable Changes

- DSE 5.1 Support
- _Breaking:_ The DSE driver now contains the core driver instead of depending on it. The driver package exposes
a single root namespace `Dse`. Classes that used to be under `Cassandra` namespace are now exposed in the `Dse`
namespace, you should change your `using` directive to point to `Dse` instead.

### Features

- [CSHARP-529] - Make protocol negotiation more resilient
- [CSHARP-533] - Duration type support
- [CSHARP-535] - DSE Auth 5.1: Support Proxy Authentication in 5.1
- [CSHARP-536] - Support DSE 5.1 DateRangeField
- [CSHARP-537] - Read optional workload set from node metadata
- [CSHARP-541] - Merge Cassandra driver code base into DSE driver

### Bug Fixes

- [CSHARP-540] - Table metadata can not read custom types column info

## 1.1.1

2017-02-15

### Features

- [CSHARP-521] - Update core driver dependency to v3.2.1

## 1.1.0

2016-12-20

### Features

- [CSHARP-488] - .NET Core Support for the DSE driver
- [CSHARP-514] - Expose query and parameters as SimpleGraphStatement properties
- [CSHARP-521] - Update core driver dependency to v3.2.0

## 1.0.1

2016-09-29

### Features

- [CSHARP-491] - Support Newtonsoft.Json version 9
- [CSHARP-502] - Update core driver dependency to v3.0.9
