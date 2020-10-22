# Changelog

Changelog for the DataStax Enterprise C# Driver.

## 2.10.4

2020-10-22

### Improvements

*   [[CSHARP-942](https://datastax-oss.atlassian.net/browse/CSHARP-942)] - Log control connection init failures

### Bug fixes

*   [[CSHARP-943](https://datastax-oss.atlassian.net/browse/CSHARP-943)] - Cluster.Connect() throws "ArgumentException 'The source argument contains duplicate keys.'"

## 2.10.3

2020-09-10

### Improvements

*   [[CSHARP-925](https://datastax-oss.atlassian.net/browse/CSHARP-925)] - Ignore credentials in secure connect bundle [DataStax Astra]
*   [[CSHARP-934](https://datastax-oss.atlassian.net/browse/CSHARP-934)] - Optimize TimeoutItem cleanup

### Bug fixes

*   [[CSHARP-902](https://datastax-oss.atlassian.net/browse/CSHARP-902)] - ProtocolEventDebouncer calls refresh keyspace for the same keyspace multiple times
*   [[CSHARP-906](https://datastax-oss.atlassian.net/browse/CSHARP-906)] - C# driver doesn't support DSE transitional authentication
*   [[CSHARP-907](https://datastax-oss.atlassian.net/browse/CSHARP-907)] - Insights CPU info not available in netcoreapp3.1 on Windows
*   [[CSHARP-908](https://datastax-oss.atlassian.net/browse/CSHARP-908)] - DateRangeSerializer precision issue

### Documentation

*   [[CSHARP-927](https://datastax-oss.atlassian.net/browse/CSHARP-927)] - Document Duration type

## 2.10.2

2020-05-12

### Bug fixes

*   [[CSHARP-659](https://datastax-oss.atlassian.net/browse/CSHARP-659)] - Query trace retrieval fails when started_at is null
*   [[CSHARP-884](https://datastax-oss.atlassian.net/browse/CSHARP-884)] - Race condition in the ControlConnection Dispose method can leak connections
*   [[CSHARP-894](https://datastax-oss.atlassian.net/browse/CSHARP-894)] - Batch Statements cause a warning on TokenMap due to null keyspace
*   [[CSHARP-896](https://datastax-oss.atlassian.net/browse/CSHARP-896)] - Invalid deserialization when paging a rowset and a schema change happens

## 2.10.1

2020-03-24

## Features

*   [[CSHARP-862](https://datastax-oss.atlassian.net/browse/CSHARP-862)] - Update System.Net.Http to fix vulnerabilities
*   [[CSHARP-863](https://datastax-oss.atlassian.net/browse/CSHARP-863)] - Update System.Net.Security to fix vulnerabilities

### AppMetrics Extension

*   [[CSHARP-851](https://datastax-oss.atlassian.net/browse/CSHARP-851)] - HistogramReservoir values should reset periodically (Timer metrics)

## Bug fixes

*   [[CSHARP-696](https://datastax-oss.atlassian.net/browse/CSHARP-696)] - HostConnectionPool incorrectly logs reconnection message after warmup
*   [[CSHARP-697](https://datastax-oss.atlassian.net/browse/CSHARP-697)] - When socket.ConnectAsync() throws an error synchronously, SocketAsyncEventArgs is not disposed
*   [[CSHARP-710](https://datastax-oss.atlassian.net/browse/CSHARP-710)] - Mapper cannot deserialize IList properties
*   [[CSHARP-711](https://datastax-oss.atlassian.net/browse/CSHARP-711)] - Race condition: re-prepare on UP might not use the current keyspace
*   [[CSHARP-786](https://datastax-oss.atlassian.net/browse/CSHARP-786)] - Support NULL in collection serializer
*   [[CSHARP-798](https://datastax-oss.atlassian.net/browse/CSHARP-798)] - Handle prepared id mismatch when repreparing on the fly
*   [[CSHARP-801](https://datastax-oss.atlassian.net/browse/CSHARP-801)] - Exception on UDT => CLR mapping of collection property which has null value
*   [[CSHARP-816](https://datastax-oss.atlassian.net/browse/CSHARP-816)] - "Local datacenter is not specified" message is logged if user specifies it with the default execution profile
*   [[CSHARP-821](https://datastax-oss.atlassian.net/browse/CSHARP-821)] - Policies.NewDefaultLoadBalancingPolicy and Policies.DefaultLoadBalancingPolicy return the default OSS default
*   [[CSHARP-839](https://datastax-oss.atlassian.net/browse/CSHARP-839)] - Mapper and Linq2Cql causes re-prepare warnings in scenarios with high concurrency
*   [[CSHARP-845](https://datastax-oss.atlassian.net/browse/CSHARP-845)] - When socket.ConnectAsync returns synchronously, driver assumes that it is connected
*   [[CSHARP-875](https://datastax-oss.atlassian.net/browse/CSHARP-875)] - Duplicate contact points cause cluster initialization failure
*   [[CSHARP-877](https://datastax-oss.atlassian.net/browse/CSHARP-877)] - NodeMetric.DefaultNodeMetrics and SessionMetric.DefaultSessionMetrics contain null values
*   [[CSHARP-878](https://datastax-oss.atlassian.net/browse/CSHARP-878)] - ControlConnection attempts to connect to DOWN nodes

## Documentation

*   [[CSHARP-489](https://datastax-oss.atlassian.net/browse/CSHARP-489)] - Use docfx or doxygen to generate API docs
*   [[CSHARP-847](https://datastax-oss.atlassian.net/browse/CSHARP-847)] - Doc: include a note about concurrent schema modifications

## 2.10.0

2020-01-15

### Features

*   [[CSHARP-791](https://datastax-oss.atlassian.net/browse/CSHARP-791)] - Unified Driver
*   [[CSHARP-840](https://datastax-oss.atlassian.net/browse/CSHARP-840)] - Linq2Cql and Mapper should generate CQL statements in a deterministic way

### Bug fixes

*   [[CSHARP-837](https://datastax-oss.atlassian.net/browse/CSHARP-837)] - Serializer protocol version is changed after init, causing current active connections to fail
*   [[CSHARP-835](https://datastax-oss.atlassian.net/browse/CSHARP-835)] - BatchStatement error message uses short.MaxValue instead of ushort.MaxValue
*   [[CSHARP-832](https://datastax-oss.atlassian.net/browse/CSHARP-832)] - Some graph related classes depend on the current culture

## Dse.AppMetrics 1.0.1

2019-11-11

### Bug fixes

*   [[CSHARP-817](https://datastax-oss.atlassian.net/browse/CSHARP-817)] - App.Metrics snapshot fails when timer metrics don't have any recorded values

## 2.9.0

2019-10-30

### Features

*   [[CSHARP-685](https://datastax-oss.atlassian.net/browse/CSHARP-685)] - Expose metrics
*   [[CSHARP-754](https://datastax-oss.atlassian.net/browse/CSHARP-754)] - DataStax Astra support
*   [[CSHARP-595](https://datastax-oss.atlassian.net/browse/CSHARP-595)] - DcAwareLoadBalancingPolicy: Warn when the local datacenter is not specified
*   [[CSHARP-788](https://datastax-oss.atlassian.net/browse/CSHARP-788)] - Add list of reserved keywords and add double quotes when they are used as identifiers
*   [[CSHARP-802](https://datastax-oss.atlassian.net/browse/CSHARP-802)] - Session.Warmup should mark host as down if no connection can be opened to that host

### Bug Fixes

*   [[CSHARP-770](https://datastax-oss.atlassian.net/browse/CSHARP-770)] - Insights errors are showing up in logs with severity ERROR
*   [[CSHARP-807](https://datastax-oss.atlassian.net/browse/CSHARP-807)] - Exception isn't logged when an exception is thrown while parsing a host token

## 2.8.0

2019-07-22

### Features

- [CSHARP-756] - ControlConnection init: Defer host map creation until system tables have been queried
- [CSHARP-757] - Include host_id in host metadata
- [CSHARP-779] - Separate socket endpoint from the host address using an endpoint resolver

### Bug Fixes

- [CSHARP-778] - NullReferenceException at Cassandra.Data.Linq.CqlExpressionVisitor.GetPropertyValue(MemberExpression node)
- [CSHARP-781] - Linq2Cql GetTable() generates invalid create table when more than one option is specified
- [CSHARP-784] - Driver is unable to correctly reestablish connection with previously decommissioned node

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
