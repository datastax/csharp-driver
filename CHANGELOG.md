# ChangeLog - DataStax Enterprise C# Driver

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
