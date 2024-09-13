# OpenTelemetry

The driver provides support for session and node level [traces](https://opentelemetry.io/docs/concepts/signals/traces/) using [OpenTelemetry instrumentation](https://opentelemetry.io/docs/instrumentation/net/).

## Including OpenTelemetry instrumentation in your code

Add the package `Cassandra.OpenTelemetry` to the project and add the extension method `AddOpenTelemetryInstrumentation()` when building your cluster:

```csharp
var cluster = Cluster.Builder()
    .AddContactPoint(Program.ContactPoint)
    .WithSessionName(Program.SessionName)
    .AddOpenTelemetryInstrumentation()
    .Build();
```

Once you have your .NET application instrumentation configured, Cassandra activities can be captured by adding the source `CassandraActivitySourceHelper.ActivitySourceName` in the tracer provider builder:

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource(CassandraActivitySourceHelper.ActivitySourceName)
    ...
```

### Attributes

The table below displays the list of included attributes in this feature:

| Attribute  | Description  | Output Values|
|---|---|---|
| db.name | The keyspace name. | *keyspace in use* |
| db.operation | The type name of the operation being executed. | *Session Request* for session level calls and *Node Request* for node level calls |
| db.statement | The database statement being executed. Included as [optional configuration](#include-statement-as-an-attribute). | *database statement in use* |
| db.system | An identifier for the database management system (DBMS) product being used. | cassandra |
| server.address | The host node. | e.g.: 127.0.0.1 |
| server.port | Port number. | e.g.: 9042 |

The console log below displays an example of a full Cassandra activity:

```console
Activity.TraceId:            710e1a99afec9bcd056c9fe825bcbb3c
Activity.SpanId:             bd42cfc78b552cd1
Activity.TraceFlags:         Recorded
Activity.ActivitySourceName: Cassandra.OpenTelemetry
Activity.ActivitySourceVersion: 1.0.0.0
Activity.DisplayName:        Session Request
Activity.Kind:               Client
Activity.StartTime:          2024-09-13T14:08:36.9762191Z
Activity.Duration:           00:00:00.0416284
Activity.Tags:
    db.system: cassandra
    db.operation: Session Request
    db.name: system
    db.statement: SELECT * FROM system.local
Resource associated with Activity:
    service.name: CassandraDemo
    service.version: 1.0.0
    service.instance.id: 5a5d88b9-cc86-4f8a-a206-9b4f507630b9
    telemetry.sdk.name: opentelemetry
    telemetry.sdk.language: dotnet
    telemetry.sdk.version: 1.8.0
```

## Advanced Configuration

The instrumentation can be configured to extend the default behavior by using `CassandraInstrumentationOptions`.

### Include Statement as an attribute

As mentioned above, the attribute `db.statement` is not included by default in the activity. To change it, set the `CassandraInstrumentationOptions` property `IncludeDatabaseStatement` as *true* when building the cluster:

```csharp
var cluster = Cluster.Builder()
    .AddContactPoint(Program.ContactPoint)
    .WithSessionName(Program.SessionName)
    .AddOpenTelemetryInstrumentation(options => options.IncludeDatabaseStatement = true)
    .Build();
```