- Feature Name: Add OpenTelemetry Traces
- Start Date: 2023-12-27

# Summary
[summary]: #summary

[OpenTelemetry](https://opentelemetry.io/docs/what-is-opentelemetry/) is a collection of APIs, SDKs, and tools used to instrument, generate, collect, and export telemetry data (metrics, logs, and traces) to help the analysis of software’s performance and behavior.
This document describes the necessary steps to include OpenTelemetry tracing in C# driver.

# Motivation
[motivation]: #motivation

OpenTelemetry is the industry standard regarding telemetry data that aggregates logs, metrics, and traces. Specifically regarding traces, it allows the developers to understand the full "path" a request takes in the application and navigate through the service(s).
For the .NET ecosystem, there are available implementations regarding the export of telemetry data in client-side calls in some major database management systems, being them community lead efforts ([SqlClient](https://github.com/open-telemetry/opentelemetry-dotnet/tree/main/src/OpenTelemetry.Instrumentation.SqlClient), [MongoDB](https://github.com/jbogard/MongoDB.Driver.Core.Extensions.OpenTelemetry)), or native implementations ([Elasticsearch](https://github.com/elastic/elasticsearch-net/blob/main/src/Elastic.Clients.Elasticsearch/Client/ElasticsearchClient.cs#L183)).

Cassandra has a community developed package available in the [opentelemetry-dotnet-contrib](https://github.com/open-telemetry/opentelemetry-dotnet-contrib/tree/main/src/OpenTelemetry.Instrumentation.Cassandra) that exports metrics but not traces. This proposal to include traces in the native Cassandra C# driver will allow the developers to have access to database operations when analyzing the requests that are handled in their systems when it includes Cassandra calls.

# Guide-level explanation
[guide-level-explanation]: #guide-level-explanation

## [Traces](https://opentelemetry.io/docs/concepts/signals/traces/)

As mentioned in [*Motivation*](#motivation), traces allows the developers to understand the full "path" a request takes in the application and navigate through a service. Traces include [Spans](https://opentelemetry.io/docs/concepts/signals/traces/#spans) which are unit of works or operation in the ecosystem that include the following information:

- Name
- Parent span ID (empty for root spans)
- Start and End Timestamps
- [Span Context](https://opentelemetry.io/docs/concepts/signals/traces/#span-context)
- [Attributes](https://opentelemetry.io/docs/concepts/signals/traces/#attributes)
- [Span Events](https://opentelemetry.io/docs/concepts/signals/traces/#span-events)
- [Span Links](https://opentelemetry.io/docs/concepts/signals/traces/#span-links)
- [Span Status](https://opentelemetry.io/docs/concepts/signals/traces/#span-status)

The spans can be correlated with each other and assembled into a trace using [context propagation](https://opentelemetry.io/docs/concepts/signals/traces/#context-propagation).

### Example of a trace in a microservice architecture

#### Architecture

![architecture](./system-architecture-example.png)

#### Data visualization using Jaeger

![jaeger](./system-architecture-trace-timeline.png)

## OpenTelemetry Semantic Conventions
[opentelemetry-semantic-conventions]: #opentelemetry-semantic-conventions

### Span name

[OpenTelemetry Trace Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/general/trace/) (at the time of this writing, it's on version 1.24.0) defines multipurpose semantic conventions regarding tracing for different components and protocols (e.g.: Database, HTTP, Messaging, etc.)

For C# driver, the focus is the [semantic conventions for database client calls](https://opentelemetry.io/docs/specs/semconv/database/database-spans/) for the generic database attributes, and the [semantic conventions for Cassandra](https://opentelemetry.io/docs/specs/semconv/database/cassandra/) for the specific Cassandra attributes.

According to the specification, the span name "SHOULD be set to a low cardinality value representing the statement executed on the database. It MAY be a stored procedure name (without arguments), DB statement without variable arguments, operation name, etc.".\
The specification also (and only) specifies the span name for SQL databases:\
"Since SQL statements may have very high cardinality even without arguments, SQL spans SHOULD be named the following way, unless the statement is known to be of low cardinality:\
`<db.operation> <db.name>.<db.sql.table>`, provided that `db.operation` and `db.sql.table` are available. If `db.sql.table` is not available due to its semantics, the span SHOULD be named `<db.operation> <db.name>`. It is not recommended to attempt any client-side parsing of `db.statement` just to get these properties,
they should only be used if the library being instrumented already provides them. When it's otherwise impossible to get any meaningful span name, `db.name` or the tech-specific database name MAY be used."

To avoid parsing the statement, the **span name** in this implementation will be `<db.operation> <db.name>` if the **keyspace name** is available. Otherwise, it will be `<db.operation>`.

### Span attributes

This implementation will include, by default, the **required** attributes for Database, and Cassandra spans.\
`server.address` and `server.port`, despite only **recommended**, are included to give information regarding the client connection.\
`db.statement` is optional given that this attribute may contain sensitive information.

| Attribute  | Description  | Type | Level | Required | Supported Values|
|---|---|---|---|---|---|
| span.kind | Describes the relationship between the Span, its parents, and its children in a Trace. | string | - | true | client |
| db.system | An identifier for the database management system (DBMS) product being used. | string | Connection | true | cassandra |
| db.name | The keyspace name in Cassandra. | string | Call | conditionally true [1] | *keyspace in use* |
| db.operation | The name of the operation being executed. | string | Call | true if `db.statement` is not applicable. [2] | method name at session level(eg.: ExecuteAsync()) |
| db.statement | The database statement being executed. | string | Call | false | *database statement in use* [3] |
| server.address | Name of the database host. | string | Connection | true | e.g.: example.com; 10.1.2.80; /tmp/my.sock |
| server.port | Server port number. Used in case the port being used is not the default. | int | Connection | false | e.g.: 9445 |

**[1]:** There are cases where the driver doesn't know about the Keyspace name. If the developer doesn't specify a default Keyspace in the builder, or doesn't run a USE Keyspace statement manually, then the driver won't know about the Keyspace because it doesn't parse statements. If the Keyspace name is not known, the `db.name` attribute is not included.

**[2]:** Despite not being required, this implementation sets the `db.operation` attribute even if `db.statement` is included.

**[3]:** The statement value is the query string and does not include any query values. As an example, having a query that as the string `SELECT * FROM table WHERE x = ?` with `x` parameter of `123`, the attribute value of `db.statement` will be `SELECT * FROM table WHERE x = ?` and not `SELECT * FROM table WHERE x = 123`.

## Usage

### Package installation

The OpenTelemetry implementation will be included in the package `CassandraCSharpDriver.OpenTelemetry`.

### Exporting Cassandra activity
[exporting-cassandra-activity]: #exporting-cassandra-activity

The extension method `AddOpenTelemetryTraceInstrumentation()` will be available in the cluster builder, so the activity can be exported for database operations:

```csharp
var cluster = Cluster.Builder()
    .AddContactPoint("127.0.0.1")
    .WithSessionName("session-name")
    .AddOpenTelemetryTraceInstrumentation()
    .Build();
```

The extension method also includes the option to enable the database statement that is disabled by default:

```csharp
var cluster = Cluster.Builder()
    .AddContactPoint("127.0.0.1")
    .WithSessionName("session-name")
    .AddOpenTelemetryTraceInstrumentation(options => options.IncludeDatabaseStatement = true)
    .Build();
```

### Capturing Cassandra activity

When setting up the tracer provider, it is necessary to include the Cassandra source for the activity to be captured. The activity source name will be available in the property `CassandraInstrumentation.ActivitySourceName` that will be included in the `CassandraCSharpDriver.OpenTelemetry` package.

Example:

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
 .AddSource(CassandraInstrumentation.ActivitySourceName)
 .AddConsoleExporter()
 .Build();
```

# Reference-level explanation
[reference-level-explanation]: #reference-level-explanation

## Cassandra.OpenTelemetry project

### Dependencies and target frameworks

Similar to the existent metrics feature, this functionality will include a project named `Cassandra.OpenTelemetry` that will extend the core `Cassandra` project and will handle the spans'generation.\
`Cassandra.OpenTelemetry` has dependencies from the following packages:

- `System.Diagnostics.DiagnosticSource`, version [`8.0.0`](https://www.nuget.org/packages/System.Diagnostics.DiagnosticSource/8.0.0#dependencies-body-tab), for activity generation;
- `OpenTelemetry.Api` package, version [`1.7.0`](https://www.nuget.org/packages/OpenTelemetry.Api/1.7.0), that is used to [record exceptions as activity events](https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/docs/trace/reporting-exceptions/README.md#option-4---use-activityrecordexception) using the most recent industry standards;

With these two package dependencies, the `Cassandra.OpenTelemetry` can target `netstandard2.0`.

### Extension methods

The project will include a `Builder` extension method named `AddOpenTelemetryTraceInstrumentation` that will instantiate a new class named `Trace` which will start, populate, and dispose the `System.Diagnostics.Activity` class that will have the Cassandra telemetry information.
This new class will implement an `IRequestTracker` interface that will be included in the Cassandra core to track request activity:

```csharp
public interface IRequestTracker
{
    Task OnStartAsync(RequestTrackingInfo request);

    Task OnSuccessAsync(RequestTrackingInfo request);

    Task OnErrorAsync(RequestTrackingInfo request, Exception ex);

    Task OnNodeSuccessAsync(RequestTrackingInfo request, HostTrackingInfo hostInfo);

    Task OnNodeErrorAsync(RequestTrackingInfo request, HostTrackingInfo hostInfo, Exception ex);
}
```

Also, a new class, and struct with contextual information will be created:

```csharp
public class RequestTrackingInfo
    {
        public RequestTrackingInfo()
        {
            this.Items = new ConcurrentDictionary<string, object>();
        }

        public ConcurrentDictionary<string, object> Items { get; }

        public IStatement Statement { get; set;  }
    }

public struct HostTrackingInfo 
{
    public Host Host { get; }
}
```

The `Trace` class, that implements `IRequesTracker`, will add and extract activity using the `Items` property, so the activity can be created and disposed for each request:

```csharp
private static readonly string otelActivityKey = "otel_activity";

public Task OnStartAsync(RequestTrackingInfo request)
{
    (...)

    var activity = ActivitySource.StartActivity("cassandra", ActivityKind.Client);

    request.Items.TryAdd(otelActivityKey, activity);

    (...)
}
```

```csharp
public Task OnSuccessAsync(RequestTrackingInfo request)
{
    (...)

    request.Items.TryGetValue(otelActivityKey, out object context);

    if (context is Activity activity)
    {
        activity?.Dispose();
    }

    (...)
}
```

### Cassandra instrumentation options

As mentioned in ["exporting cassandra activity"](#exporting-cassandra-activity) section, the builder extension includes a set of instrumentation options that, for now, includes the option to enable the `db.statement` attribute in the trace. The default value is false.

```csharp
 public class CassandraInstrumentationOptions
{
    public bool IncludeDatabaseStatement { get; set; } = false;
}
```

## Cassandra core project

### Tracer observers

The tracing implementation will use the already defined factory interfaces for the creation of the observer and observer factories, so the following classes will be created:

```csharp
internal class TracerObserverFactoryBuilder : IObserverFactoryBuilder
```

```csharp
internal class TracerObserverFactory : IObserverFactory
```

```csharp
internal class TracesRequestObserver : IRequestObserver
```

The `TracesRequestObserver` receives an `IRequestTracker` and maps the information from the observer against the tracker, ex.:

```csharp
public void OnRequestFailure(Exception ex, RequestTrackingInfo r)
{
    _requestTracker.OnErrorAsync(r, ex);
}
```

### Public API

The `Builder` will include a new method named `WithRequestTracker` that will include an `IRequestTracker` instance being passed as parameter. This method can be used by anyone to pass their tracking implementation and will be used by the extension method `AddOpenTelemetryTraceInstrumentation()` mentioned in the previous section.

```csharp
public Builder WithRequestTracker(IRequestTracker requestTracker)
{
    _requestTracker = requestTracker;
    return this;
}
```

### Changes to Cassandra core

#### IRequestObserver

The request observer interface will change, so it can be mapped to the request tracking.

- `OnRequestFinish(Exception exception);` will be split into two new methods, `OnRequestFailure()` and `OnRequestSuccess()` to differentiate between finished failed executions and finished successful executions. These two methods will include a `RequestTrackingInfo` parameter.

- `OnRequestStart()` will include a `RequestTrackingInfo` parameter.

- `OnRequestError(...)` will be renamed to `OnNodeRequestError(...)` to differentiate against the new `OnRequestFailure()`.

```csharp
internal interface IRequestObserver
{
    void OnSpeculativeExecution(Host host, long delay);

    void OnNodeRequestError(Host host, RequestErrorType errorType, RetryDecision.RetryDecisionType decision);

    void OnRequestStart(RequestTrackingInfo r);

    void OnRequestFailure(Exception ex, RequestTrackingInfo r);

    void OnRequestSuccess(RequestTrackingInfo r);
}
```

#### Cassandra.Requests

The request handler and request result handler will have some changes to propagate request activity and information.
The `RequestHandler` will instantiate a `RequestTrackingInfo` with the statement information:

```csharp
public RequestHandler(
    (...)

    _requestTrackingInfo = new RequestTrackingInfo { Statement = statement };
    
    (...)
)
```

The `IRequestResultHandler` will have its methods `TrySetResult()` and `TrySetException()` to include `RequestTrackingInfo` as a parameter.

`TcsMetricsRequestResultHandler` will change the implementation, according to the interface, and will also receive `RequestTrackingInfo` instance in the constructor, so it can be passed to the updated method `IRequestObserver.OnRequestStart(RequestTrackingInfo)`:

```csharp
public TcsMetricsRequestResultHandler(
    IRequestObserver requestObserver,
    RequestTrackingInfo requestTrackingInfo)
{
    _requestObserver = requestObserver;
    _taskCompletionSource = new TaskCompletionSource<RowSet>();
    _requestObserver.OnRequestStart(requestTrackingInfo);
}
```

Concluding, `RequestHandler` will have start passing `_requestTrackingInfo` as argument to request handler calls, e.g.:

```csharp
_requestResultHandler.TrySetResult(result, _requestTrackingInfo);
```

#### Composite observers and factories
[composite-observers-and-factories]: #composite-observers-and-factories

The core package includes observers that are instantiated through the configuration points in the driver, being the *Configuration* class one example:

```csharp
internal Configuration(...)
{
    (...)
    
    ObserverFactoryBuilder = observerFactoryBuilder ?? (MetricsEnabled ? (IObserverFactoryBuilder)new MetricsObserverFactoryBuilder() : new NullObserverFactoryBuilder());

    (...)
}
```

As it is necessary to have multiple observers being triggered on driver's actions, this proposal includes a composite pattern that will aggregate multiple observer instances for that work.

The new composite classes will implement the interfaces`IObserverFactory`, `IObserverFactoryBuilder`, `IRequestObserver`, as shown below:

```csharp
internal class CompositeObserverFactory : IObserverFactory
{
    private readonly IEnumerable<IObserverFactory> factories;

    public CompositeObserverFactory(IEnumerable<IObserverFactory> factories)
    {
        this.factories = factories;
    }
    
    (...)
}
```

```csharp
internal class CompositeObserverFactoryBuilder : IObserverFactoryBuilder
{
    private readonly IObserverFactoryBuilder[] builders;

    public CompositeObserverFactoryBuilder(params IObserverFactoryBuilder[] builders)
    {
        this.builders = builders;
    }
    
    (...)
}
```

```csharp
internal class CompositeRequestObserver : IRequestObserver
{
    private readonly IEnumerable<IRequestObserver> observers;

    public CompositeRequestObserver(IEnumerable<IRequestObserver> observers)
    {
        this.observers = observers;
    }
    
    (...)
}
```

This composite classes, when called, will iterate through all the observer instances being passed in the constructors and will call the method of their respective observer. As an example, the method `IRequestObserver.OnRequestSuccess(RequestTrackingInfo r)` will look like this:

```csharp
public void OnRequestSuccess(RequestTrackingInfo r)
{
    foreach (var observer in this.observers)
    {
        observer.OnRequestSuccess(r);
    }
}
```

The composite observers will be instantiated in the place of the current ones and will receive a list of observers as parameter. As an example, the `ObserverFactoryBuilder` in *Configuration* class mentioned [above](#composite-observers-and-factories) will include `MetricsObserverFactoryBuilder` and `TracerObserverFactoryBuilder` as parameter of the `CompositeObserverFactoryBuilder`:

```csharp
internal Configuration(...)
{
    (...)
     ObserverFactoryBuilder = new CompositeObserverFactoryBuilder(
                new MetricsObserverFactoryBuilder(MetricsEnabled),
                new TracerObserverFactoryBuilder(driverTracer));
    (...)
}
```

As it is possible to be seen, the validation to construct the `MetricsRequestObserver` will be moved to the `MetricsObserverFactoryBuilder` instead of being done in the configuration, e.g.:

```csharp
public IObserverFactory Build(IMetricsManager manager)
{
    return this.isEnabled ? new MetricsObserverFactory(manager) : NullObserverFactory.Instance;
}

```

# Drawbacks
[drawbacks]: #drawbacks

`TracesRequestObserver`, for now, doesn't have a use for the methods `OnRequestError` and `OnSpeculativeExecution` defined in the interface `IRequestObserver`. However, this can change in the future as the semantic conventions defines more attributes than the ones being included in this proposal, being some of them related to speculative execution.

Another point that should be looked into is that the current observers can sometimes be metrics-focused which means that some implementations will have method definitions that are not being used. As an example, the `IObserverFactoryBuilder.Build` that is implemented by `TracerObserverFactoryBuilder` and `NullObserverFactoryBuilder` doesn't need the `IMetricsManager` instance that is passed as parameter.\
The observer interfaces pose a possibility of disaggregation in a future version of the driver.

# Rationale and alternatives
[rationale-and-alternatives]: #rationale-and-alternatives

## Using OpenTelemetry.SemanticConventions package

The [semantic conventions](https://opentelemetry.io/docs/specs/semconv/) are a fast evolving reference that "define a common set of (semantic) attributes which provide meaning to data when collecting, producing and consuming it.".\
As its changes can be hard to follow, the .NET project includes a package named [`OpenTelemetry.SemanticConventions`](https://www.nuget.org/packages/OpenTelemetry.SemanticConventions/1.0.0-rc9.9) that maps the attributes defined in the conventions to a .NET project. Using this package will allow the Cassandra project to have its tracing attributes up-to-date to the conventions with less maintenance, however, as it's still marked as non-stable (current version is `1.0.0-rc9.9`), it is not included in this proposal.

# Prior art
[prior-art]: #prior-art

As mentioned in [*Motivation*](#motivation) section, there are other DBMS implementations regarding the export of telemetry data in client-side calls in the .NET ecosystem:

- [SqlClient](https://github.com/open-telemetry/opentelemetry-dotnet/tree/main/src/OpenTelemetry.Instrumentation.SqlClient) (Community contribution)
- [MongoDB](https://github.com/jbogard/MongoDB.Driver.Core.Extensions.OpenTelemetry) (Community contribution)
- [Elasticsearch](https://github.com/elastic/elasticsearch-net/blob/main/src/Elastic.Clients.Elasticsearch/Client/ElasticsearchClient.cs#L183) (Native)

Cassandra also has client-side implementations in other languages in the form of contribution projects, as listed below:

- [Java](https://github.com/open-telemetry/opentelemetry-java-instrumentation/tree/main/instrumentation/cassandra) (Community contribution)
- [NodeJS](https://github.com/open-telemetry/opentelemetry-js-contrib/tree/main/plugins/node/opentelemetry-instrumentation-cassandra) (Community contribution)
- [Python](https://github.com/open-telemetry/opentelemetry-python-contrib/tree/main/instrumentation/opentelemetry-instrumentation-cassandra) (Community contribution)

# Future possibilities
[future-possibilities]: #future-possibilities

## Traces

### Include missing recommended attributes

As referred in [*semantic conventions* section](#opentelemetry-semantic-conventions), there are recommended attributes that are not included in this proposal that may be useful for the users of Cassandra telemetry and can be something to look at in the future iterations of this feature:

- [Cassandra Call-level attributes](https://opentelemetry.io/docs/specs/semconv/database/cassandra/#call-level-attributes)
- [Database Call-level attributes](https://opentelemetry.io/docs/specs/semconv/database/database-spans/#call-level-attributes)
- [Database Connection-level attributes](https://opentelemetry.io/docs/specs/semconv/database/database-spans/#connection-level-attributes)

### Include customization

The implementation suggested in this document is based in SqlClient implementation regarding the attributes exposed and the inclusion of database statement as optional. However, the SqlClient implementation has other forms of customization that are not included in this document, specifically the option to [enrich](https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/src/OpenTelemetry.Instrumentation.SqlClient/README.md#enrich) the activity with additional information from the raw `SqlCommand` object (for Cassandra, possibly `IStatement`).

## Metrics

As the industry is moving to adopt OpenTelemetry, the export of metrics using this standard may be something useful for the users of Cassandra C# Driver. Although the [semantic conventions for database metrics](https://opentelemetry.io/docs/specs/semconv/database/database-metrics/) are still in experimental status, there are already some community-lead efforts [available in the opentelemetry-dotnet-contrib repositories](https://github.com/open-telemetry/opentelemetry-dotnet-contrib/tree/main/src/OpenTelemetry.Instrumentation.Cassandra).
