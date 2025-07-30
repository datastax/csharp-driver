# Metrics

The driver exposes measurements of its internal behavior through an API composed by abstractions. The client application must provide an implementation of these abstractions (`IDriverMetricsProvider`) in order to enable metrics.

DataStax offers an [Extension package] based on the [App.Metrics] library. Application developers can use [App.Metrics] to export metrics to a monitoring tool.

## Structure

There are two categories of metrics:

- **session-level**: the measured data is global to a `Session` instance. For example, `connected-nodes` measures the number of nodes to which we have connections.
- **node-level**: the data is specific to a node (and therefore there is one metric instance per node). For example, `pool.open-connections` measures the number of connections open to this particular node.

**Metric names** are path-like, dot-separated strings. An example is `errors.connection.init`. Metrics are represented by the classes `SessionMetric` and `NodeMetric`. These classes have `public static readonly` properties with all individual metrics that can be enabled on the driver. For instance, `NodeMetric.Counters.ConnectionInitErrors` is a `NodeMetric` object with a `Name` property that returns `errors.connection.init`.

Metrics are grouped into **buckets** and each bucket has a name. The most common scenario is to concatenate the bucket name with the metric name which is what the [App.Metrics provider] does. The bucket name is the name of the session (see `Builder.WithSessionName()`) and, in the case of node-level metrics, nodes followed by a textual representation of the node’s address. If the client application specifies a bucket prefix with `DriverMetricsOptions.SetBucketPrefix()`, the driver will prepend that prefix to the bucket name.

Here is an example of a session metric and a node metric without a bucket prefix and how their structure looks like:

| Bucket                            | Name                    | Metric                                 |
|-----------------------------------|-------------------------|----------------------------------------|
| `s0`                              | `connected-nodes`       | `SessionMetric.Gauges.ConnectedNodes`  |
| `s0.nodes.127_0_0_1:9042`         | `pool.open-connections` | `NodeMetric.Gauges.OpenConnections`    |

Here are the same examples but with a bucket prefix set in the `Builder`:

| Bucket                            | Name                    | Metric                                 |
|-----------------------------------|-------------------------|----------------------------------------|
| `prefix.s0`                       | `connected-nodes`       | `SessionMetric.Gauges.ConnectedNodes`  |
| `prefix.s0.nodes.127_0_0_1:9042`  | `pool.open-connections` | `NodeMetric.Gauges.OpenConnections`    |

## Configuration

Metrics are disabled by default. To enable them, use `Builder.WithMetrics()` method when creating the session.

When `Builder.WithMetrics(provider)` is called, the default metrics will be enabled. The default metrics are composed of every metric except those of type `Timer`. The reasoning behind this is the fact that enabling `Timer` metrics might increase CPU usage and impact throughput of the driver, so it's recommended to benchmark the client application with `Timer` metrics enabled before enabling them in production.

`Builder.WithMetrics(IDriverMetricsProvider,DriverMetricsOptions)` can be used to customize options related to metrics. `DriverMetricsOptions.SetEnabledNodeMetrics()` and `DriverMetricsOptions.SetEnabledSessionMetrics()` can be used to specify which metrics should be enabled. **Note that this will override the default enabled metrics**, i.e., if you want to enable `Timer` metrics on top of the default metrics then all metrics must be specified. `SessionMetric` and `NodeMetric` have a couple of `static` properties to make it easier to specify which metrics to enable. Here are some examples:

```csharp
// Enable all session metrics
builder.WithMetrics(provider, new DriverMetricsOptions().SetEnabledSessionMetrics(SessionMetric.AllSessionMetrics));

// Enable default session metrics (same effect as not calling the method at all)
builder.WithMetrics(provider, new DriverMetricsOptions().SetEnabledSessionMetrics(SessionMetric.DefaultSessionMetrics));

// Enable default session metrics except "bytes-sent", using LINQ
builder.WithMetrics(provider, new DriverMetricsOptions()
    .SetEnabledSessionMetrics(SessionMetric.DefaultSessionMetrics.Except(new [] { SessionMetric.Meters.BytesSent })));

// Enable default session metrics and 'cql-requests' timer metric, using LINQ
builder.WithMetrics(provider, new DriverMetricsOptions()
    .SetEnabledSessionMetrics(SessionMetric.DefaultSessionMetrics.Union(new [] { SessionMetric.Timers.CqlRequests })));
```

`DriverMetricsOptions.SetBucketPrefix` can be used to specify a custom prefix to be added to the bucket name of every metric. See the [previous section](#structure) for information about his.

The `provider` parameter must be an implementation of `IDriverMetricsProvider`. As was mentioned previouly, DataStax offers an [App.Metrics based implementation on a separate extension nuget package].

## Exporting metrics

With our App.Metrics provider, you can use the features offered by App.Metrics to export metrics. There are several reporter plugins that extend App.Metrics and these are available usually as nuget packages. For more information, see [the manual section related to this provider].

## Retrieving metrics

The driver exposes an API to retrieve metrics but this API consists of mostly abstractions. The available methods on each metric type will depend on the provider that is used (if the provider exposes such methods).

Note that if you are using our App.Metrics provider, you most likely don't need this API since App.Metrics offers built in reporters that export metrics to technologies like Graphite, InfluxDB, Elasticsearch and many others.

### Generic API for retrieving metrics

Here is an example of the generic API available no matter what provider is used:

```csharp
// Get metrics object
IDriverMetrics metrics = session.GetMetrics();

// Get session metrics and / or node metrics
IReadOnlyDictionary<Host, IMetricsRegistry<NodeMetric>> allNodeMetrics = metrics.NodeMetrics;
IMetricsRegistry<SessionMetric> sessionMetrics = metrics.SessionMetrics;

// Get specific host's node metrics
Host host = session.Cluster.AllHosts().First();
allNodeMetrics.TryGetValue(host, out IMetricsRegistry<NodeMetric> nodeMetrics);

// Get a specific metric of a specific host
IDriverMeter counter = metrics.GetNodeMetric<IDriverCounter>(host, NodeMetric.Counters.Errors);
```

Note that the driver's interface of each metric type is empty. This api exists to expose the metric implementations created by the metrics provider. You can set the generic parameter of the `GetNodeMetric` method to the provider's implementation type like the next section shows (with AppMetrics).

### Extension methods available in the DataStax provider based on AppMetrics

Refer to [the manual section related to this provider] for more information.

Here is a small example on how to use the provider extensions to retrieve metrics:

```csharp
// Get a specific metric of a specific host using the generic registry and converting it to the provider's type
IAppMetricsTimer cqlMessagesMetric = nodeMetrics.Timers[NodeMetric.Counters.Errors].ToAppMetricsTimer();

// Get a specific metric of a specific host (requires AppMetrics Provider)
IAppMetricsCounter counterAppMetrics = metrics.GetNodeMetric<IAppMetricsCounter>(host, NodeMetric.Counters.Errors);

// Get a specific metric of a specific host with AppMetrics extension method
IAppMetricsCounter counterAppMetrics = metrics.GetNodeCounter(host, NodeMetric.Counters.Errors);

// Get value of that metric (IAppMetricsCounter provides a GetValue() method)
long bytesSent = counterAppMetrics.GetValue();
```

## Providing your own implementation of `IDriverMetricsProvider`

If you want to use the driver's metrics feature with another third party library or even your own metrics implementation, you can look at our [provider's code] as an example on how to implement a provider.

There is no specific requirement for the implementation but note that the performance of the methods declared in `IDriverMetricsProvider` will affect the performance of the driver if metrics are enabled.

The instances returned by the `IDriverMetricsProvider` methods will be exposed by the generic API described in the [previous section](#retrieving-metrics).

[App.Metrics]: https://github.com/AppMetrics/AppMetrics
[Extension package]: app-metrics/index
[App.Metrics provider]: app-metrics/index
[App.Metrics based implementation on a separate extension nuget package]: app-metrics/index
[the manual section related to this provider]: app-metrics/index
[provider's code]: https://github.com/datastax/csharp-driver/tree/master/src/Extensions/Cassandra.AppMetrics


:::{toctree}
:hidden:
:maxdepth: 1

app-metrics/index
metrics-list/index
:::
