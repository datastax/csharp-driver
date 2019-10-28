# DataStax Metrics Provider based on App.Metrics

This documentation refers to our provider based on App.Metrics. The code examples shown here only apply if this provider is used.

## Installation

This provider is available on the [CassandraCSharpDriver.AppMetrics nuget package].

```
PM> Install-Package CassandraCSharpDriver.AppMetrics
```

## Using this provider to enable metrics

This package exposes an extension method of `App.Metrics.IMetricsRoot` that can be used to create the provider:

```csharp
var metricsRoot = new App.Metrics.MetricsBuilder().Build();

var cluster = Cluster
    .Builder()
    .AddContactPoint("127.0.0.1")
    .WithMetrics(metricsRoot.CreateDriverMetricsProvider())
    .Build();
```

## Exporting

[App.Metrics] offers a variety of integrations available through extension nuget packages. There are extensions to help you integrate App.Metrics with Graphite, Prometheus, InfluxDB, Elasticsearch and more. There's also an extension to integrate App.Metrics with ASP .NET Core.

For more information, check out the [official App.Metrics documentation].

Here's a small example to export metrics to Graphite every 5 seconds:

```csharp
var metricsRoot = new MetricsBuilder()
    .Report.ToGraphite("net.tcp://127.0.0.1:2003")
    .Build();

var cluster = Cluster
    .Builder()
    .AddContactPoint("127.0.0.1")
    .WithMetrics(metricsRoot.CreateDriverMetricsProvider())
    .Build();

var scheduler = new AppMetricsTaskScheduler(
    TimeSpan.FromMilliseconds(5000),
    async () => { await Task.WhenAll(metricsRoot.ReportRunner.RunAllAsync()); });

scheduler.Start();
```

## Advanced configuration for Timer metrics

This provider uses a custom implementation of `App.Metrics.ReservoirSampling.IReservoir` when creating `Timer` metrics. This custom implementation is based on [HdrHistogram].

There are some parameters that can be set when creating `HdrHistogram` objects and these parameters can be customized with an alternative extension method:

```csharp
var metricsRoot = new App.Metrics.MetricsBuilder().Build();

var cluster = Cluster
    .Builder()
    .AddContactPoint("127.0.0.1")
    .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(10000))
    .WithMetrics(metricsRoot.CreateDriverMetricsProvider(
        new DriverAppMetricsOptions()
            .SetHighestLatencyMilliseconds(15000) // should be set to a value that is higher than the configured timeout
            .SetSignificantDigits(3)
            .SetTimersTimeUnit(TimeUnit.Nanoseconds) // which unit should be used for the Timer metrics
    ))
    .Build();
```

`HighestLatencyMilliseconds` should be set to a higher value than the configured timeouts (`SocketOptions.ReadTimeoutMillis` or `Builder.WithQueryAbortTimeout`). This is used to scale internal data structures and whenever a measurement exceeds this value, a warning will be logged and the measurement will be discarded. It defaults to 30000, i.e., 30 seconds.

`SignificantDigits` is the number of significant decimal digits to which internal structures will maintain value resolution and separation (for example, 3 means that recordings up to 1 second will be recorded with a resolution of 1 millisecond or better). This must be between 0 and 5. If the value is out of range, an exception is thrown. It defaults to 3.

`TimersTimeUnit` is the `App.Metrics.TimeUnit` value that will be passed to `App.Metrics` when creating `Timer` metrics. This will determine the unit of the exported timer values.

## Retrieving metrics

The API that was described in the previous [section](#Exporting) is usually enough for most use cases. In addition to that, we expose an API to manually retrieve metrics which is covered in this section.

### Retrieving metrics with the provider's interfaces

The driver's interfaces for each metric type are empty so, in order to be able to retrieve the metric values, we have to retrieve the metric objects with the appropriate provider type.

You can obtain metric objects with the appropriate provider type by directly specifying that type as the generic parameter. For example, you can obtain a `IAppMetricsCounter` like this:

```csharp
var metrics = session.GetMetrics();
var appMetricsCounter = metrics.GetSessionMetric<IAppMetricsCounter>(SessionMetric.Counters.CqlClientTimeouts);
var value = appMetricsCounter.GetValue();
```

This package exposes a set of extension methods so that you don't have to specify the provider metric type as the generic parameter:

```csharp
var appMetricsCounter = metrics.GetSessionCounter(SessionMetric.Counters.CqlClientTimeouts);
```

There are extensions like the previous one for all metric types and also for node metrics:

```csharp
public static IAppMetricsCounter GetNodeCounter(this IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric);
public static IAppMetricsGauge GetNodeGauge(this IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric);
public static IAppMetricsMeter GetNodeMeter(this IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric);
public static IAppMetricsTimer GetNodeTimer(this IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric);
public static IAppMetricsCounter GetSessionCounter(this IDriverMetrics driverMetrics, SessionMetric sessionMetric);
public static IAppMetricsGauge GetSessionGauge(this IDriverMetrics driverMetrics, SessionMetric sessionMetric);
public static IAppMetricsMeter GetSessionMeter(this IDriverMetrics driverMetrics, SessionMetric sessionMetric);
public static IAppMetricsTimer GetSessionTimer(this IDriverMetrics driverMetrics, SessionMetric sessionMetric);
```

### Converting the driver abstraction interfaces to the provider's interfaces

This package exposes several extension methods that can be used to convert the generic driver metric interfaces to this provider's interfaces which expose methods to obtain the metric values.

For example, you can obtain a `IDriverCounter` (which has an empty interface) through the metrics registry and convert it to a `IAppMetricsCounter` instance that has a `GetValue()` method.

```csharp
var metrics = session.GetMetrics();
var sessionMetricsRegistry = metrics.SessionMetrics;
sessionMetricsRegistry.Counters.TryGetValue(SessionMetric.Counters.CqlClientTimeouts, out var driverCounter);
var appMetricsCounter = driverCounter.ToAppMetricsCounter();
var value = appMetricsCounter.GetValue();
```

There are extensions like the previous one for all metric types:

```csharp
public static IAppMetricsCounter ToAppMetricsCounter(this IDriverCounter counter);
public static IAppMetricsGauge ToAppMetricsGauge(this IDriverGauge gauge);
public static IAppMetricsMeter ToAppMetricsMeter(this IDriverMeter meter);
public static IAppMetricsTimer ToAppMetricsTimer(this IDriverTimer timer);
```

[CassandraCSharpDriver.AppMetrics nuget package]: https://www.nuget.org/packages/CassandraCSharpDriver.AppMetrics/
[HdrHistogram]: https://github.com/HdrHistogram/HdrHistogram.NET
[official App.Metrics documentation]: https://www.app-metrics.io/
[App.Metrics]: https://github.com/AppMetrics/AppMetrics
