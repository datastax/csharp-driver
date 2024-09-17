# Request Tracker

The driver provides the `IRequestTracker` interface that tracks the requests at Session and Node levels. It contains *start*, *finish*, and *error* events that can be subscribed by implementing the interface, and should be used by passing the implementation as an argument of the method `WithRequestTracker` that is available in the `Builder`.\
An example of an `IRequestTracker` implementation is the extension package `Cassandra.OpenTelemetry` that can be checked in the [documentation](/doc/features/opentelemetry/README.md).

## Available events

The full API doc is available [here](https://docs.datastax.com/en/drivers/csharp/latest/api/Cassandra.IRequestTracker.html) but the list below summarizes the events are available in the tracker:

- **OnStartAsync** - that is triggered when a session level request starts.
- **OnSuccessAsync** - that is triggered when the session level request finishes successfully.
- **OnErrorAsync** - that is triggered when the session level request finishes unsuccessfully.
- **OnNodeStartAsync** - that is triggered when the node request starts
- **OnNodeSuccessAsync** - that is triggered when the node level request finishes successfully.
- **OnNodeErrorAsync** - that is triggered when the node request finishes unsuccessfully.
