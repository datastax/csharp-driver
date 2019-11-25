# Query timestamps

In Apache Cassandra, each mutation has a microsecond-precision timestamp, which is used to order operations relative to
each other.

The timestamp can be provided by the client or assigned server-side based on the time the server processes the request.

Letting the server assign the timestamp can be a problem when the order of the writes matter: with unlucky
timing (different coordinators, network latency, etc.), two successive requests from the same client might be
processed in a different order server-side, and end up with out-of-order timestamps.

## Client-side generation

### Using a timestamp generator

When using Apache Cassandra 2.1+ or DataStax Enterprise 4.7+, it's possible to send the operation timestamp in the
request. Starting from version 3.3 of the C# driver, the driver uses `AtomicMonotonicTimestampGenerator` 
by default to generate the request timestamps.

You can provide a different generator when building the `ICluster` instance:

```csharp
var cluster = Cluster.Builder()
                        .WithTimestampGenerator(generator)
                        .AddContactPoint("host1")
                        .Build()
```

To implement a custom timestamp generator, you must implement `ITimestampGenerator` interface.

In addition, you can also set the default timestamp on a per-execution basis in the query options using the
`IStatement.SetTimestamp()` method.

#### Accuracy

The driver provides 2 built-in implementations:

- `AtomicMonotonicTimestampGenerator` that uses `DateTime.UtcNow.Ticks` to generate timestamps. The precision
of `UtcNow` is system dependent and unspecified, which in summary can be around 15.6ms.
- `AtomicMonotonicWinApiTimestampGenerator` that uses Win API's [`GetSystemTimeAsFileTime()`][win-api] to generate
timestamps. This method call is only available when running on Windows 8+ / Windows Server 2012+ and [according
to the documentation][win-api], the precision is higher than 1us.

#### Monotonicity

`AtomicMononoticTimestampGenerator` and `AtomicMonotonicWinApiTimestampGenerator` implementations also guarantee
that the returned timestamps will always be monotonically increasing, even if multiple updates happen under the
same millisecond.

Note that to guarantee such monotonicity, if more than one thousand timestamps are generated within the same
millisecond, or in the event of a system clock skew, _the implementation might return timestamps that drift out into
the future_. When this happens, the built-in generator logs a periodic warning message. See their non-default
constructors for ways to control the warning interval.

[win-api]: https://msdn.microsoft.com/en-us/library/windows/desktop/hh706895.aspx


### Provide the timestamp in the query

Alternatively, if you are using a lower server version, you can explicitly provide the timestamp in your CQL query:

```csharp
session.Execute("INSERT INTO my_table (c1, c2) VALUES (1, 1) USING TIMESTAMP 1482156745633040");
```