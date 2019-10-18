# List of metrics provided by the driver

In this section you can find a description of each metric type used by the driver and a list of all metrics provided by the driver.

## Metric types

| Metric Type        | Description               |
|--------------------|---------------------------|
| Counter | Counters are atomic `long` values which can be incremented. |
| Gauge | A Gauge is simply an action that returns the instantaneous measure of a value, where the value abitrarily increases and decreases. |
| Meter | A Meter measures the rate at which an event occurs along with a total count of the occurances. |
| Timer | A Timer allows us to measure the duration of a type of event, the rate of its occurrence and provide duration statistics. |

## Session level metrics

| Name              | Type  | C# Property                            | Description               |
|-------------------|-------|----------------------------------------|---------------------------|
| `connected-nodes` | Gauge | `SessionMetric.Gauges.ConnectedNodes`  | The number of nodes to which the driver has at least one active connection. |
| `cql-client-timeouts` | Counter | `SessionMetric.Counters.CqlClientTimeouts`  | The number of timeouts in synchronous API calls like `Session.Execute()` (related to `QueryAbortTimeout`). |
| `bytes-received` | Meter | `SessionMetric.Meters.BytesReceived`  | The number and rate of bytes received for the entire session.  |
| `bytes-sent` | Meter | `SessionMetric.Meters.BytesSent`  | The number and rate of bytes sent for the entire session.  |
| `cql-requests` | Timer | `SessionMetric.Timers.CqlRequests`  | The throughput and latency percentiles of CQL requests (overall duration of the `Session.Execute()` call). |

## Node level metrics

| Name              | Type  | C# Property                            | Description               |
|-------------------|-------|----------------------------------------|---------------------------|
| `pool.open-connections` | Gauge | `NodeMetric.Gauges.OpenConnections` | The number of connections open to this node for regular requests. |
| `pool.in-flight` | Gauge | `NodeMetric.Gauges.InFlight` | The number of requests currently executing on the connections to this node. |
| `cql-messages` | Timer | `NodeMetric.Timers.CqlMessages` | The throughput and latency percentiles of individual CQL messages sent to this node as part of an overall request. |
| `bytes-sent` | Meter | `NodeMetric.Meters.BytesSent` | The number and rate of bytes sent to this node. |
| `bytes-received` | Meter | `NodeMetric.Meters.BytesReceived` | The number and rate of bytes received from this node. |
| `speculative-executions` | Counter | `NodeMetric.Counters.SpeculativeExecutions` | The number of speculative executions triggered by a slow response from this node. |
| `errors.connection.init` | Counter | `NodeMetric.Counters.ConnectionInitErrors` | The number of errors encountered while trying to establish a connection to this node. Authentication errors are tracked separately in `errors.connection.auth`. |
| `errors.connection.auth` | Counter | `NodeMetric.Counters.AuthenticationErrors` | The number of authentication errors encountered while trying to establish a connection to this node. |
| `errors.request.read-timeouts` | Counter | `NodeMetric.Counters.ReadTimeouts` | The number of times this node replied with a `READ_TIMEOUT` error. |
| `errors.request.write-timeouts` | Counter | `NodeMetric.Counters.WriteTimeouts` | The number of times this node replied with a `WRITE_TIMEOUT` error. |
| `errors.request.unavailables` | Counter | `NodeMetric.Counters.UnavailableErrors` | The number of times this node replied with an `UNAVAILABLE` error. |
| `errors.request.others` | Counter | `NodeMetric.Counters.OtherErrors` | The number of times this node replied with a server error that doesn't fall under other `errors.request.*` metrics. |
| `errors.request.aborted` | Counter | `NodeMetric.Counters.AbortedRequests` | The number of times a request was aborted before the driver even received a response from this node. Client timeouts are  |
| `errors.request.unsent` | Counter | `NodeMetric.Counters.UnsentRequests` | The number of times the driver failed to send a request to this node. |
| `errors.request.client-timeouts` | Counter | `NodeMetric.Counters.ClientTimeouts` | The number of times the request failed due to a client-side timeout, when the client didn't hear back from the server within `SocketOptions.ReadTimeoutMillis`. In this scenario, an `OperationTimedOutException` is thrown. |
| `retries.read-timeout` | Counter | `NodeMetric.Counters.RetriesOnReadTimeout` | The number of **read timeout** errors on this node that caused the RetryPolicy to trigger a retry. |
| `retries.write-timeout` | Counter | `NodeMetric.Counters.RetriesOnWriteTimeout` | The number of **write timeout** errors on this node that caused the RetryPolicy to trigger a retry. |
| `retries.unavailable` | Counter | `NodeMetric.Counters.RetriesOnUnavailable` | The number of **unavailable** errors on this node that caused the RetryPolicy to trigger a retry. |
| `retries.other` | Counter | `NodeMetric.Counters.RetriesOnOtherError` | The number of errors **other** than **read timeouts**, **write timeouts** and **unavailable** errors on this node that caused the RetryPolicy to trigger a retry. |
| `retries.total` | Counter | `NodeMetric.Counters.Retries` | The total number of retries triggered by the RetryPolicy. |
| `ignores.read-timeout` | Counter | `NodeMetric.Counters.IgnoresOnReadTimeout` | The number of **read timeout** errors on this node that were ignored by the RetryPolicy. |
| `ignores.write-timeout` | Counter | `NodeMetric.Counters.IgnoresOnWriteTimeout` | The number of **write timeout** errors on this node that were ignored by the RetryPolicy. |
| `ignores.unavailable` | Counter | `NodeMetric.Counters.IgnoresOnUnavailable` | The number of **unavailable** errors on this node that were ignored by the RetryPolicy. |
| `ignores.other` | Counter | `NodeMetric.Counters.IgnoresOnOtherError` | The number of errors **other** than **read timeouts**, **write timeouts** and **unavailable** errors on this node that were ignored by the RetryPolicy. |
| `ignores.total` | Counter | `NodeMetric.Counters.Ignores` | The total number of errors on this node that were ignored by the RetryPolicy. |
