# Connection heartbeat

If a connection stays idle for too long, it could be dropped by intermediate network devices (for example, routers, firewalls, etc.). Normally, TCP keep-alive should take care of this; but tweaking low-level keep-alive settings might be impractical in some environments. The driver provides application-side keep-alive in the form of a
connection heartbeat: if a connection has been idle for a given amount of time, the driver simulates activity by writing a dummy request to the connection.

This feature is enabled by default since version 3.0 of the driver. The default heartbeat interval is 30 seconds, but this can be customized with the [SetHeartBeatInterval()][setheartbeat-api] method of the pooling options:

```csharp
poolingOptions.SetHeartBeatInterval(60000);
```

The heartbeat interval should be set higher than `SocketOptions.ReadTimeoutMillis`: the read timeout is the maximum time that the driver waits for a regular query to complete, therefore the connection should not be considered idle before it has elapsed.

To disable heartbeat, set the interval to `0`.

[setheartbeat-api]: https://docs.datastax.com/en/drivers/csharp/latest/api/Cassandra.PoolingOptions.html#Cassandra_PoolingOptions_SetHeartBeatInterval_System_Int32_
