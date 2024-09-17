# Distributed Tracing example

There are two projects here:

- Api: An Asp Net Core web API that connects to an Apache Cassandra compatible cluster on the localhost using the driver
- Client: A Console Application that performs an HTTP request to the API every 5 seconds

Launch the Api first, then launch the Client application. 

Both of these applications have the OpenTelemetry Console Exporter enabled so you can see the trace id being propagated from the client to the driver in real time by looking at both console outputs.