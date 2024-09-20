# Distributed Tracing example

There are two projects here:

- Api: An Asp Net Core web API that connects to an Apache Cassandra compatible cluster on the localhost using the driver
- Client: A Console Application that performs an HTTP request to the API every 5 seconds

Launch the Api first, then launch the Client application. 

The Api has the OpenTelemetry Console Exporter enabled and the Client prints the trace id before and after sending a request so you can see the trace id being propagated from the client to the driver in real time by looking at both console outputs.

## Using an OTLP Exporter

If you want to try this example with an OTLP exporter you can uncomment this line in the Program.cs file of both projects:

`//.AddOtlpExporter(opt => opt.Endpoint = new Uri("http://localhost:4317")) // uncomment if you want to use an OTPL exporter like Jaeger
`

For example, you can run Jaeger locally using the following command (Jaeger UI will be listening on `localhost:16686`, you can open it with your browser):

`docker run -d -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 -p 16686:16686 -p 4317:4317 -p 4318:4318 -p 9411:9411 jaegertracing/all-in-one:latest`