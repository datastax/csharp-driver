using Cassandra;
using Cassandra.OpenTelemetry;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace ConsoleExporter
{
    internal class Program
    {
        private const string ContactPoint = "localhost";
        private const string SessionName = "otel-example";

        private static void Main(string[] args)
        {
            Program.MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            using var tracerProvider = Sdk.CreateTracerProviderBuilder()
             .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(
                 serviceName: "CassandraDemo",
                 serviceVersion: "1.0.0"))
             .AddSource(CassandraInstrumentation.ActivitySourceName)
             .AddConsoleExporter()
             .Build();

            var cluster = Cluster.Builder()
                .AddContactPoint(Program.ContactPoint)
                .WithSessionName(Program.SessionName)
                .AddOpenTelemetryInstrumentation(options => options.IncludeDatabaseStatement = true)
                .Build();

            var session = await cluster.ConnectAsync().ConfigureAwait(false);

            //// Execute a query every 5 seconds
            
            var cts = new CancellationTokenSource();
            var task = Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        var x = await session.ExecuteAsync(new SimpleStatement("SELECT * FROM system.local"));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"ERROR: {ex}");
                    }

                    Thread.Sleep(5000);
                }
            });

            Console.WriteLine("Press enter to shutdown the session and exit.");
            Console.ReadLine();

            cts.Cancel();

            await task.ConfigureAwait(false);
            await cluster.ShutdownAsync().ConfigureAwait(false);
        }
    }
}
