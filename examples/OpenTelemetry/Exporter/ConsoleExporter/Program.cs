//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

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
             .AddSource(CassandraActivitySourceHelper.ActivitySourceName)
             .AddConsoleExporter()
             .Build();

            var cluster = Cluster.Builder()
                .AddContactPoint(Program.ContactPoint)
                .WithSessionName(Program.SessionName)
                .WithOpenTelemetryInstrumentation(options => options.IncludeDatabaseStatement = true)
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
                        var x = await session.ExecuteAsync(new SimpleStatement("SELECT key FROM system.local WHERE key='local'"));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"ERROR: {ex}");
                    }

                    await Task.Delay(5000).ConfigureAwait(false);
                }
            });

            Console.WriteLine("Press enter to shutdown the session and exit.");
            Console.ReadLine();

            await cts.CancelAsync().ConfigureAwait(false);

            await task.ConfigureAwait(false);
            await session.ShutdownAsync().ConfigureAwait(false);
            await cluster.ShutdownAsync().ConfigureAwait(false);
        }
    }
}
