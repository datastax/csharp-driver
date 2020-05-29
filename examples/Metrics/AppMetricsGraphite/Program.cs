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

using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using App.Metrics;
using App.Metrics.Scheduling;

using Cassandra;
using Cassandra.Metrics;

namespace AppMetricsGraphite
{
    /// <summary>
    /// Sample application that enables all metrics and exports them to Graphite.
    /// 
    /// To setup graphite and grafana, launch these containers:
    ///
    /// <code>
    /// docker run -d --name graphite -p 80:80 -p 2003-2004:2003-2004 -p 2023-2024:2023-2024 -p 8125:8125/udp -p 8126:8126 graphiteapp/graphite-statsd
    /// docker run -d --name grafana -p 3000:3000 grafana/grafana
    /// </code>
    /// 
    /// Then:
    /// 1 - Load up grafana http://127.0.0.1:3000
    /// 2 - Login with admin/admin
    /// 3 - Create a graphite datasource with http://127.0.0.1:80 url (use browser access if needed)
    /// 4 - Import this dashboard: https://grafana.com/grafana/dashboards/11090 (set graphite datasource to the one you created previously)
    ///
    /// If you already use port 80 for something else, map the graphite container's 80 port to another one and use that when creating the datasource on grafana.
    /// </summary>
    internal class Program
    {
        private const int GraphiteUpdateIntervalMilliseconds = 5000;
        private const string ContactPoint = "127.0.0.1";
        private const string LocalDatacenter = "datacenter1";
        private const string SessionName = "metrics-example";
        private const string GraphiteIp = "127.0.0.1";
        private const int GraphitePort = 2003;
        
        private static readonly IPEndPoint GraphiteEndpoint = new IPEndPoint(IPAddress.Parse(Program.GraphiteIp), Program.GraphitePort);

        private static void Main(string[] args)
        {
            Program.MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            //// App Metrics configuration

            // Build metrics root
            var metrics =
                new MetricsBuilder()
                    .Report.ToGraphite($"net.tcp://{Program.GraphiteEndpoint}")
                    .Build();

            // Build and run scheduler
            var scheduler = new AppMetricsTaskScheduler(
                TimeSpan.FromMilliseconds(Program.GraphiteUpdateIntervalMilliseconds),
                async () => { await Task.WhenAll(metrics.ReportRunner.RunAllAsync()).ConfigureAwait(false); });

            scheduler.Start();

            //// DataStax C# Driver configuration
            
            Cassandra.Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Warning;
            Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));

            var cluster = Cluster.Builder()
                .AddContactPoint(Program.ContactPoint)
                .WithLocalDatacenter(Program.LocalDatacenter)
                .WithSessionName(Program.SessionName)
                .WithMetrics(
                    metrics.CreateDriverMetricsProvider(), 
                    new DriverMetricsOptions()
                        .SetEnabledNodeMetrics(NodeMetric.AllNodeMetrics)
                        .SetEnabledSessionMetrics(SessionMetric.AllSessionMetrics))
                .Build();

            var session = await cluster.ConnectAsync().ConfigureAwait(false);


            //// Run some queries to have metrics data
            
            var cts = new CancellationTokenSource();
            var task = Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        await session.ExecuteAsync(
                            new SimpleStatement("SELECT * FROM system.local")).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"ERROR: {ex}");
                    }
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