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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace ExecuteInLoop
{
    /// <summary>
    /// Inserts multiple rows in a table limiting the amount of parallel requests.
    ///
    /// You can also use existing libraries to limit concurrency in a loop, for example:
    /// - https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/dataflow-task-parallel-library
    /// </summary>
    internal class Program
    {
        private const string CqlQuery = "INSERT INTO tbl_sample_kv (id, value) VALUES (?, ?)";

        private ICluster _cluster;
        private ISession _session;
        private PreparedStatement _ps;

        private static void Main(string[] args)
        {
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            // feel free to change these two const variables
            const int maxConcurrencyLevel = 32;
            const int totalLength = 10000;

            Console.WriteLine($"MaxConcurrencyLevel={maxConcurrencyLevel}\tTotalLength={totalLength}");

            // build cluster
            _cluster =
                Cluster.Builder()
                    .AddContactPoint("127.0.0.1")
                    .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("datacenter1")))
                    .Build();

            // create session
            _session = await _cluster.ConnectAsync().ConfigureAwait(false);

            // prepare schema
            await _session.ExecuteAsync(new SimpleStatement("CREATE KEYSPACE IF NOT EXISTS examples WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }")).ConfigureAwait(false);
            await _session.ExecuteAsync(new SimpleStatement("USE examples")).ConfigureAwait(false);
            await _session.ExecuteAsync(new SimpleStatement("CREATE TABLE IF NOT EXISTS tbl_sample_kv(id uuid, value text, PRIMARY KEY(id))")).ConfigureAwait(false);

            // prepare query
            _ps = await _session.PrepareAsync(Program.CqlQuery).ConfigureAwait(false);

            var concurrencyLevel = maxConcurrencyLevel >= totalLength ? totalLength : maxConcurrencyLevel;

            // The maximum amount of async executions that are going to be launched in parallel at any given time
            var tasks = new List<Task>(concurrencyLevel);

            try
            {
                // Compute operations per Task (rounded up, so the first tasks will process more operations)
                var maxCount = (int)Math.Ceiling(totalLength / (double)concurrencyLevel);

                // Launch up to N Tasks in parallel (N being the max concurrency level)
                var k = 0;
                while (k < totalLength)
                {
                    var inc = maxCount;
                    if (k + maxCount > totalLength)
                    {
                        inc = (totalLength - k);
                    }

                    tasks.Add(ExecuteOneAtATimeAsync(k, inc));
                    k += inc;
                }

                Console.WriteLine($"Executing {k} queries with a concurrency level of {tasks.Count}, i.e., {tasks.Count} tasks. Each task will process up to {maxCount} operations.");

                // The task returned by Task.WhenAll completes when all the executions are completed.
                await Task.WhenAll(tasks).ConfigureAwait(false);

                Console.WriteLine($"Finished executing {k} queries with a concurrency level of {tasks.Count}.");
                Console.WriteLine("Press enter to exit.");
                Console.ReadLine();
            }
            finally
            {
                await _cluster.ShutdownAsync().ConfigureAwait(false);
            }
        }

        private async Task ExecuteOneAtATimeAsync(int i, int count)
        {
            foreach (var counter in Enumerable.Range(i, count))
            {
                var bs = _ps.Bind(Guid.NewGuid(), $"{counter}").SetIdempotence(true);
                await _session.ExecuteAsync(bs).ConfigureAwait(false);
            }
        }
    }
}