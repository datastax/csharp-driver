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
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace MultipleKeyspacesSingleSession
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            // async over sync is bad, your app will probably have true async support so I wanted the example to be async
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            using var cluster = Cluster.Builder()
                .AddContactPoint("127.0.0.1")
                .Build();

            using var session = await cluster.ConnectAsync().ConfigureAwait(false);

            var keyspaces = new[]
            {
                "ks1",
                "ks2",
                "ks3"
            };

            foreach (var ks in keyspaces)
            {
                session.Execute(
                    $"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = " +
                    "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
                session.Execute(
                    $"CREATE TABLE IF NOT EXISTS {ks}.users (id uuid PRIMARY KEY, name text)");
            }

            var mapperManager = new MapperManager(session);

            // launch 4 tasks, each task inserts 8 users sequentially on a random keyspace (out of the 3 above)
            var tasks = Enumerable.Range(0, 4).Select(taskIdx => Task.Run(async () =>
            {
                // lazy way to create a random object per task without risking reusing the same seed, for testing purposes only
                var r = new Random(Guid.NewGuid().GetHashCode());
                foreach (var i in Enumerable.Range(0, 8))
                {
                    var ksIndex = r.Next() % keyspaces.Length;
                    var ks = keyspaces[ksIndex];
                    var mapper = mapperManager.GetMapperForKeyspace(ks);
                    var newUserId = Guid.NewGuid();
                    await mapper.InsertAsync(new User { Id = newUserId, Name = $"User Task={taskIdx} Idx={i} Keyspace={ks}" }).ConfigureAwait(false);
                }
            }));

            await Task.WhenAll(tasks).ConfigureAwait(false);

            foreach (var ks in keyspaces)
            {
                await PrintUsersAsync(mapperManager, ks).ConfigureAwait(false);
            }
        }

        public static async Task PrintUsersAsync(MapperManager mapperManager, string keyspace)
        {
            var users = await mapperManager.GetMapperForKeyspace(keyspace).FetchAsync<User>().ConfigureAwait(false);
            Console.WriteLine($"###### KEYSPACE {keyspace}");
            foreach (var user in users)
            {
                Console.WriteLine($"NAME='{user.Name}' ID={user.Id}");
            }
        }
    }
}
