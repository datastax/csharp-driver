//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;

//based on https://github.com/pchalamet/cassandra-sharp/tree/master/Samples

namespace TPLSample.FutureSample
{
    public static class FutureSample
    {
        public static void Run()
        {
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").WithoutRowSetBuffering().Build();

            using (var session = cluster.Connect("system"))
            {
                Table<schema_keyspaces> cqlKeyspaces = session.GetTable<schema_keyspaces>();
                var allResults = new List<Task<List<schema_keyspaces>>>();

                for (int i = 0; i < 100; ++i)
                {
                    Task<List<schema_keyspaces>> futRes = Task<IEnumerable<schema_keyspaces>>.Factory.FromAsync(cqlKeyspaces.BeginExecute,
                                                                                                                cqlKeyspaces.EndExecute, null)
                                                                                             .ContinueWith(a => a.Result.ToList());
                    allResults.Add(futRes);
                }

                foreach (Task<List<schema_keyspaces>> result in allResults)
                {
                    DisplayKeyspace(result);
                }
            }

            cluster.Shutdown();
        }

        private static void DisplayKeyspace(Task<List<schema_keyspaces>> result)
        {
            try
            {
                foreach (schema_keyspaces resKeyspace in result.Result)
                {
                    Console.WriteLine("DurableWrites={0} KeyspaceName={1} strategy_Class={2} strategy_options={3}",
                                      resKeyspace.durable_writes, resKeyspace.keyspace_name, resKeyspace.strategy_class, resKeyspace.strategy_options);
                }
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Command failed {0}", ex.Message);
            }
        }
    }
}