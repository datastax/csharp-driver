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
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;

//based on https://github.com/pchalamet/cassandra-sharp/tree/master/Samples
namespace TPLSample.KeyspacesSample
{
    public static class KeyspacesSample
    {
        public static void Run()
        {
            // this sample requires buffering
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();

            using (var session = cluster.Connect())
            {
                const string cqlKeyspaces = "SELECT * from system.schema_keyspaces";

                var query = new SimpleStatement(cqlKeyspaces).EnableTracing();

                var allTasks = new List<Task>();
                for (int i = 0; i < 100; ++i)
                {
                    var futRes = Task<RowSet>.Factory.FromAsync(session.BeginExecute, session.EndExecute, cqlKeyspaces, session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(), null)
                        .ContinueWith(t => DisplayKeyspace(t.Result));
                    allTasks.Add(futRes);
                }

                Task.WaitAll(allTasks.ToArray());
            }

            cluster.Shutdown();
        }

        private static void DisplayKeyspace(RowSet result)
        {
            try
            {
                foreach (var resKeyspace in result.GetRows())
                {
                    Console.WriteLine("durable_writes={0} keyspace_name={1} strategy_Class={2} strategy_options={3}",
                                      resKeyspace.GetValue<bool>("durable_writes"), 
                                      resKeyspace.GetValue<string>("keyspace_name"), 
                                      resKeyspace.GetValue<string>("strategy_class"), 
                                      resKeyspace.GetValue<string>("strategy_options"));
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