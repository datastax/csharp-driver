using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;

namespace TPLSample.KeyspacesSample
{
    public static class KeyspacesSample
    {
        public static void Run()
        {
            // this sample requires buffering
            Cluster cluster = Cluster.Builder().AddContactPoint("cassi.cloudapp.net").Build();

            using (var session = cluster.Connect())
            {
                const string cqlKeyspaces = "SELECT * from system.schema_keyspaces";

                var query = new SimpleStatement(cqlKeyspaces).EnableTracing();

                var rowset = session.Execute(query);
                var trace = rowset.QueryTrace;

                var coord = trace.Coordinator;

                var allTasks = new List<Task>();
                for (int i = 0; i < 100; ++i)
                {
                    var futRes = Task<CqlRowSet>.Factory.FromAsync(session.BeginExecute, session.EndExecute, cqlKeyspaces, ConsistencyLevel.Default, null)
                        .ContinueWith(t => DisplayKeyspace(t.Result));
                    allTasks.Add(futRes);
                }

                Task.WaitAll(allTasks.ToArray());
            }

            cluster.Shutdown();
        }

        private static void DisplayKeyspace(CqlRowSet result)
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