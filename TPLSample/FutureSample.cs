using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;

namespace TPLSample.FutureSample
{
    public class schema_keyspaces
    {
        public bool durable_writes { get; set; }

        public string keyspace_name { get; set; }

        public string strategy_class { get; set; }

        public string strategy_options { get; set; }
    }

    public static class FutureSample
    {
        public static void Run()
        {

            Cluster cluster = Cluster.Builder().AddContactPoint("cassi.cloudapp.net").WithoutRowSetBuffering().Build();

            using (var session = cluster.Connect("system"))
            {

                var context = new Context(session);
                context.AddTable<schema_keyspaces>();

                var cqlKeyspaces = context.GetTable<schema_keyspaces>();

                var allResults = new List<Task<List<schema_keyspaces>>>();
                for (int i = 0; i < 100; ++i)
                {
                    var futRes = Task<IEnumerable<schema_keyspaces>>.Factory.FromAsync(cqlKeyspaces.BeginExecute, cqlKeyspaces.EndExecute, ConsistencyLevel.Default, null)
                        .ContinueWith(a => a.Result.ToList());
                    allResults.Add(futRes);
                }

                foreach (var result in allResults)
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
                foreach (var resKeyspace in result.Result)
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