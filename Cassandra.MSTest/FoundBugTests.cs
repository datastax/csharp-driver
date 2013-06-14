using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MyTest;

namespace Cassandra.MSTest
{
    public class FoundBugTests
    {
        [TestMethod]
        [NeedSomeFix]
        public void Jira_CSHARP_40()
        //During reconnect the tablespace name becomes invalid
        {
            var CCMCluster = CCMBridge.CCMCluster.Create(2, Cluster.Builder());
            try
            {
                var Session = CCMCluster.Session;
                var Keyspace = "Excelsior";
                Session.CreateKeyspaceIfNotExists(Keyspace);
                Session.ChangeKeyspace(Keyspace);
                const string cqlKeyspaces = "SELECT * from system.schema_keyspaces";
                var query = new SimpleStatement(cqlKeyspaces).EnableTracing();
                {
                    var result = Session.Execute(query);

                    foreach (var resKeyspace in result.GetRows())
                    {
                        Console.WriteLine("durable_writes={0} keyspace_name={1} strategy_Class={2} strategy_options={3}",
                                          resKeyspace.GetValue<bool>("durable_writes"),
                                          resKeyspace.GetValue<string>("keyspace_name"),
                                          resKeyspace.GetValue<string>("strategy_class"),
                                          resKeyspace.GetValue<string>("strategy_options"));
                    }
                }

                CCMCluster.CassandraCluster.ForceStop(1);
                CCMCluster.CassandraCluster.ForceStop(2);
                TestUtils.waitForDown(CCMBridge.IP_PREFIX + "1", CCMCluster.Cluster, 40);
                TestUtils.waitForDown(CCMBridge.IP_PREFIX + "2", CCMCluster.Cluster, 40);

                try
                {
                    var result = Session.Execute(query);

                    foreach (var resKeyspace in result.GetRows())
                    {
                    }
                }
                catch (Exception)
                {
                }

                CCMCluster.CassandraCluster.Start(1);
                TestUtils.waitFor(CCMBridge.IP_PREFIX + "1", CCMCluster.Cluster, 60);

                {
                    var result = Session.Execute(query);

                    foreach (var resKeyspace in result.GetRows())
                    {
                        Console.WriteLine("durable_writes={0} keyspace_name={1} strategy_Class={2} strategy_options={3}",
                                          resKeyspace.GetValue<bool>("durable_writes"),
                                          resKeyspace.GetValue<string>("keyspace_name"),
                                          resKeyspace.GetValue<string>("strategy_class"),
                                          resKeyspace.GetValue<string>("strategy_options"));
                    }
                }

            }
            finally
            {
                CCMCluster.Discard();
            }
        }
    }
}
