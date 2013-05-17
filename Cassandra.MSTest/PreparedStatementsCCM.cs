using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

#if MYTEST
using MyTest;
using System.Threading;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif


namespace Cassandra.MSTest
{
    
    public partial class PreparedStatementsCCMTests
    {     
        string Keyspace = "tester";
        Cluster Cluster;
        CCMBridge.CCMCluster CCMCluster;
        Session Session;


        public PreparedStatementsCCMTests()
        {
        }

        [TestInitialize]
        public void SetFixture()
        {
            CCMCluster = CCMBridge.CCMCluster.Create(2, Cluster.Builder());
            Session = CCMCluster.Session;
            Cluster = CCMCluster.Cluster;
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMCluster.Discard();
        }

        private void reprepareOnNewlyUpNodeTest(bool useKeyspace)
        {
            Session.CreateKeyspaceIfNotExists(Keyspace);
            Thread.Sleep(1000);
            string modifiedKs = "";

            if (useKeyspace)
                Session.ChangeKeyspace(Keyspace);
            else
                modifiedKs = Keyspace + ".";

            try
            {
                Session.Execute("CREATE TABLE " + modifiedKs + "test(k text PRIMARY KEY, i int)");
            }
            catch (AlreadyExistsException)
            {
            }
            Thread.Sleep(1000);
            Session.Execute("INSERT INTO " + modifiedKs +"test (k, i) VALUES ('123', 17)");
            Session.Execute("INSERT INTO " + modifiedKs +"test (k, i) VALUES ('124', 18)");

            PreparedStatement ps = Session.Prepare("SELECT * FROM " + modifiedKs + "test WHERE k = ?");

            Assert.Equal(Session.Execute(ps.Bind("123")).GetRows().First().GetValue<int>("i"), 17); // ERROR

            CCMCluster.CassandraCluster.Stop();            
            TestUtils.waitForDown(CCMBridge.IP_PREFIX + "1", Cluster, 20);            

            CCMCluster.CassandraCluster.Start();
            TestUtils.waitFor(CCMBridge.IP_PREFIX + "1", Cluster, 20);

            try
            {
                Assert.Equal(Session.Execute(ps.Bind("124")).GetRows().First().GetValue<int>("i"), 18);
            }
            catch (NoHostAvailableException e)
            {
                Debug.WriteLine(">> " + e.Errors);
                throw e;
            }
        }
    }
}
