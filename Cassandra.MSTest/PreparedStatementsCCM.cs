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

    [TestClass]
    public partial class PreparedStatementsCCMTests
    {     
        Session Session;


        public PreparedStatementsCCMTests()
        {
        }

        [TestInitialize]
        public void SetFixture()
        {
            CCMBridge.ReusableCCMCluster.Setup(2);
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder());
            Session = CCMBridge.ReusableCCMCluster.Connect();
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMBridge.ReusableCCMCluster.Drop();
        }

        private void reprepareOnNewlyUpNodeTest(bool useKeyspace)
        {
            string keyspace = "tester";
            Session.CreateKeyspaceIfNotExists(keyspace);
            string modifiedKs = "";

            if (useKeyspace)
                Session.ChangeKeyspace(keyspace);
            else
                modifiedKs = keyspace + ".";

            try
            {
                Session.Cluster.WaitForSchemaAgreement(
                    Session.Execute("CREATE TABLE " + modifiedKs + "test(k text PRIMARY KEY, i int)")
                );
            }
            catch (AlreadyExistsException)
            {
            }
            Session.Execute("INSERT INTO " + modifiedKs +"test (k, i) VALUES ('123', 17)");
            Session.Execute("INSERT INTO " + modifiedKs +"test (k, i) VALUES ('124', 18)");

            PreparedStatement ps = Session.Prepare("SELECT * FROM " + modifiedKs + "test WHERE k = ?");

            using (var rs = Session.Execute(ps.Bind("123")))
            {
                Assert.Equal(rs.GetRows().First().GetValue<int>("i"), 17); // ERROR
            }
            CCMBridge.ReusableCCMCluster.CCMBridge.Stop();            
            TestUtils.waitForDown(Options.Default.IP_PREFIX + "1", Session.Cluster, 20);

            CCMBridge.ReusableCCMCluster.CCMBridge.Start();
            TestUtils.waitFor(Options.Default.IP_PREFIX + "1", Session.Cluster, 20);

            try
            {
                using (var rowset = Session.Execute(ps.Bind("124")))
                {
                    Assert.Equal(rowset.GetRows().First().GetValue<int>("i"), 18);
                }
            }
            catch (NoHostAvailableException e)
            {
                Debug.WriteLine(">> " + e.Errors);
                throw e;
            }
        }
    }
}
