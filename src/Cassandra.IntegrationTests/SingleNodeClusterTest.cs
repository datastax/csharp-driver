using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;

namespace Cassandra.IntegrationTests
{
    [TestFixture]
    public class SingleNodeClusterTest
    {
        public virtual string CcmLocalConfigDir { get; set; }

        public virtual ISession Session { get; set; }

        [SetUp]
        public virtual void Setup()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            var keyspaceName = "tester";
            if (ConfigurationManager.AppSettings["UseRemote"] == "true")
            {
                CCMBridge.ReusableCCMCluster.Setup(1);
                CCMBridge.ReusableCCMCluster.Build(Cluster.Builder());
                Session = CCMBridge.ReusableCCMCluster.Connect(keyspaceName);
            }
            else
            {
                //Create a local instance
                CcmLocalConfigDir = TestUtils.CreateTempDirectory();
                var output = TestUtils.ExecuteLocalCcmClusterStart(CcmLocalConfigDir, "2.0.6");

                if (output.ExitCode != 0)
                {
                    throw new TestInfrastructureException("Local ccm could not start: " + output.ToString());
                }
                var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
                Session = cluster.Connect();
                Session.CreateKeyspaceIfNotExists(keyspaceName);
                Session.ChangeKeyspace(keyspaceName);
            }
        }

        [Test]
        public void TestConnections()
        {
            var rs = Session.Execute("SELECT * FROM system.schema_keyspaces");
            Assert.True(rs.Count() > 0);
        }

        [TearDown]
        public virtual void Teardown()
        {
            if (ConfigurationManager.AppSettings["UseRemote"] == "true")
            {
                CCMBridge.ReusableCCMCluster.Drop();
            }
            else
            {
                try
                {
                    //Try to close the connections
                    Session.Dispose();
                }
                catch
                {

                }
                //Remove the cluster
                TestUtils.ExecuteLocalCcmClusterRemove(CcmLocalConfigDir);
            }
        }
    }
}
