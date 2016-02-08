using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Graph;
using Dse.Test.Integration.ClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration.Graph
{
    public class GraphTests : BaseIntegrationTest
    {
        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
            CcmHelper.Start(1);
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            CcmHelper.Remove();
        }

        [Test]
        public void Should_Execute_Simple_Graph_Query()
        {
            using (var cluster = DseCluster.Builder().AddContactPoint(CcmHelper.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                //create graph1
                session.ExecuteGraph(new SimpleGraphStatement("system.createGraph('graph1').ifNotExist().build()"));
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()").SetGraphName("graph1"));
                Assert.NotNull(rs);
            }
        }
    }
}
