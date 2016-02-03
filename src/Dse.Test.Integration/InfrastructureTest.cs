using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;
using Dse.Test.Integration.ClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration
{
    public class InfrastructureTest : BaseIntegrationTest
    {
        [Test]
        public void Test_Infrastructure()
        {
            CcmHelper.Start(1);
            using (var cluster = Cluster.Builder().AddContactPoint(CcmHelper.InitialContactPoint).Build())
            {
                Assert.DoesNotThrow(() => cluster.Connect());
            }
            CcmHelper.Remove();
        }
    }
}
