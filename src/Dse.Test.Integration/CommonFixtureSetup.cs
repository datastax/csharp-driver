using System.Diagnostics;
using System.Globalization;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests
{
    [SetUpFixture]
    public class CommonFixtureSetup
    {
        [OneTimeSetUp]
        public void SetupTestSuite()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            Trace.TraceInformation("TestBase Setup Complete. Starting Test Run ...");
        }

        [OneTimeTearDown]
        public void TearDownTestSuite()
        {
            // this method is executed once after all the fixtures have completed execution
            TestClusterManager.TryRemove();
        }
    }
}
