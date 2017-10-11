using System;
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
            if (Environment.GetEnvironmentVariable("TEST_TRACE")?.ToUpper() == "ON")
            {
                Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));
            }
            Trace.TraceInformation("Starting Test Run ...");
        }

        [OneTimeTearDown]
        public void TearDownTestSuite()
        {
            // this method is executed once after all the fixtures have completed execution
            TestClusterManager.TryRemove();
            SimulacronManager.Instance.Stop();
        }
    }
}
