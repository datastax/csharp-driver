using System.Diagnostics;
using System.Globalization;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;

// This namespace must remain at the top level of integration tests
namespace Cassandra.IntegrationTests
{
    [SetUpFixture]
    public class TestSuiteBase : TestGlobals
    {
        public TestSuiteBase() { }

        [TearDown]
        public void TearDownTestSuite()
        {
            if (TestClusterManager != null)
            {
                Trace.TraceInformation("In final tear-down method, shutting down shared " + TestClusterManager.GetType().Name + " object");
                //TestClusterManager.ShutDownAllCcmTestClusters();
                TestClusterManager.RemoveAllTestClusters();
            }
        }

        [SetUp]
        public void SetupTestSuite()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en-US");
            Trace.TraceInformation("TestBase Setup Complete. Starting Test Run ...");
        }



    }

    
}
