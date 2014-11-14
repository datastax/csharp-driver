using System;
using NUnit.Framework;
using System.Threading;
using System.Globalization;

namespace Cassandra.IntegrationTests
{
    [SetUpFixture]
    public class TestBase : TestGlobals
    {
        private static Logger _logger = new Logger(typeof(TestBase));

        public TestBase() {}

        [TearDown]
        public void TearDownTestSuite()
        {
            Console.WriteLine("This should run after all tests are complete ...");
            if (TestClusterManager != null)
                TestClusterManager.RemoveAllClusters();
        }

        [SetUp]
        public void SetupTestSuite()
        {
            Console.WriteLine("This should only once, before all tests ...");
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
        }


    }

    
}
