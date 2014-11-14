using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Threading;
using System.Globalization;

namespace Cassandra.IntegrationTests
{
    [SetUpFixture]
    public class TestBase : TestGlobals
    {
        private static Logger logger = new Logger(typeof(TestBase));
        private TestClusterManager clusterManager = null;
        private static bool clusterManagerIsInitializing = false;
        private static bool clusterManagerIsInitalized = false;

        public TestBase() {}

        public TestClusterManager testClusterManager
        {
            get
            {
                if (clusterManagerIsInitalized)
                    return clusterManager;
                else if (clusterManagerIsInitializing)
                {
                    while (clusterManagerIsInitializing)
                    {
                        int sleepMs = 1000;
                        logger.Info("Shared " + clusterManagerIsInitializing.GetType().Name + " object is initializing. Sleeping " + sleepMs + " MS ... ");
                        Thread.Sleep(sleepMs);
                    }
                }
                else
                {
                    clusterManagerIsInitializing = true;
                    clusterManager = new TestClusterManager(UseCtool);
                    clusterManagerIsInitializing = false;
                    clusterManagerIsInitalized = true;
                }
                return clusterManager;
            }
        }

        [TearDown]
        public void TearDownTestSuite() 
        {
            Console.WriteLine("This should run after all tests are complete ...");
            // testClusterManager.removeAllClusters();
        }

        [SetUp]
        public void SetupTestSuite()
        {
            Console.WriteLine("This should only once, before all tests ...");
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
        }


    }

    
}
