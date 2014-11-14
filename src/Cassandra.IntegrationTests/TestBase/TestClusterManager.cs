using Cassandra.IntegrationTests.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Cassandra.IntegrationTests
{
    /// <summary>
    /// Test Helper class for keeping track of multiple CCM (Cassandra Cluster Manager) instances
    /// </summary>
    public class TestClusterManager : TestGlobals
    {
        public bool useCtool = false;
        private static Logger logger = new Logger(typeof(TestClusterManager));
        private static Mutex mutex = new Mutex();
        private static List<Tuple<int, int>> ipPrefixesInUse = new List<Tuple<int, int>>();

        List<TestCluster> testClusters = new List<TestCluster>();

        public TestClusterManager(bool useCtool_in = false)
        {
            useCtool = useCtool_in;
            if (useCtool)
            {
                testClusters.Add(new TestCluster(TEST_CLUSTER_NAME_DEFAULT + "_with4nodes", 4, "107.178.218.220", TEST_KEYSPACE_DEFAULT));
                testClusters.Add(new TestCluster(TEST_CLUSTER_NAME_DEFAULT + "_with2nodes", 2, "107.178.218.220", TEST_KEYSPACE_DEFAULT));
            }
        }

        public TestCluster getTestCluster(int nodeCount)
        {
            foreach (TestCluster existingTestCluster in testClusters) {
                if (existingTestCluster.nodeCount == nodeCount)
                {
                    // TODO: First make sure there is not an existing cluster running with a different number of nodes
                    if (!existingTestCluster.isInitializing)
                    {
                        WaitForTestClusterToInitialize(existingTestCluster);
                        if (!existingTestCluster.isInitialized && existingTestCluster.isInitializing)
                            throw new Exception(string.Format("Test cluster with did not start after the max allowed milliseconds: {1}", CLUSTER_INIT_SLEEP_MS_MAX));
                    }
                    return existingTestCluster;
                }
            }

            // We must need to create, connect to a new test C* cluster
            if (useCtool)
            {
                // Create new cluster via ctool
                throw new Exception("Setup FAIL: Auto cluster creation won't work until you create CToolBridge");
            }
            else
            {
                // Create new cluster via ccm
                TestCluster testClusterToAdd = new TestCluster(TEST_CLUSTER_NAME_DEFAULT, nodeCount, IpPrefix + "1", TEST_KEYSPACE_DEFAULT);
                testClusterToAdd.initialize_ccm();
                WaitForTestClusterToInitialize(testClusterToAdd);
                testClusters.Add(testClusterToAdd);
                return testClusterToAdd;
            }
        }

        private static void WaitForTestClusterToInitialize(TestCluster testCluster)
        {
            int millisecondsSlept = 0;
            while (testCluster.isInitializing && millisecondsSlept < CLUSTER_INIT_SLEEP_MS_MAX)
            {
                logger.Info(string.Format("Cluster is initializing, sleeping {1} seconds before attempting to use it ...", CLUSTER_INIT_SLEEP_MS_PER_ITERATION));
                Thread.Sleep(CLUSTER_INIT_SLEEP_MS_PER_ITERATION);
                millisecondsSlept += CLUSTER_INIT_SLEEP_MS_PER_ITERATION;
            }
        }

        // This method was created with the assumption that ccm can create multiple local instances, but unfortunately this is not the case
        private static string getNextLocalIpPrefix()
        {
            // in the case of concurrent threads, we don't want to try and attach multiple local clusters
            // to the same local ip
            mutex.WaitOne();

            string nextAvailableLocalIpPrefix = null;
            for (int octet1 = 0; octet1 < 255 && nextAvailableLocalIpPrefix == null; octet1++)
            {
                for (int octet2 = 0; octet2 < 255 && nextAvailableLocalIpPrefix == null; octet2++)
                {
                    Tuple<int, int> currentTupleBeingChecked = new Tuple<int, int>(octet1, octet2);
                    if (!ipPrefixesInUse.Contains(currentTupleBeingChecked))
                    {
                        ipPrefixesInUse.Add(currentTupleBeingChecked);
                        nextAvailableLocalIpPrefix = formatOctetSetForLocalIpPrefix(currentTupleBeingChecked);
                    }
                }
            }
            if (nextAvailableLocalIpPrefix == null)
                throw new Exception("We have run out of local IP prefixes!");

            mutex.ReleaseMutex();
            return nextAvailableLocalIpPrefix;
        }

        private static string formatOctetSetForLocalIpPrefix(Tuple<int, int> octetSet)
        {
            return string.Format("{0}.{1}.{2}.", 127, octetSet.Item1, octetSet.Item2);
        }

        public void removeAllClusters()
        {
            foreach (TestCluster testCluster in testClusters)
            {
                testCluster.tearDown();
            }
        }
    }


}
