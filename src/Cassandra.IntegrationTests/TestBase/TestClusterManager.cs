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
        private static readonly Logger Logger = new Logger(typeof(TestClusterManager));
        private readonly Mutex _mutex = new Mutex();
        private List<Tuple<int, int>> _ipPrefixesInUse = new List<Tuple<int, int>>();

        private List<TestCluster> _testClusters = new List<TestCluster>();

        public TestClusterManager()
        {
            if (UseCtool)
            {
                _testClusters.Add(new TestCluster(TestClusterNameDefault + "_with4nodes", 4, "107.178.218.220", TestKeyspaceDefault));
                _testClusters.Add(new TestCluster(TestClusterNameDefault + "_with2nodes", 2, "107.178.218.220", TestKeyspaceDefault));
            }
        }

        public TestCluster GetTestCluster(int nodeCount)
        {
            foreach (TestCluster existingTestCluster in _testClusters) {
                if (existingTestCluster.NodeCount == nodeCount)
                {
                    // First make sure there is not an existing cluster running with a different number of nodes
                    if (!existingTestCluster.IsInitializing)
                    {
                        WaitForTestClusterToInitialize(existingTestCluster);
                        if (!existingTestCluster.IsInitialized && existingTestCluster.IsInitializing)
                            throw new Exception(string.Format("Test cluster with did not start after the max allowed milliseconds: {1}", ClusterInitSleepMsMax));
                    }
                    return existingTestCluster;
                }
            }

            // We must need to create, connect to a new test C* cluster
            if (UseCtool)
            {
                // Create new cluster via ctool
                throw new Exception("Setup FAIL: Auto cluster creation won't work until you create CToolBridge");
            }
            else
            {
                // Create new cluster via ccm
                TestCluster testClusterToAdd = new TestCluster(TestClusterNameDefault, nodeCount, IpPrefix + "1", TestKeyspaceDefault);
                testClusterToAdd.InitializeCcm();
                WaitForTestClusterToInitialize(testClusterToAdd);
                _testClusters.Add(testClusterToAdd);
                return testClusterToAdd;
            }
        }

        private static void WaitForTestClusterToInitialize(TestCluster testCluster)
        {
            int millisecondsSlept = 0;
            while (testCluster.IsInitializing && millisecondsSlept < ClusterInitSleepMsMax)
            {
                Logger.Info(string.Format("Cluster is initializing, sleeping {1} seconds before attempting to use it ...", ClusterInitSleepMsPerIteration));
                Thread.Sleep(ClusterInitSleepMsPerIteration);
                millisecondsSlept += ClusterInitSleepMsPerIteration;
            }
        }

        // This method was created with the assumption that ccm can create multiple local instances, but unfortunately this is not the case
        private string GetNextLocalIpPrefix()
        {
            // in the case of concurrent threads, we don't want to try and attach multiple local clusters
            // to the same local ip
            _mutex.WaitOne();

            string nextAvailableLocalIpPrefix = null;
            for (int octet1 = 0; octet1 < 255 && nextAvailableLocalIpPrefix == null; octet1++)
            {
                for (int octet2 = 0; octet2 < 255 && nextAvailableLocalIpPrefix == null; octet2++)
                {
                    Tuple<int, int> currentTupleBeingChecked = new Tuple<int, int>(octet1, octet2);
                    if (!_ipPrefixesInUse.Contains(currentTupleBeingChecked))
                    {
                        _ipPrefixesInUse.Add(currentTupleBeingChecked);
                        nextAvailableLocalIpPrefix = FormatOctetSetForLocalIpPrefix(currentTupleBeingChecked);
                    }
                }
            }
            if (nextAvailableLocalIpPrefix == null)
                throw new Exception("We have run out of local IP prefixes!");

            _mutex.ReleaseMutex();
            return nextAvailableLocalIpPrefix;
        }

        private static string FormatOctetSetForLocalIpPrefix(Tuple<int, int> octetSet)
        {
            return string.Format("{0}.{1}.{2}.", 127, octetSet.Item1, octetSet.Item2);
        }

        public void RemoveAllClusters()
        {
            foreach (TestCluster testCluster in _testClusters)
            {
                testCluster.TearDown();
            }
        }
    }


}
