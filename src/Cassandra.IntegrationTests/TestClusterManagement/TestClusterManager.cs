using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework.Constraints;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    /// <summary>
    /// Test Helper class for keeping track of multiple CCM (Cassandra Cluster Manager) instances
    /// </summary>
    public class TestClusterManager : TestGlobals
    {
        private readonly Logger _logger = new Logger(typeof(TestClusterManager));
        private const int MaxClusterCreationRetries = 2;
        private readonly Mutex _mutex = new Mutex();
        private List<Tuple<int, int>> _ipPrefixesInUse = new List<Tuple<int, int>>();

        private List<ITestCluster> _testClusters = new List<ITestCluster>();

        public TestClusterManager()
        {
            if (UseCtool)
            {
                // manually add CToolClusters until CToolBridge is created
                CToolCluster cToolCluster;
                cToolCluster = new CToolCluster(TestUtils.GetTestClusterNameBasedOnCurrentEpochTime() + "_with4nodes", 4, DefaultKeyspaceName);
                cToolCluster.InitialContactPoint = "107.178.218.220";
                _testClusters.Add(cToolCluster);
                cToolCluster = new CToolCluster(TestUtils.GetTestClusterNameBasedOnCurrentEpochTime() + "_with2nodes", 2, DefaultKeyspaceName);
                cToolCluster.InitialContactPoint = "107.178.218.220";
                _testClusters.Add(cToolCluster);
            }
        }

        public ITestCluster CreateNewCluster(string clusterName, int dc1NodeCount, string keyspaceName, bool isUsingDefaultConfig, bool startCluster, int maxTries)
        {
            return CreateNewCluster(clusterName, dc1NodeCount, 0, keyspaceName, isUsingDefaultConfig, startCluster, maxTries);
        }

        public ITestCluster CreateNewCluster(string clusterName, int dc1NodeCount, int dc2NodeCount, string keyspaceName, bool isUsingDefaultConfig, bool startCluster, int maxTries)
        {
            try
            {
                if (UseCtool)
                {
                    // Create new cluster via ctool
                    // At this point we don't have the means to create remote 
                    throw new Exception("Setup FAIL: CTool cluster creation won't work until you create CToolBridge");
                }
                else
                {
                    // first stop any existing CCM clusters
                    ShutDownAllCcmTestClusters();
                    KillAllCcmProcesses();

                    // Create new cluster via ccm
                    CcmCluster testCluster = new CcmCluster(TestUtils.GetTestClusterNameBasedOnCurrentEpochTime(), dc1NodeCount, dc2NodeCount, GetNextLocalIpPrefix(), DefaultKeyspaceName, isUsingDefaultConfig);
                    testCluster.Create(startCluster);
                    return testCluster;
                }
            }
            catch (Exception e)
            {
                _logger.Error("Unexpected Error occurred while trying to get test cluster with nodeCount: " + dc1NodeCount);
                _logger.Error("Error message: " + e.Message);
                _logger.Error("Stack trace: " + e.StackTrace);
                _logger.Error("Killing all Java processes and trying again ... ");
            }
            
            return null;
        }

        // Create a "non default" test cluster that will not be available via the standard "GetTestCluster" command
        // NOTE: right now this returns a bare "TestCluster" object that has not been initialized in any way
        public ITestCluster GetNonShareableTestCluster(int dc1NodeCount, int dc2NodeCount, int maxTries = DefaultMaxClusterCreateRetries, bool startCluster = true, bool initClient = true)
        {
            // This is a non-shareable cluster with a potentially two DCs
            bool thisClusterShouldBeShareable = false;
            return GetTestCluster(dc1NodeCount, dc2NodeCount, thisClusterShouldBeShareable, maxTries, startCluster, initClient);
        }

        public ITestCluster GetNonShareableTestCluster(int dc1NodeCount, int maxTries = DefaultMaxClusterCreateRetries, bool startCluster = true, bool initClient = true)
        {
            if (startCluster == false)
                initClient = false;

            // This is a non-shareable cluster with a single DC
            bool thisClusterShouldBeShareable = false;
            int secondDcNodeCount = 0;
            return GetTestCluster(dc1NodeCount, secondDcNodeCount, thisClusterShouldBeShareable, maxTries, startCluster, initClient);
        }

        public ITestCluster GetTestCluster(int dc1NodeCount, int dc2NodeCount, bool shareable = true, int maxTries = DefaultMaxClusterCreateRetries, bool startCluster = true, bool initClient = true, int currentRetryCount = 0)
        {
            ITestCluster testCluster = null;
            if (shareable)
                testCluster = GetExistingClusterWithNodeCount(dc1NodeCount);

            // If we found a valid shareable test cluster, then switch to it and start it
            if (testCluster != null)
            {
                _logger.Info("Found existing test cluster with nodeCount: " + dc1NodeCount + ", name: " + testCluster.Name);
                if (testCluster.Cluster.AllHosts().ToList().Count != dc1NodeCount)
                    _logger.Warning("There a different number of actual hosts in the session than nodes assigned to the TestCluster ! ");

                // Make sure only the single TestCluster that we want running is running
                if (!UseCtool)
                    StopAllCcmTestClustersExceptFor((CcmCluster)testCluster);
                testCluster.SwitchToThisAndStart();
            }

            // If no Test Cluster was found, then we need to create a new one.
            else
            {
                testCluster = CreateNewCluster(
                    TestUtils.GetTestClusterNameBasedOnCurrentEpochTime(), dc1NodeCount, dc2NodeCount, DefaultKeyspaceName, shareable, startCluster, 2);
            }


            // if this fails, then remove the cluster and try another creating another one
            if (initClient)
            {
                if (!TryToInititalizeClusterClient(testCluster))
                {
                    testCluster.ShutDown();
                    KillAllCcmProcesses();
                    if (currentRetryCount > MaxClusterCreationRetries)
                        throw new Exception("Cluster with node count " + dc1NodeCount + " has already failed " + currentRetryCount + " times ... is there something wrong with this environment?");
                    testCluster = null; // signal that we need to try again
                }
            }

            // loop back, try again while we haven't exceeded max tries
            if (testCluster == null && currentRetryCount < maxTries)
                return GetTestCluster(dc1NodeCount, dc2NodeCount, shareable, maxTries, startCluster, initClient, ++currentRetryCount);

            // fail out if the test cluster was never created
            if (testCluster == null)
                throw new Exception("Test cluster was not created successfully!");

            // only add this to the test cluster list if its not there already
            if (!_testClusters.Contains(testCluster))
                _testClusters.Add(testCluster);

            return testCluster;
        }

        // Get existing test cluster that can be shared, otherwise create a new one that can be shared.
        public ITestCluster GetTestCluster(int dc1NodeCount, int maxTries = DefaultMaxClusterCreateRetries, bool startCluster = true, bool initClient = true)
        {
            // Assume this is a shareable cluster with a single DC
            bool thisClusterShouldBeShareable = true;
            int secondDcNodeCount = 0;
            return GetTestCluster(dc1NodeCount, secondDcNodeCount, thisClusterShouldBeShareable, maxTries, startCluster, initClient);
        }

        private bool TryToInititalizeClusterClient(ITestCluster testCluster)
        {
            try
            {
                foreach (string host in testCluster.ExpectedInitialHosts)
                    TestUtils.WaitForUp(host, DefaultCassandraPort, 15);

                // at this point we expect all the nodes to be up
                testCluster.InitClient();
                return true;
            }
            catch (Exception e)
            {
                _logger.Error("Unexpected Error occurred when trying to get shared test cluster with InitialContactPoint: " + testCluster.InitialContactPoint + ", name: " + testCluster.Name);
                _logger.Error("Error Message: " + e.Message);
                _logger.Error("Error Stack Trace: " + e.StackTrace);
                _logger.Error("Removing this cluster, and looping back to create a new one ... ");
            }
            return false;
        }

        public void KillAllCcmProcesses()
        {
            // TODO: get Jenkins Proc ID, make sure the proc ID you're killing isn't Jenkins
            Process[] procs = Process.GetProcessesByName("java");
            if (procs.Length > 0)
                _logger.Warning("found " + procs.Length + " java procs that are about to be killed ... ");
            foreach (Process proc in procs)
            {
                _logger.Warning(string.Format("KILLING process with ID: {0}, and name: {1}", proc.Id, proc.MachineName));
                try
                {
                    proc.Kill();
                }
                catch (Exception e)
                {
                    _logger.Error("FAILED to kill process ID: " + proc.Id);
                    _logger.Error("Exception Message: " + e.Message);
                }
            }
        }

        public int CountCcmProcesses()
        {
            // TODO: get Jenkins Proc ID, don't count it
            int ccmProcessCount = Process.GetProcessesByName("java").Length;
            return ccmProcessCount;
        }

        public void WaitForCcmProcessesToGoAway(int msToWait)
        {
            DateTime futureDateTime = DateTime.Now.AddMilliseconds(msToWait);
            while (CountCcmProcesses() > 0 && DateTime.Now < futureDateTime)
            {
                int sleepMs = 500;
                _logger.Warning("Found Ccm Processes still running! Sleeping for " + sleepMs + " MS ... ");
                Thread.Sleep(sleepMs);
            }
        }

        public ITestCluster GetExistingClusterWithNodeCount(int nodeCount)
        {
            foreach (ITestCluster existingTestCluster in _testClusters)
            {
                if (existingTestCluster.Dc1NodeCount == nodeCount && existingTestCluster.IsUsingDefaultConfig == true)
                {
                    // First make sure there is not an existing cluster running with a different number of nodes
                    if (!existingTestCluster.IsBeingCreated)
                    {
                        WaitForTestClusterToInitialize(existingTestCluster);
                        if (!existingTestCluster.IsCreated && existingTestCluster.IsBeingCreated)
                            throw new Exception(string.Format("Test cluster with did not start after the max allowed milliseconds: {1}", ClusterInitSleepMsMax));
                    }
                    return existingTestCluster;
                }
            }
            return null;
        }

        public void RemoveShareableClusterWithNodeCount(int nodeCount)
        {
            for (int i = 0; i < _testClusters.Count(); i++)
            {
                ITestCluster existingTestCluster = _testClusters[i];
                if (existingTestCluster.Dc1NodeCount == nodeCount && existingTestCluster.IsUsingDefaultConfig == true)
                {
                    try
                    {
                        existingTestCluster.Remove();
                    }
                    catch (Exception e)
                    {
                        _logger.Error("Unexpected Error occurred when trying to get shared test cluster with nodeCount: " + nodeCount + ", name: " + existingTestCluster.Name);
                        _logger.Error("Error Message: " + e.Message);
                        _logger.Error("Error Stack Trace: " + e.StackTrace);
                        _logger.Error("Forcibly removing the test cluster from the list anyway ... ");
                    }
                    _testClusters.Remove(existingTestCluster);
                }
            }
        }


        private void WaitForTestClusterToInitialize(ITestCluster testCluster)
        {
            int millisecondsSlept = 0;
            while (testCluster.IsBeingCreated && millisecondsSlept < ClusterInitSleepMsMax)
            {
                Console.WriteLine(string.Format("Cluster is initializing, sleeping {1} seconds before attempting to use it ...", ClusterInitSleepMsPerIteration));
                Thread.Sleep(ClusterInitSleepMsPerIteration);
                millisecondsSlept += ClusterInitSleepMsPerIteration;
            }
        }

        public void ShutDownAllCcmTestClusters(bool throwOnError = false)
        {
            foreach (ITestCluster existingTestCluster in _testClusters)
            {
                if (existingTestCluster.IsStarted)
                {
                    try
                    {
                        _logger.Info("Shutting down cluster: " + existingTestCluster.Name + " initial contact point: " + existingTestCluster.InitialContactPoint);
                        existingTestCluster.SwitchToThisCluster();
                        existingTestCluster.ShutDown();
                    }
                    catch (Exception e)
                    {
                        _logger.Error("Unexpected Error caught when shutting down cluster with name: " + existingTestCluster.Name + " initial contact point: " +existingTestCluster.InitialContactPoint);
                        _logger.Error("Error message: " + e.Message);
                        _logger.Error("Error stack trace: " + e.StackTrace);
                        if (throwOnError)
                        {
                            throw e;
                        }
                    }
                }
            }
        }

        public void RemoveAllTestClusters()
        {
            for (int j=_testClusters.Count - 1 ; j >= 0; j--)
            {
                ITestCluster existingTestCluster = _testClusters[j];
                if (!existingTestCluster.IsRemoved)
                {
                    _logger.Info("Removing cluster: " + existingTestCluster.Name + " initial contact point: " +
                                 existingTestCluster.InitialContactPoint);
                    existingTestCluster.Remove();
                }
                // Remove from Test Cluster Manager list
                _testClusters[j].Remove();
            }
        }

        private void StopAllCcmTestClustersExceptFor(CcmCluster ccmClusterWeDontWantToStop)
        {
            // if we are checking
            foreach (ITestCluster existingTestCluster in _testClusters)
            {
                if (existingTestCluster.GetType() == typeof (CcmCluster))
                {
                    CcmCluster currentlyRunningCcmCluster = (CcmCluster) existingTestCluster;
                    if (currentlyRunningCcmCluster.Name != ccmClusterWeDontWantToStop.Name && 
                        currentlyRunningCcmCluster.CcmBridge.CcmDir != ccmClusterWeDontWantToStop.CcmBridge.CcmDir && 
                        currentlyRunningCcmCluster.IsStarted)
                    {
                        existingTestCluster.ShutDown();
                    }
                }
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

    }
}
