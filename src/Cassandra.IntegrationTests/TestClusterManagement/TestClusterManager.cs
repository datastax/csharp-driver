using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;

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

        public ITestCluster CreateNewClusterAndAddToList(string clusterName, int dc1NodeCount, string keyspaceName, bool isUsingDefaultConfig, bool initClusterAndSession, int maxTries)
        {
            return CreateNewClusterAndAddToList(clusterName, dc1NodeCount, 0, keyspaceName, isUsingDefaultConfig, initClusterAndSession, maxTries);
        }

        public ITestCluster CreateNewClusterAndAddToList(string clusterName, int dc1NodeCount, int dc2NodeCount, string keyspaceName, bool isUsingDefaultConfig, bool initClusterAndSession, int maxTries)
        {
            int tries = 0;
            while (tries < maxTries)
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
                        CcmCluster testClusterToAdd = new CcmCluster(TestUtils.GetTestClusterNameBasedOnCurrentEpochTime(), dc1NodeCount, dc2NodeCount, GetNextLocalIpPrefix(), DefaultKeyspaceName, isUsingDefaultConfig);
                        testClusterToAdd.Create(initClusterAndSession);
                        _testClusters.Add(testClusterToAdd);
                        return testClusterToAdd;
                    }
                }
                catch (Exception e)
                {
                    _logger.Error("Unexpected Error occurred while trying to get test cluster with nodeCount: " + dc1NodeCount);
                    _logger.Error("Error message: " + e.Message);
                    _logger.Error("Stack trace: " + e.StackTrace);
                    _logger.Error("Killing all Java processes and trying again ... ");
                }
                if (!UseCtool)
                {
                    KillAllCcmProcesses();
                }
                tries++;
            }
            return null;
        }

        // Create a "non default" test cluster that will not be available via the standard "GetTestCluster" command
        // NOTE: right now this returns a bare "TestCluster" object that has not been initialized in any way
        public ITestCluster GetNonShareableTestCluster(int dc1NodeCount, int dc2NodeCount, int maxTries = DefaultMaxClusterCmdRetries, bool initClusterAndSession = true)
        {
            ITestCluster nonShareableTestCluster = CreateNewClusterAndAddToList(
                TestUtils.GetTestClusterNameBasedOnCurrentEpochTime(), dc1NodeCount, dc2NodeCount, DefaultKeyspaceName, false, initClusterAndSession, maxTries);
            return nonShareableTestCluster;
        }

        public ITestCluster GetNonShareableTestCluster(int dc1NodeCount, int maxTries = DefaultMaxClusterCmdRetries, bool initClusterAndSession = true)
        {
            return GetNonShareableTestCluster(dc1NodeCount, 0, maxTries, initClusterAndSession);
        }

        // Get existing test cluster that can be shared, otherwise create a new one that can be shared.
        public ITestCluster GetTestCluster(int nodeCount, int maxTries = DefaultMaxClusterCmdRetries, bool initClusterAndSession = true, int retryCount = 0)
        {
            ITestCluster shareableTestCluster = GetExistingClusterWithNodeCount(nodeCount);

            // The following out is for debugging / session state experimentation purposes
            if (shareableTestCluster != null)
            {
                _logger.Info("Found existing test cluster with nodeCount: " + nodeCount + ", name: " + shareableTestCluster.Name);
                if (shareableTestCluster.Cluster.AllHosts().ToList().Count != nodeCount)
                    _logger.Warning("why is there a different number of actual hosts in the session than nodes assigned to the TestCluster ?");

                // make sure the existing TestCluster is running
                if (!UseCtool)
                {
                    StopAllCcmTestClustersExceptFor((CcmCluster)shareableTestCluster);
                }
                shareableTestCluster.SwitchToThisStartAndConnect();
            }

            // If no Test Cluster was found, then we need to create a new one.
            else
            {
                shareableTestCluster = CreateNewClusterAndAddToList(
                    TestUtils.GetTestClusterNameBasedOnCurrentEpochTime(), nodeCount, DefaultKeyspaceName, true, initClusterAndSession, 2);
            }

            return shareableTestCluster;
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

        public void RemoveExistingClusterWithNodeCount_Force(int nodeCount)
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
