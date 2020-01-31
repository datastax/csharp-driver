//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    /// <summary>
    /// Test Helper class for keeping track of multiple CCM (Cassandra Cluster Manager) instances
    /// </summary>
    public static class TestClusterManager
    {
        public static ITestCluster LastInstance { get; private set; }
        public static TestClusterOptions LastOptions { get; private set; }

        public static int LastAmountOfNodes { get; private set; }
        public const string DefaultKeyspaceName = "test_cluster_keyspace";
        private static ICcmProcessExecuter _executor;

        private static readonly Version Version2Dot0 = new Version(2, 0);
        private static readonly Version Version2Dot1 = new Version(2, 1);
        private static readonly Version Version2Dot2 = new Version(2, 2);
        private static readonly Version Version3Dot0 = new Version(3, 0);
        private static readonly Version Version3Dot1 = new Version(3, 1);
        private static readonly Version Version3Dot11 = new Version(3, 11);
        private static readonly Version Version3Dot12 = new Version(3, 12);
        private static readonly Version Version4Dot0 = new Version(4, 0);
        private static readonly Version Version4Dot6 = new Version(4, 6);
        private static readonly Version Version4Dot7 = new Version(4, 7);
        private static readonly Version Version4Dot8 = new Version(4, 8);
        private static readonly Version Version5Dot0 = new Version(5, 0);
        private static readonly Version Version5Dot1 = new Version(5, 1);
        private static readonly Version Version6Dot0 = new Version(6, 0);
        private static readonly Version Version6Dot7 = new Version(6, 7);

        /// <summary>
        /// Gets the Cassandra version used for this test run
        /// </summary>
        public static Version CassandraVersion
        {
            get
            {
                if (IsDse)
                {
                    var dseVersion = DseVersion;
                    if (dseVersion < Version4Dot7)
                    {
                        // C* 2.0
                        return Version2Dot0;
                    }
                    if (dseVersion < Version5Dot0)
                    {
                        // C* 2.1
                        return Version2Dot1;
                    }
                    if (dseVersion < Version5Dot1)
                    {
                        // C* 3.0
                        return Version3Dot0;
                    }
                    if (dseVersion < Version6Dot0)
                    {
                        // C* 3.11
                        return Version3Dot11;
                    }
                    // C* 4.0
                    return Version4Dot0;
                }

                return new Version(TestClusterManager.CassandraVersionString);
            }
        }

        /// <summary>
        /// Gets the IP prefix for the DSE instances
        /// </summary>
        public static string IpPrefix
        {
            get { return Environment.GetEnvironmentVariable("DSE_INITIAL_IPPREFIX") ?? "127.0.0."; }
        }

        /// <summary>
        /// Gets the path to DSE source code
        /// </summary>
        public static string DsePath
        {
            get { return Environment.GetEnvironmentVariable("DSE_PATH"); }
        }

        public static string InitialContactPoint
        {
            get { return IpPrefix + "1"; }
        }

        public static string DseVersionString
        {
            get { return Environment.GetEnvironmentVariable("DSE_VERSION") ?? "6.7.7"; }
        }

        private static string CassandraVersionString
        {
            get { return Environment.GetEnvironmentVariable("CASSANDRA_VERSION") ?? "3.11.2"; }
        }

        public static bool IsDse
        {
            get { return Environment.GetEnvironmentVariable("DSE_VERSION") != null; }
        }
        
        public static Version DseVersion
        {
            get { return IsDse ? new Version(DseVersionString) : TestClusterManager.GetDseVersion(new Version(CassandraVersionString)); }
        }

        public static bool SupportsDecommissionForcefully()
        {
            return TestClusterManager.CheckDseVersion(new Version(5, 1), Comparison.GreaterThanOrEqualsTo);
        }

        public static bool SupportsNextGenGraph()
        {
            return TestClusterManager.CheckDseVersion(new Version(6, 8), Comparison.GreaterThanOrEqualsTo);
        }

        public static bool SchemaManipulatingQueriesThrowInvalidQueryException()
        {
            return TestClusterManager.CheckDseVersion(new Version(6, 8), Comparison.GreaterThanOrEqualsTo);
        }

        public static bool CheckDseVersion(Version version, Comparison comparison)
        {
            if (!TestClusterManager.IsDse)
            {
                return false;
            }

            return TestDseVersion.VersionMatch(version, TestClusterManager.DseVersion, comparison);
        }

        public static bool CheckCassandraVersion(bool requiresOss, Version version, Comparison comparison)
        {
            if (requiresOss && TestClusterManager.IsDse)
            {
                return false;
            }

            var runningVersion = TestClusterManager.IsDse ? TestClusterManager.DseVersion : TestClusterManager.CassandraVersion;
            var expectedVersion = TestClusterManager.IsDse ? TestClusterManager.GetDseVersion(version) : version;

            return TestDseVersion.VersionMatch(expectedVersion, runningVersion, comparison);
        }

        /// <summary>
        /// Get the ccm executor instance (local or remote)
        /// </summary>
        public static ICcmProcessExecuter Executor
        {
            get
            {
                if (TestClusterManager._executor != null)
                {
                    return TestClusterManager._executor;
                }

                if (bool.Parse(Environment.GetEnvironmentVariable("DSE_IN_REMOTE_SERVER") ?? "false"))
                {
                    var remoteDseServer = Environment.GetEnvironmentVariable("DSE_SERVER_IP") ?? "127.0.0.1";
                    var remoteDseServerUser = Environment.GetEnvironmentVariable("DSE_SERVER_USER") ?? "vagrant";
                    var remoteDseServerPassword = Environment.GetEnvironmentVariable("DSE_SERVER_PWD") ?? "vagrant";
                    var remoteDseServerPort = int.Parse(Environment.GetEnvironmentVariable("DSE_SERVER_PORT") ?? "2222");
                    var remoteDseServerUserPrivateKey = Environment.GetEnvironmentVariable("DSE_SERVER_PRIVATE_KEY");
                    TestClusterManager._executor = 
                        new RemoteCcmProcessExecuter(
                            remoteDseServer, remoteDseServerUser, remoteDseServerPassword, 
                            remoteDseServerPort, remoteDseServerUserPrivateKey);
                }
                else if (bool.Parse(Environment.GetEnvironmentVariable("CCM_USE_WSL") ?? "false"))
                {
                    TestClusterManager._executor = WslCcmProcessExecuter.Instance;
                }
                else
                {
                    TestClusterManager._executor = LocalCcmProcessExecuter.Instance;
                }

                return TestClusterManager._executor;
            }
        }

        public static Version GetDseVersion(Version cassandraVersion)
        {
            if (cassandraVersion < Version2Dot1)
            {
                // C* 2.0 => DSE 4.6
                return Version4Dot6;
            }
            if (cassandraVersion < Version2Dot2)
            {
                // C* 2.1 => DSE 4.8
                return Version4Dot8;
            }
            if (cassandraVersion < Version3Dot1)
            {
                // C* 3.0 => DSE 5.0
                return Version5Dot0;
            }
            if (cassandraVersion < Version3Dot12)
            {
                // C* 3.11 => DSE 5.1
                return Version5Dot1;
            }
            // DSE 6.0
            return Version6Dot0;
        }

        private static ITestCluster CreateNewNoRetry(int nodeLength, TestClusterOptions options, bool startCluster)
        {
            TryRemove();
            options = options ?? new TestClusterOptions();
            var testCluster = new CcmCluster(
                TestUtils.GetTestClusterNameBasedOnRandomString(),
                IpPrefix,
                DsePath,
                Executor,
                DefaultKeyspaceName,
                IsDse ? DseVersionString : CassandraVersionString);
            testCluster.Create(nodeLength, options);
            if (startCluster)
            {
                testCluster.Start(options.JvmArgs);
            }
            LastInstance = testCluster;
            LastAmountOfNodes = nodeLength;
            LastOptions = options;
            return testCluster;
        }

        /// <summary>
        /// Creates a new test cluster
        /// </summary>
        public static ITestCluster CreateNew(int nodeLength = 1, TestClusterOptions options = null, bool startCluster = true)
        {
            const int maxAttempts = 2;
            var attemptsSoFar = 0;
            while (true)
            {
                try
                {
                    return CreateNewNoRetry(nodeLength, options, startCluster);
                }
                catch (TestInfrastructureException ex)
                {
                    attemptsSoFar++;
                    if (attemptsSoFar >= maxAttempts)
                    {
                        throw;
                    }

                    Trace.WriteLine("Exception during ccm create / start. Retrying." + Environment.NewLine + ex.ToString());
                }
            }
        }

        /// <summary>
        /// Deprecated, use <see cref="TestClusterManager.CreateNew"/> method instead
        /// </summary>
        public static ITestCluster GetNonShareableTestCluster(int dc1NodeCount, int dc2NodeCount, int maxTries = 1, bool startCluster = true, bool initClient = true)
        {
            return GetTestCluster(dc1NodeCount, dc2NodeCount, false, maxTries, startCluster, initClient);
        }

        /// <summary>
        /// Deprecated, use <see cref="TestClusterManager.CreateNew"/> method instead
        /// </summary>
        public static ITestCluster GetNonShareableTestCluster(int dc1NodeCount, int maxTries = 1, bool startCluster = true, bool initClient = true)
        {
            if (startCluster == false)
                initClient = false;

            return GetTestCluster(dc1NodeCount, 0, false, maxTries, startCluster, initClient);
        }

        /// <summary>
        /// Deprecated, use <see cref="TestClusterManager.CreateNew"/> method instead
        /// </summary>
        public static ITestCluster GetTestCluster(int dc1NodeCount, int dc2NodeCount, bool shareable = true, int maxTries = 1, bool startCluster = true, bool initClient = true, int currentRetryCount = 0, string[] jvmArgs = null, bool useSsl = false)
        {
            var testCluster = CreateNew(
                dc1NodeCount,
                new TestClusterOptions
                {
                    Dc2NodeLength = dc2NodeCount,
                    UseSsl = useSsl,
                    JvmArgs = jvmArgs
                },
                startCluster);
            if (initClient)
            {
                testCluster.InitClient();
            }
            return testCluster;
        }

        /// <summary>
        /// Deprecated, use <see cref="TestClusterManager.CreateNew"/> method instead
        /// </summary>
        public static ITestCluster GetTestCluster(int dc1NodeCount, int maxTries = 1, bool startCluster = true, bool initClient = true)
        {
            return GetTestCluster(dc1NodeCount, 0, true, maxTries, startCluster, initClient);
        }

        /// <summary>
        /// Removes the current ccm cluster, without throwing exceptions if it fails
        /// </summary>
        public static void TryRemove()
        {
            try
            {
                Executor.ExecuteCcm("remove");
            }
            catch (Exception ex)
            {
                if (Diagnostics.CassandraTraceSwitch.Level == TraceLevel.Verbose)
                {
                    Trace.TraceError("ccm test cluster could not be removed: {0}", ex);   
                }
            }
        }
    }
}
