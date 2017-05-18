//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Dse.Test.Integration.TestClusterManagement
{
    /// <summary>
    /// Test Helper class for keeping track of multiple CCM (Cassandra Cluster Manager) instances
    /// </summary>
    public class TestClusterManager
    {
        public static ITestCluster LastInstance { get; private set; }
        public const string DefaultKeyspaceName = "test_cluster_keyspace";
        private static ICcmProcessExecuter _executor;

        private static readonly Version Version2Dot0 = new Version(2, 0);
        private static readonly Version Version2Dot1 = new Version(2, 1);
        private static readonly Version Version2Dot2 = new Version(2, 2);
        private static readonly Version Version3Dot0 = new Version(3, 0);
        private static readonly Version Version3Dot10 = new Version(3, 10);
        private static readonly Version Version4Dot6 = new Version(4, 6);
        private static readonly Version Version4Dot7 = new Version(4, 7);
        private static readonly Version Version4Dot8 = new Version(4, 8);
        private static readonly Version Version5Dot0 = new Version(5, 0);
        private static readonly Version Version5Dot1 = new Version(5, 1);

        /// <summary>
        /// Gets the Cassandra version used for this test run
        /// </summary>
        public static Version CassandraVersion
        {
            get
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
                // C* 3.10
                return Version3Dot10;
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
            get { return Environment.GetEnvironmentVariable("DSE_VERSION") ?? "5.0.0"; }
        }

        public static Version DseVersion
        {
            get { return new Version(DseVersionString); }
        }

        /// <summary>
        /// Get the ccm executor instance (local or remote)
        /// </summary>
        public static ICcmProcessExecuter Executor
        {
            get
            {
                if (_executor != null)
                {
                    return _executor;
                }
                var dseRemote = bool.Parse(Environment.GetEnvironmentVariable("DSE_IN_REMOTE_SERVER") ?? "true");
                if (!dseRemote)
                {
                    _executor = LocalCcmProcessExecuter.Instance;
                }
                else
                {
                    var remoteDseServer = Environment.GetEnvironmentVariable("DSE_SERVER_IP") ?? "127.0.0.1";
                    var remoteDseServerUser = Environment.GetEnvironmentVariable("DSE_SERVER_USER") ?? "vagrant";
                    var remoteDseServerPassword = Environment.GetEnvironmentVariable("DSE_SERVER_PWD") ?? "vagrant";
                    var remoteDseServerPort = int.Parse(Environment.GetEnvironmentVariable("DSE_SERVER_PORT") ?? "2222");
                    var remoteDseServerUserPrivateKey = Environment.GetEnvironmentVariable("DSE_SERVER_PRIVATE_KEY");
                    _executor = new RemoteCcmProcessExecuter(remoteDseServer, remoteDseServerUser, remoteDseServerPassword,
                        remoteDseServerPort, remoteDseServerUserPrivateKey);
                }
                return _executor;
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
            if (cassandraVersion < Version3Dot10)
            {
                // C* 3.0 => DSE 5.0
                return Version5Dot0;
            }
            // DSE 5.1
            return Version5Dot1;
        }

        /// <summary>
        /// Creates a new test cluster
        /// </summary>
        public static ITestCluster CreateNew(int nodeLength = 1, TestClusterOptions options = null, bool startCluster = true)
        {
            TryRemove();
            options = options ?? new TestClusterOptions();
            var testCluster = new CcmCluster(
                TestUtils.GetTestClusterNameBasedOnTime(), 
                IpPrefix, 
                DsePath, 
                Executor,
                DefaultKeyspaceName,
                DseVersionString);
            testCluster.Create(nodeLength, options);
            if (startCluster)
            {
                testCluster.Start(options.JvmArgs);   
            }
            LastInstance = testCluster;
            return testCluster;
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
        public ITestCluster GetTestCluster(int dc1NodeCount, int maxTries = 1, bool startCluster = true, bool initClient = true)
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
