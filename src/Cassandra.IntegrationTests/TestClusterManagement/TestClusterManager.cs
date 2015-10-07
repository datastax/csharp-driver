using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    /// <summary>
    /// Test Helper class for keeping track of multiple CCM (Cassandra Cluster Manager) instances
    /// </summary>
    public class TestClusterManager
    {
        public const string DefaultKeyspaceName = "test_cluster_keyspace";
        private static string _cassandraVersionText;
        private static Version _cassandraVersion;

        /// <summary>
        /// Gets the Cassandra version used for this test run
        /// </summary>
        public static Version CassandraVersion
        {
            get
            {
                LoadCassandraVersion();
                return _cassandraVersion;
            }
        }

        /// <summary>
        /// Gets the full Cassandra version used for this test run, in semver format: 2.2.0-rc1
        /// </summary>
        public static string CassandraVersionText
        {
            get
            {
                LoadCassandraVersion();
                return _cassandraVersionText;
            }
        }

        /// <summary>
        /// Gets the ip prefix for the Cassandra instances
        /// </summary>
        public static string IpPrefix
        {
            get { return ConfigurationManager.AppSettings["DefaultIpPrefix"] ?? "127.0.0."; }
        }

        /// <summary>
        /// Loads the cassandra version from environment variables and configuration
        /// </summary>
        /// <exception cref="ConfigurationErrorsException"></exception>
        private static void LoadCassandraVersion()
        {
            if (_cassandraVersionText != null)
            {
                return;
            }
            var versionText = Environment.GetEnvironmentVariable("CASSANDRA_VERSION");
            if (versionText == null)
            {
                versionText = ConfigurationManager.AppSettings["CassandraVersion"];
            }
            if (String.IsNullOrEmpty(versionText))
            {
                throw new ConfigurationErrorsException("You must specify the cassandra version either via CASSANDRA_VERSION environment variable or by 'CassandraVersion' app setting");
            }
            _cassandraVersionText = versionText;
            //in case there is a version label like rc1 / beta1
            versionText = versionText.Split('-')[0];
            _cassandraVersion = Version.Parse(versionText);
        }
        
        /// <summary>
        /// Creates a new test cluster
        /// </summary>
        public static ITestCluster CreateNew(int nodeLength = 1, TestClusterOptions options = null, bool startCluster = true)
        {
            TryRemove(true);
            options = options ?? new TestClusterOptions();
            var testCluster = new CcmCluster(CassandraVersionText, TestUtils.GetTestClusterNameBasedOnTime(), IpPrefix, DefaultKeyspaceName);
            testCluster.Create(nodeLength, options);
            if (startCluster)
            {
                testCluster.Start(options.JvmArgs);   
            }
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
        /// Removes the current ccm cluster
        /// </summary>
        public static void Remove()
        {
            CcmBridge.ExecuteCcm("remove");
        }

        /// <summary>
        /// Removes the current ccm cluster, without throwing exceptions if it fails
        /// </summary>
        public static void TryRemove(bool onlyRemoveTestCluster = false)
        {
	        bool skipRemove = false;
			if (onlyRemoveTestCluster)
			{
				try
				{
					var output = CcmBridge.ExecuteCcm("status").OutputText.ToString();
					var match = Regex.Match(output, "^Cluster: '(.*)'");
					string clusterName = match.Groups[1].Value;
					if (!clusterName.StartsWith(string.Format(TestUtils.TEST_CLUSTER_NAME_FORMAT, "")))
					{
						if (Diagnostics.CassandraTraceSwitch.TraceInfo)
						{
							Trace.TraceInformation("ccm current cluster does not appear to be a test cluster, skipping remove: {0}", clusterName);
						}
						skipRemove = true;
					}
				}
				catch (Exception ex)
				{
					if (Diagnostics.CassandraTraceSwitch.Level == TraceLevel.Verbose)
					{
						Trace.TraceError("ccm test cluster status could not be determined, skipping remove: {0}", ex);
					}
					skipRemove = true;
				}
			}
			if (skipRemove)
			{
				return;
			}
            try
            {
                CcmBridge.ExecuteCcm("remove");
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
