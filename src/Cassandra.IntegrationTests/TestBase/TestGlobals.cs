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
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.TestBase
{
    public class TestGlobals
    {
        public const int DefaultCassandraPort = 9042;
        public const int DefaultMaxClusterCreateRetries = 2;
        public const string DefaultLocalIpPrefix = "127.0.0.";
        public const string DefaultInitialContactPoint = DefaultLocalIpPrefix + "1";
        public const int ClusterInitSleepMsPerIteration = 500;
        public const int ClusterInitSleepMsMax = 60 * 1000;

        private static TestClusterManager _clusterManager;
        private static bool _clusterManagerIsInitializing;
        private static bool _clusterManagerIsInitalized;

        public string CassandraVersionStr => TestClusterManager.CassandraVersionText;

        public Version CassandraVersion => TestClusterManager.CassandraVersion;

        /// <summary>
        /// Determines if we are running on AppVeyor.
        /// </summary>
        protected static bool IsAppVeyor => !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("APPVEYOR"));

        /// <summary>
        /// Gets the latest protocol version depending on the Cassandra Version running the tests
        /// </summary>
        public ProtocolVersion GetProtocolVersion()
        {
            var cassandraVersion = CassandraVersion;
            var protocolVersion = ProtocolVersion.V1;
            if (cassandraVersion >= Version.Parse("2.2"))
            {
                protocolVersion = ProtocolVersion.V4;
            }
            else if (cassandraVersion >= Version.Parse("2.1"))
            {
                protocolVersion = ProtocolVersion.V3;
            }
            else if (cassandraVersion > Version.Parse("2.0"))
            {
                protocolVersion = ProtocolVersion.V2;
            }
            return protocolVersion;
        }
        
        public bool UseCtool { get; set; }
        
        public string DefaultIpPrefix { get; set; }
        
        public bool UseLogger { get; set; }
        
        public string LogLevel { get; set; }
        
        public string SSHHost { get; set; }
        
        public int SSHPort { get; set; }
        
        public string SSHUser { get; set; }
        
        public string SSHPassword { get; set; }
        
        public bool UseCompression { get; set; }
        
        public bool NoUseBuffering { get; set; }

        public TestClusterManager TestClusterManager
        {
            get
            {
                if (_clusterManagerIsInitalized)
                    return _clusterManager;
                else if (_clusterManagerIsInitializing)
                {
                    while (_clusterManagerIsInitializing)
                    {
                        int SleepMs = 1000;
                        Trace.TraceInformation(
                            $"Shared {_clusterManagerIsInitializing.GetType().Name} object is initializing. Sleeping {SleepMs} MS ... ");
                        Thread.Sleep(SleepMs);
                    }
                }
                else
                {
                    _clusterManagerIsInitializing = true;
                    _clusterManager = new TestClusterManager();
                    _clusterManagerIsInitializing = false;
                    _clusterManagerIsInitalized = true;
                }
                return _clusterManager;
            }
        }

        [SetUp]
        public void IndividualTestSetup()
        {
            VerifyAppropriateCassVersion();
        }

        // If any test is designed for another C* version, mark it as ignored
        private void VerifyAppropriateCassVersion()
        {
            var test = TestContext.CurrentContext.Test;
            var typeName = TestContext.CurrentContext.Test.ClassName;
            var type = Type.GetType(typeName);
            if (type == null)
            {
                return;
            }
            var testName = test.Name;
            if (testName.IndexOf('(') > 0)
            {
                //The test name could be a TestCase: NameOfTheTest(ParameterValue);
                //Remove the parenthesis
                testName = testName.Substring(0, testName.IndexOf('('));
            }
            var methodAttr = type.GetMethod(testName)
                .GetCustomAttribute<TestCassandraVersion>(true);
            var attr = type.GetTypeInfo().GetCustomAttribute<TestCassandraVersion>();
            if (attr == null && methodAttr == null)
            {
                //It does not contain the attribute, move on.
                return;
            }
            if (methodAttr != null)
            {
                attr = methodAttr;
            }
            var versionAttr = attr;
            var executingVersion = CassandraVersion;
            if (!VersionMatch(versionAttr, executingVersion))
                Assert.Ignore(
                    $"Test Ignored: Test suitable to be run against Cassandra {versionAttr.Major}.{versionAttr.Minor}.{versionAttr.Build} {(versionAttr.Comparison >= 0 ? "or above" : "or below")}");
        }

        public static bool VersionMatch(TestCassandraVersion versionAttr, Version executingVersion)
        {
            //Compare them as integers
            var expectedVersion = versionAttr.Major * 100000000 + versionAttr.Minor * 10000 + versionAttr.Build;
            var actualVersion = executingVersion.Major * 100000000 + executingVersion.Minor * 10000 + executingVersion.Build;
            var comparison = (Comparison)actualVersion.CompareTo(expectedVersion);

            if (comparison >= Comparison.Equal && versionAttr.Comparison == Comparison.GreaterThanOrEqualsTo)
            {
                return true;
            }
            return comparison == versionAttr.Comparison;
        }

        public static async Task Connect(Cluster cluster, bool asyncConnect, Action<ISession> action)
        {
            if (asyncConnect)
            {
                try
                {
                    var session = await cluster.ConnectAsync().ConfigureAwait(false);
                    action(session);
                }
                finally
                {
                    var shutdownAsync = cluster?.ShutdownAsync();
                    if (shutdownAsync != null) await shutdownAsync.ConfigureAwait(false);
                }
            }
            else
            {
                using (cluster)
                {
                    var session = cluster.Connect();
                    action(session);
                }
            }
        }

    }
}
