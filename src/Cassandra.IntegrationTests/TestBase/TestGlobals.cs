//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Collections;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using Cassandra.IntegrationTests.TestClusterManagement;
using CommandLine;
using CommandLine.Text;
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
        // Make current default test cluster name unique
        public const string DefaultKeyspaceName = "test_cluster_keyspace";

        private static TestClusterManager _clusterManager;
        private static bool _clusterManagerIsInitializing;
        private static bool _clusterManagerIsInitalized;

        [Option('c', "cassandra-version",
            HelpText = "CCM Cassandra Version.", DefaultValue = "2.1.0" )]
        public string CassandraVersionStr { get; set; }

        public Version CassandraVersion
        {
            get
            {
                int mayor = 0, minor = 0, build = 0;
                if (this.CassandraVersionStr != null)
                {
                    var versionParts = this.CassandraVersionStr.Split('.');
                    if (versionParts.Length >= 2)
                    {
                        mayor = Convert.ToInt32(versionParts[0]);
                        minor = Convert.ToInt32(versionParts[1]);
                        if (versionParts.Length == 3)
                        {
                            int.TryParse(versionParts[2], out build);
                        }
                    }
                }
                return new Version(mayor, minor, build);
            }
        }

        [Option("use-ctool",
            HelpText = "Pass in 'true' for this value to use ctool instead of ccm (default)", DefaultValue = false, Required = true)]
        public bool UseCtool { get; set; }

        [Option('i', "ip-prefix",
            HelpText = "CCM Ip prefix", DefaultValue = DefaultLocalIpPrefix)]
        public string DefaultIpPrefix { get; set; }

        [Option("logger",
            HelpText = "Use Logger", DefaultValue = false)]
        public bool UseLogger { get; set; }

        [Option("log-level",
            HelpText = "Log Level", DefaultValue = "Trace")]
        public string LogLevel { get; set; }

        [Option('h', "ssh-host",
            HelpText = "CCM SSH host", DefaultValue = DefaultInitialContactPoint)]
        public string SSHHost { get; set; }

        [Option('t', "ssh-port",
            HelpText = "CCM SSH port", DefaultValue = 22)]
        public int SSHPort { get; set; }

        [Option('u', "ssh-user", Required = true,
            HelpText = "CCM SSH user")]
        public string SSHUser { get; set; }

        [Option('p', "ssh-password", Required = true,
            HelpText = "CCM SSH password")]
        public string SSHPassword { get; set; }

        //test configuration
        [Option("compression",
            HelpText = "Use Compression", DefaultValue = false)]
        public bool UseCompression { get; set; }

        [Option("nobuffering",
            HelpText = "No Buffering", DefaultValue = false)]
        public bool NoUseBuffering { get; set; }

        public TestGlobals()
        {
            if (ConfigurationManager.AppSettings.Count > 0)
            {
                //Load the values from configuration
                CassandraVersionStr = ConfigurationManager.AppSettings["CassandraVersion"] ?? CassandraVersionStr;
                DefaultIpPrefix = ConfigurationManager.AppSettings["DefaultIpPrefix"] ?? this.DefaultIpPrefix;
                LogLevel = ConfigurationManager.AppSettings["LogLevel"] ?? this.LogLevel;

                if (ConfigurationManager.AppSettings["NoUseBuffering"] != null)
                    this.NoUseBuffering = Convert.ToBoolean(ConfigurationManager.AppSettings["NoUseBuffering"]);

                SSHHost = ConfigurationManager.AppSettings["SSHHost"] ?? this.SSHHost;
                SSHPassword = ConfigurationManager.AppSettings["SSHPassword"] ?? this.SSHPassword;

                if (ConfigurationManager.AppSettings["SSHPort"] != null)
                    SSHPort = Convert.ToInt32(ConfigurationManager.AppSettings["SSHPort"]);

                SSHUser = ConfigurationManager.AppSettings["SSHUser"] ?? this.SSHUser;

                bool useCtoolParsed;
                Boolean.TryParse(ConfigurationManager.AppSettings["UseCtool"], out useCtoolParsed);
                UseCtool = useCtoolParsed;
            }

        }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this,
                                      (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }

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
                        Trace.TraceInformation("Shared " + _clusterManagerIsInitializing.GetType().Name + " object is initializing. Sleeping " + SleepMs + " MS ... ");
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
            VerifyLocalCcmOnly();
        }

        // If any test is designed for another test group, mark as ignored
        private void VerifyLocalCcmOnly()
        {
            if (((ArrayList) TestContext.CurrentContext.Test.Properties["_CATEGORIES"]).Contains(TestCategories.CcmOnly) && UseCtool)
            {
                Assert.Ignore("Test Ignored: Requires CCM and tests are currently running using CTool");
            }

        }

        // If any test is designed for another C* version, mark it as ignored
        private void VerifyAppropriateCassVersion()
        {
            var test = TestContext.CurrentContext.Test;
            var methodFullName = TestContext.CurrentContext.Test.FullName;
            var typeName = methodFullName.Substring(0, methodFullName.Length - test.Name.Length - 1);
            var type = Assembly.GetExecutingAssembly().GetType(typeName);
            if (type == null)
            {
                return;
            }
            var methodAttr = type.GetMethod(test.Name)
                .GetCustomAttributes(true)
                .Select(a => (Attribute)a)
                .FirstOrDefault((a) => a is TestCassandraVersion);
            var attr = Attribute.GetCustomAttributes(type).FirstOrDefault((a) => a is TestCassandraVersion);
            if (attr == null && methodAttr == null)
            {
                //It does not contain the attribute, move on.
                return;
            }
            if (methodAttr != null)
            {
                attr = methodAttr;
            }
            var versionAttr = (TestCassandraVersion)attr;
            var executingVersion = CassandraVersion;
            if (!VersionMatch(versionAttr, executingVersion))
                Assert.Ignore(String.Format("Test Ignored: Test suitable to be run against Cassandra {0}.{1}.{2} {3}", versionAttr.Major, versionAttr.Minor, versionAttr.Build, versionAttr.Comparison >= 0 ? "or above" : "or below"));
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


    }
}
