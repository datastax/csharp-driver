//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Dse.Test.Integration.TestClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration.TestClusterManagement
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

        public Version CassandraVersion => TestClusterManager.CassandraVersion;

        public Version DseVersion => TestClusterManager.DseVersion;

        /// <summary>
        /// Determines if we are running on AppVeyor.
        /// </summary>
        protected static bool IsAppVeyor => !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("APPVEYOR"));

        /// <summary>
        /// Gets the latest protocol version depending on the Cassandra Version running the tests
        /// </summary>
        public ProtocolVersion GetProtocolVersion()
        {
            if (DseVersion >= Version.Parse("6.0"))
            {
                return ProtocolVersion.DseV2;
            }

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

        public static async Task Connect(Cluster cluster, bool asyncConnect, Action<ISession> action)
        {
            if (asyncConnect)
            {
                try
                {
                    var session = await cluster.ConnectAsync();
                    action(session);
                }
                finally
                {
                    var shutdownAsync = cluster?.ShutdownAsync();
                    if (shutdownAsync != null) await shutdownAsync;
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
