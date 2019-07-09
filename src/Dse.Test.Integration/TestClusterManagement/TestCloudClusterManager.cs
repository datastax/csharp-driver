// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System;
using System.Diagnostics;

namespace Dse.Test.Integration.TestClusterManagement
{
    public class TestCloudClusterManager
    {
        public static string SniProxyEndPoint => Environment.GetEnvironmentVariable("SNI_PROXY_ENDPOINT") ?? "127.0.0.1:30002";

        public static string SniMetadataEndPoint => Environment.GetEnvironmentVariable("SNI_METADATA_ENDPOINT") ?? "127.0.0.1:30443";

        public static string VersionString => Environment.GetEnvironmentVariable("CLOUD_VERSION") ?? "3.11.2";

        public static Version Version => new Version(VersionString);

        public static string CertFile => Environment.GetEnvironmentVariable("SNI_CERTIFICATE_PATH");

        public static ITestCluster CreateNew(bool enableCert)
        {
            TryRemove();
            var testCluster = new CloudCluster(
                TestUtils.GetTestClusterNameBasedOnTime(), 
                VersionString,
                enableCert);
            testCluster.Create(3, null);
            return testCluster;
        }

        public static void TryRemove()
        {
            try
            {
                CloudCluster.DockerKill();
            }
            catch (Exception ex)
            {
                if (Diagnostics.CassandraTraceSwitch.Level == TraceLevel.Verbose)
                {
                    Trace.TraceError("cloud test cluster could not be removed: {0}", ex);   
                }
            }
        }
    }
}