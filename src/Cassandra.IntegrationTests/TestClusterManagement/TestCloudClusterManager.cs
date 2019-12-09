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
using Cassandra.IntegrationTests.TestBase;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class TestCloudClusterManager
    {
        public static bool Created = false;

        public static string SniProxyEndPoint => Environment.GetEnvironmentVariable("SNI_PROXY_ENDPOINT") ?? "127.0.0.1:30002";

        public static string SniMetadataEndPoint => Environment.GetEnvironmentVariable("SNI_METADATA_ENDPOINT") ?? "127.0.0.1:30443";

        public static string VersionString => Environment.GetEnvironmentVariable("CLOUD_VERSION") ?? "3.11.2";

        public static Version Version => new Version(VersionString);

        public static string CertFile => Environment.GetEnvironmentVariable("SNI_CERTIFICATE_PATH");

        public static string CaFile => Environment.GetEnvironmentVariable("SNI_CA_PATH");

        public static ITestCluster CreateNew(bool enableCert)
        {
            TryRemove();
            var testCluster = new CloudCluster(
                TestUtils.GetTestClusterNameBasedOnTime(), 
                VersionString,
                enableCert);
            testCluster.Create(3, null);
            TestCloudClusterManager.Created = true;
            return testCluster;
        }

        public static void TryRemove()
        {
            if (!TestCloudClusterManager.Created)
            {
                return;
            }

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