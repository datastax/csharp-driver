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
using System.IO;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestClusterManagement;

namespace Cassandra.IntegrationTests
{
    public abstract class SharedCloudClusterTest : SharedClusterTest
    {
        private const int MaxRetries = 20;

        private readonly bool _sniCertValidation;
        private readonly bool _clientCert;
        protected override string[] SetupQueries => base.SetupQueries;
        
        protected new ICluster Cluster { get; set; }

        protected SharedCloudClusterTest(
            bool createSession = true, bool reuse = true, bool sniCertValidation = true, bool clientCert = true) :
            base(3, createSession, reuse)
        {
            _sniCertValidation = sniCertValidation;
            _clientCert = clientCert;
        }

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
        }

        public override void OneTimeTearDown()
        {
            base.OneTimeTearDown();
        }

        protected override void CreateCommonSession()
        {
            Exception last = null;
            for (var i = 0; i < SharedCloudClusterTest.MaxRetries; i++)
            {
                try
                {
                    Cluster = CreateCluster();
                    SetBaseSession(Cluster.Connect());
                    return;
                }
                catch (Exception ex) { last = ex; Task.Delay(1000).GetAwaiter().GetResult(); }
            }
            throw last;
        }

        protected ICluster CreateCluster(string creds = "creds-v1.zip", Action<Builder> act = null)
        {
            var builder = Cassandra.Cluster.Builder();
            act?.Invoke(builder);
            builder = builder
                .WithCloudSecureConnectionBundle(
                     Path.Combine(((CloudCluster) TestCluster).SniHomeDirectory, "certs", "bundles", creds))
                 .WithPoolingOptions(
                     new PoolingOptions().SetHeartBeatInterval(200))
                 .WithReconnectionPolicy(new ConstantReconnectionPolicy(100));
            var cluster = builder.Build();
            ClusterInstances.Add(cluster);

            return cluster;
        }
        
        protected Task<ISession> CreateSessionAsync(string creds = "creds-v1.zip", Action<Builder> act = null)
        {
            return CreateCluster(creds, act).ConnectAsync();
        }

        protected override ITestCluster CreateNew(int nodeLength, TestClusterOptions options, bool startCluster)
        {
            return TestCloudClusterManager.CreateNew(_sniCertValidation);
        }
        
        protected override bool IsSimilarCluster(ITestCluster reusableInstance, TestClusterOptions options, int nodeLength)
        {
            return reusableInstance is CloudCluster c && c.SniCertificateValidation == _sniCertValidation;
        }
    }
}
