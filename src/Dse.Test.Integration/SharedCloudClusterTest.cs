//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Threading.Tasks;
using Dse.Test.Integration.TestClusterManagement;

namespace Dse.Test.Integration
{
    public abstract class SharedCloudClusterTest : SharedDseClusterTest
    {
        private readonly bool _sniCertValidation;
        private readonly bool _clientCert;
        protected override string[] SetupQueries => base.SetupQueries;

        protected SharedCloudClusterTest(bool createSession = true, bool reuse = true, bool sniCertValidation = true, bool clientCert = true) :
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
            for (var i = 0; i < 100; i++) {
            try {
            Cluster = DseCluster
                          .ForClusterConfigAsync($"https://{TestCloudClusterManager.SniMetadataEndPoint}/metadata", _clientCert ? TestCloudClusterManager.CertFile : null)
                          .GetAwaiter().GetResult();   
            SetBaseSession(Cluster.Connect());  
            return;
            } catch (Exception ex) { last = ex; Task.Delay(1000).GetAwaiter().GetResult(); }
            }
            throw last;
        }

        protected async Task<IDseSession> CreateSessionAsync(bool? enableClientCert = null)
        {
            return await (await DseCluster
                             .ForClusterConfigAsync(
                                 $"https://{TestCloudClusterManager.SniMetadataEndPoint}/metadata",
                                 (enableClientCert ?? _clientCert) ? TestCloudClusterManager.CertFile : null))
                         .ConnectAsync().ConfigureAwait(false);
        }

        protected override ITestCluster CreateNew(int nodeLength, TestClusterOptions options, bool startCluster)
        {
            return TestCloudClusterManager.CreateNew(_sniCertValidation);
        }

        protected override void ExecuteSetupQueries()
        {
            base.ExecuteSetupQueries();
        }

        protected override bool IsSimilarCluster(ITestCluster reusableInstance, TestClusterOptions options, int nodeLength)
        {
            return reusableInstance is CloudCluster c && c.SniCertificateValidation == _sniCertValidation;
        }
    }
}
