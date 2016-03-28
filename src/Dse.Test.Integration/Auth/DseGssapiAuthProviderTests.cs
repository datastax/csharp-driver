using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Cassandra;
using Dse.Auth;
using Dse.Test.Integration.ClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration.Auth
{
    [Explicit(
        "It can only run when there is a Kerberos-enabled DSE cluster and the host running the test is authenticated" +
        "against the KDC")]
    public class DseGssapiAuthProviderTests : BaseIntegrationTest
    {
        [Test]
        [Ignore("Kerberos auth not available")]
        public void Gssapi_Auth_Test()
        {
            var provider = new DseGssapiAuthProvider();
            using (var cluster = Cluster.Builder()
                                        .WithAuthProvider(provider)
                                        .WithQueryTimeout(Timeout.Infinite)
                                        .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(3600000))
                                        .AddContactPoint(CcmHelper.InitialContactPoint)
                                        .Build())
            {
                try
                {
                    cluster.Connect();
                }
                catch (NoHostAvailableException ex)
                {
                    throw ex.Errors.Values.First();
                }
            }
        }
    }
}
