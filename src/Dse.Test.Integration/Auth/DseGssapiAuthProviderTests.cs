//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

#if !NETCORE
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Dse;
using Dse.Auth;
using Dse.Test.Integration.TestClusterManagement;
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
                                        .AddContactPoint(TestClusterManager.InitialContactPoint)
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
#endif