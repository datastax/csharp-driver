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

#if !NETCORE
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Cassandra;
using Cassandra.Auth;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Auth
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