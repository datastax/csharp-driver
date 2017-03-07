//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Dse;
using Dse.Test.Integration.TestClusterManagement;
using NUnit.Framework;
using Dse.Auth;

namespace Dse.Test.Integration.Auth
{
    public class DsePlainTextAuthProviderTests : BaseIntegrationTest
    {
        private static void AssertCanQuery(ISession session)
        {
            for (var i = 0; i < 10; i++)
            {
                Assert.DoesNotThrow(() => session.Execute("SELECT key FROM system.local"));
            }
        }

        [Test, TestDseVersion(5,0)]
        public void Should_Authenticate_Against_Dse_5_DseAuthenticator()
        {
            TestClusterManager.CreateNew(1, new TestClusterOptions
            {
                DseYaml = new[] { "authentication_options.default_scheme: internal" },
                CassandraYaml = new[] { "authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator" },
                JvmArgs = new[] { "-Dcassandra.superuser_setup_delay_ms=0" }
            });
            Trace.TraceInformation("Waiting additional time for test Cluster to be ready");
            Thread.Sleep(15000);
            var authProvider = new DsePlainTextAuthProvider("cassandra", "cassandra");
            using (var cluster = Cluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithAuthProvider(authProvider)
                .Build())
            {
                var session = cluster.Connect();
                AssertCanQuery(session);
            }
        }

        [Test]
        public void Should_Authenticate_Against_Dse_Daemon_With_PasswordAuthenticator()
        {
            TestClusterManager.CreateNew(1, new TestClusterOptions
            {
                CassandraYaml = new[] { "authenticator: PasswordAuthenticator" },
                JvmArgs = new[] { "-Dcassandra.superuser_setup_delay_ms=0" }
            });
            Trace.TraceInformation("Waiting additional time for test Cluster to be ready");
            Thread.Sleep(15000);
            var authProvider = new DsePlainTextAuthProvider("cassandra", "cassandra");
            using (var cluster = Cluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithAuthProvider(authProvider)
                .Build())
            {
                var session = cluster.Connect();
                AssertCanQuery(session);
            }
        }

        [TearDown]
        public void TearDown()
        {
            TestClusterManager.TryRemove();
        }
    }
}
