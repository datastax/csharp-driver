using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;
using NUnit.Framework;
using Dse.Auth;
using Dse.Test.Integration.ClusterManagement;

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

        [Test]
        public void Should_Authenticate_Against_Dse_5_DseAuthenticator()
        {
            CcmHelper.Start(
                1,
                new[] { "authentication_options.default_scheme: internal" },
                new[] { "authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator" },
                new[] { "-Dcassandra.superuser_setup_delay_ms=0" });
            var authProvider = new DsePlainTextAuthProvider("cassandra", "cassandra");
            using (var cluster = Cluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
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
            CcmHelper.Start(
                1,
                cassYamlOptions: new[] { "authenticator: PasswordAuthenticator" },
                jvmArgs: new[] { "-Dcassandra.superuser_setup_delay_ms=0" });
            var authProvider = new DsePlainTextAuthProvider("cassandra", "cassandra");
            using (var cluster = Cluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithAuthProvider(authProvider)
                .Build())
            {
                var session = cluster.Connect();
                AssertCanQuery(session);
            }
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            try
            {
                CcmHelper.Remove();
            }
            catch
            {
                //Tried to remove, never mind if it fails.
            }
        }
    }
}
