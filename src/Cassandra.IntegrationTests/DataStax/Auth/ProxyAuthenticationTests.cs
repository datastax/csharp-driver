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

using System.Linq;
using Cassandra.DataStax.Auth;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.DataStax.Auth
{
    [Category("short"), TestDseVersion(5, 1), Category("serverapi")]
    public class ProxyAuthenticationTests : TestGlobals
    {
        private ITestCluster _testCluster;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            _testCluster = TestClusterManager.CreateNew(1, new TestClusterOptions
            {
                CassandraYaml = new[]
                {
                    "authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator",
                    "authorizer:com.datastax.bdp.cassandra.auth.DseAuthorizer"
                },
                DseYaml = new []
                {
                    "authentication_options.default_scheme: internal",
                    "authorization_options.enabled:true",
                    "authentication_options.enabled:true",
                    "audit_logging_options.enabled:true"
                },
                JvmArgs = new[] { "-Dcassandra.superuser_setup_delay_ms=0" }
            });
            var authProvider = new DsePlainTextAuthProvider("cassandra", "cassandra");
            using (var cluster = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .WithAuthProvider(authProvider)
                .Build())
            {
                var session = cluster.Connect();
                var queries = new[]
                {
                    "CREATE ROLE IF NOT EXISTS alice WITH PASSWORD = 'alice' AND LOGIN = FALSE",
                    "CREATE ROLE IF NOT EXISTS ben WITH PASSWORD = 'ben' AND LOGIN = TRUE",
                    "CREATE ROLE IF NOT EXISTS 'bob@DATASTAX.COM' WITH LOGIN = TRUE",
                    "CREATE ROLE IF NOT EXISTS 'charlie@DATASTAX.COM' WITH PASSWORD = 'charlie' AND LOGIN = TRUE",
                    "CREATE ROLE IF NOT EXISTS steve WITH PASSWORD = 'steve' AND LOGIN = TRUE",
                    "CREATE ROLE IF NOT EXISTS paul WITH PASSWORD = 'paul' AND LOGIN = TRUE",
                    "CREATE ROLE IF NOT EXISTS aliceselect WITH PASSWORD = 'alice' AND LOGIN = FALSE",
                    "CREATE KEYSPACE IF NOT EXISTS aliceks " +
                    "WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}",
                    "CREATE TABLE IF NOT EXISTS aliceks.alicetable (key text PRIMARY KEY, value text)",
                    "INSERT INTO aliceks.alicetable (key, value) VALUES ('hello', 'world')",
                    "GRANT ALL ON KEYSPACE aliceks TO alice",
                    "GRANT SELECT ON KEYSPACE aliceks TO aliceselect",
                    "GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'ben'",
                    "GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'bob@DATASTAX.COM'",
                    "GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'steve'",
                    "GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'charlie@DATASTAX.COM'",
                    "GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'aliceselect'",
                    "GRANT PROXY.LOGIN ON ROLE 'alice' TO 'ben'",
                    "GRANT PROXY.LOGIN ON ROLE 'alice' TO 'bob@DATASTAX.COM'",
                    "GRANT PROXY.LOGIN ON ROLE 'alice' TO 'paul'",
                    "GRANT PROXY.EXECUTE ON ROLE 'alice' TO 'steve'",
                    "GRANT PROXY.EXECUTE ON ROLE 'alice' TO 'charlie@DATASTAX.COM'",
                    "GRANT PROXY.EXECUTE ON ROLE 'alice' TO 'paul'",
                    "GRANT PROXY.EXECUTE ON ROLE 'aliceselect' TO 'ben'",
                    "GRANT PROXY.EXECUTE ON ROLE 'aliceselect' TO 'steve'",
                };
                foreach (var q in queries)
                {
                    session.Execute(q);
                }
            }
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            _testCluster.Remove();
        }

        /// <summary>
        /// Test if user can connect impersonated with a proxy user and execute <see cref="IStatement"/>
        /// </summary>
        [Test]
        public void Should_Allow_Plain_Text_Authorized_User_To_Login_As()
        {
            ConnectAndQuery(new DsePlainTextAuthProvider("ben", "ben", "alice"));
            ConnectAndBatchQuery(new DsePlainTextAuthProvider("ben", "ben", "alice"));
        }

        /// <summary>
        /// Test if user can execute an <see cref="IStatement"/> (simple and batch) impersonated
        /// with a proxy user <see cref="IStatement.ExecutingAs"/>
        /// </summary>
        [Test]
        public void Should_Allow_Plain_Text_Authorized_User_To_Execute_As()
        {
            ConnectAndQuery(new DsePlainTextAuthProvider("steve", "steve"), "alice");
            ConnectAndBatchQuery(new DsePlainTextAuthProvider("steve", "steve"), "alice");
        }

        /// <summary>
        /// Test if <see cref="UnauthorizedException"/> is thrown if user without PROXY.EXECUTE 
        /// tries to impersonate a statement execution <see cref="IStatement.ExecutingAs"/>
        /// </summary>
        [Test]
        public void Should__Not_Allow_Plain_Text_Unauthorized_User_To_Execute_As()
        {
            Assert.Throws<UnauthorizedException>(
                () => ConnectAndQuery(new DsePlainTextAuthProvider("ben", "ben"), "alice"));
            Assert.Throws<UnauthorizedException>(
                () => ConnectAndBatchQuery(new DsePlainTextAuthProvider("ben", "ben"), "alice"));
        }

        /// <summary>
        /// Test if <see cref="UnauthorizedException"/> is thrown if user without 
        /// PROXY.LOGIN tries to login impersonated <see cref="DsePlainTextAuthProvider"/>
        /// </summary>
        [Test]
        public void Should__Not_Allow_Plain_Text_Unauthorized_User_To_Login_As()
        {
            var ex = Assert.Throws<NoHostAvailableException>(
                () => ConnectAndQuery(new DsePlainTextAuthProvider("steve", "steve", "alice")));
            Assert.AreEqual(1, ex.Errors.Count);
            Assert.IsInstanceOf<AuthenticationException>(ex.Errors.Values.First());


            var exBatch = Assert.Throws<NoHostAvailableException>(
                () => ConnectAndQuery(new DsePlainTextAuthProvider("steve", "steve", "alice"),
                "DELETE FROM aliceks.alicetable WHERE KEY = 'doesnotexist'"));
            Assert.AreEqual(1, exBatch.Errors.Count);
            Assert.IsInstanceOf<AuthenticationException>(exBatch.Errors.Values.First());
        }

        /// <summary>
        /// Very peculiar scenario which the user has GRANT.LOGIN and GRANT.EXECUTE of another role
        /// but do not have EXECUTE ON ALL AUTHENTICATION SCHEMES.
        /// </summary>
        [Test]
        public void Should__Not_Allow_Plain_Text_Unauthorized_User_To_LOGIN_As_Without_EXECUTE_ON_ROLE()
        {
            var ex = Assert.Throws<UnauthorizedException>(
                () => ConnectAndQuery(new DsePlainTextAuthProvider("paul", "paul")));

            var exBatch = Assert.Throws<UnauthorizedException>(
                () => ConnectAndQuery(new DsePlainTextAuthProvider("paul", "paul"),
                "DELETE FROM aliceks.alicetable WHERE KEY = 'doesnotexist'"));
        }

        /// <summary>
        /// Test if <see cref="UnauthorizedException"/> is thrown
        /// if user tries to impersonate a statement execution <see cref="IStatement.ExecutingAs"/>
        /// with a role not granted to insert and delete at keyspace
        /// </summary>
        [Test]
        public void Should__Not_Allow_Plain_Text_Proxy_Authorized_User_Without_Grants_To_Modify_Keyspace()
        {
            Assert.Throws<UnauthorizedException>(
                () => ConnectAndQuery(new DsePlainTextAuthProvider("ben", "ben"), "aliceselect",
                "INSERT INTO aliceks.alicetable (key, value) VALUES ('bye', 'world')"));
            Assert.Throws<UnauthorizedException>(
                () => ConnectAndBatchQuery(new DsePlainTextAuthProvider("ben", "ben"), "aliceselect"));

            Assert.Throws<UnauthorizedException>(
                () => ConnectAndQuery(new DsePlainTextAuthProvider("ben", "ben"), "aliceselect",
                "DELETE FROM aliceks.alicetable WHERE KEY = 'doesnotexist'"));
            Assert.Throws<UnauthorizedException>(
                () => ConnectAndBatchQuery(new DsePlainTextAuthProvider("ben", "ben"), "aliceselect"));
        }

        private void ConnectAndQuery(IAuthProvider authProvider, string executeAs = null, string query = "SELECT * FROM aliceks.alicetable")
        {
            using (var cluster = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .WithAuthProvider(authProvider)
                .Build())
            {
                var session = cluster.Connect();
                var statement = new SimpleStatement(query);
                if (executeAs != null)
                {
                    statement.ExecutingAs(executeAs);
                }
                var rs = session.Execute(statement);
                Assert.NotNull(rs.FirstOrDefault());
            }
        }

        private void ConnectAndBatchQuery(IAuthProvider authProvider, string executeAs = null)
        {
            using (var cluster = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .WithAuthProvider(authProvider)
                .Build())
            {
                var session = cluster.Connect();
                //Test batch statement proxy auth 
                var batchStatements = new BatchStatement();
                var statement = new SimpleStatement("DELETE FROM aliceks.alicetable WHERE KEY = 'doesnotexist'");
                if (executeAs != null)
                {
                    batchStatements.ExecutingAs(executeAs);
                }
                batchStatements.Add(statement);
                session.Execute(batchStatements);
            }
        }
    }
}
