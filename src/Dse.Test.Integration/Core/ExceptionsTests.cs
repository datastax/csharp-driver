//
//      Copyright (C) 2012-2014 DataStax Inc.
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

using System.Diagnostics;
using System.Security;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
#if !NETCORE
using System.Security.Permissions;
#endif
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class ExceptionsTests : TestGlobals
    {
        /// <summary>
        ///  Tests the AlreadyExistsException. Create a keyspace twice and a table twice.
        ///  Catch and test all the exception methods.
        /// </summary>
        [Test]
        public void AlreadyExistsException()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1);
            ISession session = testCluster.Session;
            
            string keyspace = TestUtils.GetUniqueKeyspaceName();
            string table = TestUtils.GetUniqueTableName();

            String[] cqlCommands =
            {
                String.Format(TestUtils.CreateKeyspaceSimpleFormat, keyspace, 1),
                String.Format("USE \"{0}\"", keyspace),
                String.Format(TestUtils.CreateTableSimpleFormat, table)
            };

            // Create the schema once
            session.Execute(cqlCommands[0]);
            session.Execute(cqlCommands[1]);
            session.Execute(cqlCommands[2]);

            // Try creating the keyspace again
            var ex = Assert.Throws<AlreadyExistsException>(() => session.Execute(cqlCommands[0]));
            Assert.AreEqual(ex.Keyspace, keyspace);
            Assert.AreEqual(ex.Table, null);
            Assert.AreEqual(ex.WasTableCreation, false);

            session.Execute(cqlCommands[1]);

            // Try creating the table again
            try
            {
                session.Execute(cqlCommands[2]);
            }
            catch (AlreadyExistsException e)
            {
                Assert.AreEqual(e.Keyspace, keyspace);
                Assert.AreEqual(e.Table, table.ToLower());
                Assert.AreEqual(e.WasTableCreation, true);
            }
        }

        /// <summary>
        ///  Tests the NoHostAvailableException. by attempting to build a cluster using
        ///  the IP address "255.255.255.255" and test all available exception methods.
        /// </summary>
        [Test]
        public void NoHostAvailableException()
        {
            var ipAddress = "255.255.255.255";
            var errorsHashMap = new Dictionary<IPAddress, Exception>();
            errorsHashMap.Add(IPAddress.Parse(ipAddress), null);

            try
            {
                Cluster.Builder().AddContactPoint(ipAddress).Build();
            }
            catch (NoHostAvailableException e)
            {
                Assert.AreEqual(e.Message, String.Format("All host tried for query are in error (tried: {0})", ipAddress));
                Assert.AreEqual(e.Errors.Keys.ToArray(), errorsHashMap.Keys.ToArray());
            }
        }

        /// <summary>
        ///  Tests the ReadTimeoutException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then forcibly kill single node and attempt a read of
        ///  the key at CL.ALL. Catch and test all available exception methods.
        /// </summary>
        [Test]
        public void ReadTimeoutException()
        {
            ITestCluster nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(2);
            ISession session = nonShareableTestCluster.Session;

            string keyspace = "TestKeyspace_" + Randomm.RandomAlphaNum(10);
            string table = "TestTable_" + Randomm.RandomAlphaNum(10);
            int replicationFactor = 2;
            string key = "1";

            session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, keyspace, replicationFactor));
            Thread.Sleep(5000);
            session.ChangeKeyspace(keyspace);
            session.Execute(String.Format(TestUtils.CreateTableSimpleFormat, table));
            Thread.Sleep(3000);

            session.Execute(
                new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(
                    ConsistencyLevel.All));
            session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));

            nonShareableTestCluster.StopForce(2);
            var ex = Assert.Throws<ReadTimeoutException>(() => 
                session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All)));
            Assert.AreEqual(ex.ConsistencyLevel, ConsistencyLevel.All);
            Assert.AreEqual(ex.ReceivedAcknowledgements, 1);
            Assert.AreEqual(ex.RequiredAcknowledgements, 2);
            Assert.AreEqual(ex.WasDataRetrieved, true);
        }

        /// <summary>
        ///  Tests SyntaxError. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void SyntaxError()
        {
            var errorMessage = "Test Message";

            try
            {
                throw new SyntaxError(errorMessage);
            }
            catch (SyntaxError e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests TraceRetrievalException. Tests basic message.
        /// </summary>
        [Test]
        public void TraceRetrievalException()
        {
            var errorMessage = "Test Message";

            try
            {
                throw new TraceRetrievalException(errorMessage);
            }
            catch (TraceRetrievalException e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests TruncateException. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void TruncateException()
        {
            var errorMessage = "Test Message";

            try
            {
                throw new TruncateException(errorMessage);
            }
            catch (TruncateException e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests UnauthorizedException. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void UnauthorizedException()
        {
            var errorMessage = "Test Message";

            try
            {
                throw new UnauthorizedException(errorMessage);
            }
            catch (UnauthorizedException e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests the UnavailableException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then kill single node, wait for gossip to propogate the
        ///  new state, and attempt to read and write the same key at CL.ALL. Catch and
        ///  test all available exception methods.
        /// </summary>
        [Test]
        public void UnavailableException()
        {
            ITestCluster nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(2);
            ISession session = nonShareableTestCluster.Session;

            string keyspaceName = "TestKeyspace_" + Randomm.RandomAlphaNum(10);
            string tableName = "TestTable_" + Randomm.RandomAlphaNum(10);
            int replicationFactor = 2;
            string key = "1";
            session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, keyspaceName, replicationFactor));
            session.ChangeKeyspace(keyspaceName);
            session.Execute(String.Format(TestUtils.CreateTableSimpleFormat, tableName));

            session.Execute(
                new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, tableName, key, "foo", 42, 24.03f)).SetConsistencyLevel(ConsistencyLevel.All));
            session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, tableName)).SetConsistencyLevel(ConsistencyLevel.All));

            nonShareableTestCluster.StopForce(2);
            // Ensure that gossip has reported the node as down.

            bool expectedExceptionWasCaught = false;
            int readTimeoutWasCaught = 0;
            int maxReadTimeouts = 6; // as long as we're getting Read Timeouts, then we're on the right track

            while (!expectedExceptionWasCaught && readTimeoutWasCaught < maxReadTimeouts)
            {
                try
                {
                    session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, tableName)).SetConsistencyLevel(ConsistencyLevel.All));
                }
                catch (UnavailableException e)
                {
                    Assert.AreEqual(e.Consistency, ConsistencyLevel.All);
                    Assert.AreEqual(e.RequiredReplicas, replicationFactor);
                    Assert.AreEqual(e.AliveReplicas, replicationFactor - 1);
                    expectedExceptionWasCaught = true;
                }
                catch (ReadTimeoutException e)
                {
                    Assert.AreEqual(e.ConsistencyLevel, ConsistencyLevel.All);
                    Trace.TraceInformation("We caught a ReadTimeoutException as expected, extending the total time to wait ... ");
                    readTimeoutWasCaught++;
                }
                Trace.TraceInformation("Expected exception was not thrown, trying again ... ");
            }

            Assert.True(expectedExceptionWasCaught,
                string.Format("Expected exception {0} was not caught after {1} read timeouts were caught!", "UnavailableException", maxReadTimeouts));
        }

        /// <summary>
        ///  Tests the WriteTimeoutException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then forcibly kill single node and attempt to write the
        ///  same key at CL.ALL. Catch and test all available exception methods.
        /// </summary>
        [Test]
        public void WriteTimeoutException()
        {
            ITestCluster nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(2);
            ISession session = nonShareableTestCluster.Session;

            string keyspace = "TestKeyspace_" + Randomm.RandomAlphaNum(10);
            string table = "TestTable_" + Randomm.RandomAlphaNum(10);
            int replicationFactor = 2;
            string key = "1";

            session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, keyspace, replicationFactor));
            session.ChangeKeyspace(keyspace);
                session.Execute(String.Format(TestUtils.CreateTableSimpleFormat, table));

            session.Execute(
                new SimpleStatement(
                    String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(ConsistencyLevel.All));
            session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));

            nonShareableTestCluster.StopForce(2);
            try
            {
                session.Execute(
                    new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(
                        ConsistencyLevel.All));
            }
            catch (WriteTimeoutException e)
            {
                Assert.AreEqual(e.ConsistencyLevel, ConsistencyLevel.All);
                Assert.AreEqual(1, e.ReceivedAcknowledgements);
                Assert.AreEqual(2, e.RequiredAcknowledgements);
                Assert.AreEqual(e.WriteType, "SIMPLE");
            }
        }

        [Test]
        public void PreserveStackTraceTest()
        {
            // we need to make sure at least a single node cluster is available, running locally
            var session = TestClusterManager.GetNonShareableTestCluster(1).Session;
            try // writing without delegate assertion since that is undependable for this use case
            {
                session.Execute("SELECT WILL FAIL");
                Assert.Fail("Expected Exception was not thrown!");
            }
            catch (SyntaxError ex) 
            {
                Assert.True(ex.StackTrace.Contains("PreserveStackTraceTest"));
                Assert.True(ex.StackTrace.Contains("ExceptionsTests"));
            }
        }

#if !NETCORE
        public static AppDomain CreatePartialTrustDomain()
        {
            AppDomainSetup setup = new AppDomainSetup() { ApplicationBase = AppDomain.CurrentDomain.BaseDirectory };
            PermissionSet permissions = new PermissionSet(null);
            permissions.AddPermission(new SecurityPermission(SecurityPermissionFlag.Execution));
            permissions.AddPermission(new ReflectionPermission(ReflectionPermissionFlag.RestrictedMemberAccess));
            permissions.AddPermission(new SocketPermission(PermissionState.Unrestricted));
            return AppDomain.CreateDomain("Partial Trust AppDomain", null, setup, permissions);
        }
        
        [Test]
        public void ExceptionsOnPartialTrust()
        {
            var testCluster = TestClusterManager.CreateNew();
            var appDomain = CreatePartialTrustDomain();
            appDomain.DoCallBack(() => PreserveStackTraceOnConnectAndAssert(testCluster.InitialContactPoint));
        }
        
        public static void PreserveStackTraceOnConnectAndAssert(string contactPoint)
        {
            var ex = Assert.Throws<SecurityException>(() => Cluster.Builder().AddContactPoint(contactPoint).Build());
            string stackTrace = ex.StackTrace;

            //Must maintain the original call stack trace
            StringAssert.Contains("PreserveStackTraceOnConnectAndAssert", stackTrace);
            StringAssert.Contains("ExceptionsTests", stackTrace);
            StringAssert.Contains("Cassandra.Utils.ResolveHostByName", stackTrace); // something actually from the Cassandra library
        }
#endif

        [Test]
        public void RowSetIteratedTwice()
        {
            ISession session = TestClusterManager.GetNonShareableTestCluster(1).Session;
            string keyspace = "TestKeyspace_" + Randomm.RandomAlphaNum(10);
            string table = "TestTable_" + Randomm.RandomAlphaNum(10);
            string key = "1";

            session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, keyspace, 1));
            session.ChangeKeyspace(keyspace);
            session.Execute(String.Format(TestUtils.CreateTableSimpleFormat, table));

            session.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)));
            var rowset = session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table))).GetRows();
            //Linq Count iterates
            Assert.AreEqual(1, rowset.Count());
            Assert.AreEqual(0, rowset.Count());
        }

        [Test]
        public void RowSetPagingAfterSessionDispose()
        {
            ISession session = TestClusterManager.GetNonShareableTestCluster(1).Session;
            string keyspace = "TestKeyspace_" + Randomm.RandomAlphaNum(10);
            string table = "TestTable_" + Randomm.RandomAlphaNum(10);

            session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, keyspace, 1));
            session.ChangeKeyspace(keyspace);
            session.Execute(String.Format(TestUtils.CreateTableSimpleFormat, table));

            session.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, "1", "foo", 42, 24.03f)));
            session.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, "2", "foo", 42, 24.03f)));
            var rs = session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetPageSize(1));
            if (CassandraVersion.Major < 2)
            {
                //Paging should be ignored in 1.x
                //But setting the page size should work
                Assert.AreEqual(2, rs.InnerQueueCount);
                return;
            }
            Assert.AreEqual(1, rs.InnerQueueCount);

            session.Dispose();
            //It should not fail, just do nothing
            rs.FetchMoreResults();
            Assert.AreEqual(1, rs.InnerQueueCount);
        }

        [Test]
        public void WriteFailureExceptionTest()
        {
            if (TestClusterManager.CassandraVersion < Version.Parse("2.2"))
            {
                Assert.Ignore("Write failure error were introduced in Cassandra 2.2");
            }
            const string keyspace = "ks_wfail";
            const string table = keyspace + ".tbl1";
            var testCluster = TestClusterManager.GetTestCluster(2, 0, false, DefaultMaxClusterCreateRetries, false, false);
            testCluster.Start(1, "--jvm_arg=-Dcassandra.test.fail_writes_ks=" + keyspace);
            testCluster.Start(2);
            using (var cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, keyspace, 2));
                session.Execute(String.Format(TestUtils.CreateTableSimpleFormat, table));
                var query = String.Format("INSERT INTO {0} (k, t) VALUES ('ONE', 'ONE VALUES')", table);
                Assert.Throws<WriteFailureException>(() => 
                    session.Execute(new SimpleStatement(query).SetConsistencyLevel(ConsistencyLevel.All)));
            }
        }

        [Test]
        public void ReadFailureExceptionTest()
        {
            if (TestClusterManager.CassandraVersion < Version.Parse("2.2"))
            {
                Assert.Ignore("Read failure error were introduced in Cassandra 2.2");
            }
            const string keyspace = "ks_rfail";
            const string table = keyspace + ".tbl1";
            var testCluster = TestClusterManager.GetTestCluster(2, 0, false, DefaultMaxClusterCreateRetries, false, false);
            testCluster.UpdateConfig("tombstone_failure_threshold: 1000");
            TestHelper.ParallelInvoke(new Action[]
            {
                () => testCluster.Start(1),
                () => testCluster.Start(2)
            });
            var builder = Cluster
                .Builder()
                .WithLoadBalancingPolicy(new WhiteListLoadBalancingPolicy(2))
                .AddContactPoint(testCluster.ClusterIpPrefix + "2");
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, keyspace, 1));
                session.Execute(String.Format("CREATE TABLE {0} (pk int, cc int, v int, primary key (pk, cc))", table));
                // The rest of the test relies on the fact that the PK '1' will be placed on node1 with MurmurPartitioner
                var ps = session.Prepare(String.Format("INSERT INTO {0} (pk, cc, v) VALUES (1, ?, null)", table));
                var counter = 0;
                TestHelper.ParallelInvoke(() =>
                {
                    var rs = session.Execute(ps.Bind(Interlocked.Increment(ref counter)));
                    Assert.AreEqual(2, TestHelper.GetLastAddressByte(rs.Info.QueriedHost));
                }, 1100);
                Assert.Throws<ReadFailureException>(() => 
                    session.Execute(String.Format("SELECT * FROM {0} WHERE pk = 1", table)));
            }
        }

        [Test]
        public void FunctionFailureExceptionTest()
        {
            if (TestClusterManager.CassandraVersion < Version.Parse("2.2"))
            {
                Assert.Ignore("Function failure error were introduced in Cassandra 2.2");
            }
            var testCluster = TestClusterManager.GetTestCluster(1, 0, false, DefaultMaxClusterCreateRetries, false, false);
            testCluster.UpdateConfig("enable_user_defined_functions: true");
            testCluster.Start(1);
            var builder = Cluster
                .Builder()
                .AddContactPoint(testCluster.InitialContactPoint);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, "ks_func", 1));
                session.Execute("CREATE TABLE ks_func.tbl1 (id int PRIMARY KEY, v1 int, v2 int)");
                session.Execute("INSERT INTO ks_func.tbl1 (id, v1, v2) VALUES (1, 1, 0)");
                session.Execute("CREATE FUNCTION ks_func.div(a int, b int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return a / b;'");

                Assert.Throws<FunctionFailureException>(() =>
                    session.Execute("SELECT ks_func.div(v1,v2) FROM ks_func.tbl1 where id = 1"));
            }
        }

        ///////////////////////
        /// Helper Methods
        ///////////////////////

        private class WhiteListLoadBalancingPolicy: ILoadBalancingPolicy
        {
            private readonly ILoadBalancingPolicy _childPolicy = new RoundRobinPolicy();
            private readonly byte[] _list;

            public WhiteListLoadBalancingPolicy(params byte[] listLastOctet)
            {
                _list = listLastOctet;
            }

            public void Initialize(ICluster cluster)
            {
                _childPolicy.Initialize(cluster);
            }

            public HostDistance Distance(Host host)
            {
                return _childPolicy.Distance(host);
            }

            public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
            {
                var hosts = _childPolicy.NewQueryPlan(keyspace, query);
                return hosts.Where(h => _list.Contains(TestHelper.GetLastAddressByte(h)));
            }
        }
    }
}
