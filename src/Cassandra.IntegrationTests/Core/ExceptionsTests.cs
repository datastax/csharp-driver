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

using System.Security;
using System.Security.Permissions;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class ExceptionsTests
    {
        public ExceptionsTests()
        {
            Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("en-US");
            Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en-US");
        }

        /// <summary>
        ///  Tests the AlreadyExistsException. Create a keyspace twice and a table twice.
        ///  Catch and test all the exception methods.
        /// </summary>
        [Test]
        public void AlreadyExistsExceptionCcm()
        {
            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                var session = clusterInfo.Session;
                const string keyspace = "TestKeyspace";
                const string table = "TestTable";

                String[] cqlCommands =
                {
                    String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1),
                    String.Format("USE \"{0}\"", keyspace),
                    String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table)
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
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        /// <summary>
        ///  Tests DriverInternalError. Tests basic message, rethrow, and copy abilities.
        /// </summary>
        [Test]
        public void DriverInternalError()
        {
            var errorMessage = "Test Message";

            try
            {
                throw new DriverInternalError(errorMessage);
            }
            catch (DriverInternalError e1)
            {
                try
                {
                    throw new DriverInternalError("", e1);
                }
                catch (DriverInternalError e2)
                {
                    Assert.AreEqual(e2.InnerException.Message, errorMessage);
                }
            }
        }


        /// <summary>
        ///  Tests InvalidConfigurationInQueryException. Tests basic message abilities.
        /// </summary>
        [Test]
        public void InvalidConfigurationInQueryExceptionCCM()
        {
            var errorMessage = "Test Message";

            try
            {
                throw new InvalidConfigurationInQueryException(errorMessage);
            }
            catch (InvalidConfigurationInQueryException e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests the NoHostAvailableException. by attempting to build a cluster using
        ///  the IP address "255.255.255.255" and test all available exception methods.
        /// </summary>
        [Test]
        public void NoHostAvailableExceptionCCM()
        {
            var ipAddress = "255.255.255.255";
            var errorsHashMap = new Dictionary<IPAddress, Exception>();
            errorsHashMap.Add(IPAddress.Parse(ipAddress), null);

            try
            {
                var cluster = Cluster.Builder().AddContactPoint("255.255.255.255").Build();
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
        public void ReadTimeoutExceptionCcm()
        {
            var clusterInfo = TestUtils.CcmSetup(3);
            try
            {
                var session = clusterInfo.Session;

                var keyspace = "TestKeyspace";
                var table = "TestTable";
                var replicationFactor = 3;
                var key = "1";

                session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor));
                Thread.Sleep(5000);
                session.ChangeKeyspace(keyspace);
                session.Execute(String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table));
                Thread.Sleep(3000);

                session.Execute(
                    new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(
                        ConsistencyLevel.All));
                session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));

                TestUtils.CcmStopForce(clusterInfo, 2);
                var ex = Assert.Throws<ReadTimeoutException>(() => 
                    session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All)));
                Assert.AreEqual(ex.ConsistencyLevel, ConsistencyLevel.All);
                Assert.AreEqual(ex.ReceivedAcknowledgements, 2);
                Assert.AreEqual(ex.RequiredAcknowledgements, 3);
                Assert.AreEqual(ex.WasDataRetrieved, true);
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        /// <summary>
        ///  Tests SyntaxError. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void SyntaxErrorCCM()
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
        public void TraceRetrievalExceptionCCM()
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
        public void TruncateExceptionCCM()
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
        public void UnauthorizedExceptionCCM()
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
        public void UnavailableExceptionCCM()
        {
            var clusterInfo = TestUtils.CcmSetup(3);
            try
            {
                var session = clusterInfo.Session;

                var keyspace = "TestKeyspace";
                var table = "TestTable";
                var replicationFactor = 3;
                var key = "1";

                session.WaitForSchemaAgreement(
                    session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor))
                    );
                session.ChangeKeyspace(keyspace);
                session.WaitForSchemaAgreement(
                    session.Execute(String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table))
                    );

                session.Execute(
                    new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(
                        ConsistencyLevel.All));
                session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));

                TestUtils.CcmStopNode(clusterInfo, 2);
                // Ensure that gossip has reported the node as down.
                Thread.Sleep(1000);

                try
                {
                    session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));
                    Assert.Fail("It should throw an exception");
                }
                catch (Exception ex)
                {
                    if (ex is UnavailableException)
                    {
                        var e = (UnavailableException)ex;
                        Assert.AreEqual(e.Consistency, ConsistencyLevel.All);
                        Assert.AreEqual(e.RequiredReplicas, replicationFactor);
                        Assert.AreEqual(e.AliveReplicas, replicationFactor - 1);
                    }
                    else if (ex is ReadTimeoutException)
                    {
                        var e = (ReadTimeoutException)ex;
                        Assert.AreEqual(e.ConsistencyLevel, ConsistencyLevel.All);
                    }
                    else
                    {
                        Assert.Fail("It can be either a UnavailableException or a ReadTimeoutException from Cassandra");
                    }
                }

                try
                {
                    session.Execute(
                        new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(
                            ConsistencyLevel.All));
                    Assert.Fail("It should throw an exception");
                }
                catch (Exception ex)
                {
                    if (ex is UnavailableException)
                    {
                        var e = (UnavailableException)ex;
                        Assert.AreEqual(e.Consistency, ConsistencyLevel.All);
                        Assert.AreEqual(e.RequiredReplicas, replicationFactor);
                        Assert.AreEqual(e.AliveReplicas, replicationFactor - 1);
                    }
                    else if (ex is WriteTimeoutException)
                    {
                        var e = (WriteTimeoutException)ex;
                        Assert.AreEqual(e.ConsistencyLevel, ConsistencyLevel.All);
                    }
                    else
                    {
                        Assert.Fail("It can be either a UnavailableException or a ReadTimeoutException from Cassandra: {0}", ex.GetType().FullName);
                    }
                }
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        /// <summary>
        ///  Tests the WriteTimeoutException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then forcibly kill single node and attempt to write the
        ///  same key at CL.ALL. Catch and test all available exception methods.
        /// </summary>
        [Test]
        public void WriteTimeoutExceptionCCM()
        {
            var clusterInfo = TestUtils.CcmSetup(3);
            try
            {
                var session = clusterInfo.Session;

                var keyspace = "TestKeyspace";
                var table = "TestTable";
                var replicationFactor = 3;
                var key = "1";

                session.WaitForSchemaAgreement(
                    session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor))
                    );
                session.ChangeKeyspace(keyspace);
                session.WaitForSchemaAgreement(
                    session.Execute(String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table))
                    );

                session.Execute(
                    new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(
                        ConsistencyLevel.All));
                session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));

                TestUtils.CcmStopForce(clusterInfo, 2);
                try
                {
                    session.Execute(
                        new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(
                            ConsistencyLevel.All));
                }
                catch (WriteTimeoutException e)
                {
                    Assert.AreEqual(e.ConsistencyLevel, ConsistencyLevel.All);
                    Assert.AreEqual(e.ReceivedAcknowledgements, 2);
                    Assert.AreEqual(e.RequiredAcknowledgements, 3);
                    Assert.AreEqual(e.WriteType, "SIMPLE");
                }
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void PreserveStackTraceTest()
        {
            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                PreserveStackTraceAssertions();
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        public static void PreserveStackTraceAssertions()
        {
            var ipPrefix = AppDomain.CurrentDomain.GetData("ipPrefix");
            if (ipPrefix == null)
            {
                ipPrefix = Options.Default.IP_PREFIX;
            }
            var cluster = Cluster.Builder().AddContactPoint(ipPrefix.ToString() + "1").Build();
            var session = cluster.Connect();
            var ex = Assert.Throws<SyntaxError>(() => session.Execute("SELECT WILL FAIL"));
            //Must maintain the original call stack trace
            Assert.True(ex.StackTrace.Contains("PreserveStackTraceAssertions"));
            Assert.True(ex.StackTrace.Contains("ExceptionsTests"));
        }

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
        public void ExceptionsOnPartialTrustTest()
        {

            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                var appDomain = CreatePartialTrustDomain();
                appDomain.SetData("ipPrefix", Options.Default.IP_PREFIX);
                appDomain.DoCallBack(PreserveStackTraceAssertions);
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void RowsetIteratedTwice()
        {
            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                var session = clusterInfo.Session;

                var keyspace = "TestKeyspace";
                var table = "TestTable";
                var key = "1";

                session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
                session.ChangeKeyspace(keyspace);
                session.Execute(String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table));

                session.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)));
                var rowset = session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table))).GetRows();
                //Linq Count iterates
                //The first time should give you the rows
                Assert.AreEqual(1, rowset.Count());
                //The following times should be consumed
                Assert.AreEqual(0, rowset.Count());
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void RowSetPagingAfterSessionDispose()
        {
            var clusterInfo = TestUtils.CcmSetup(1);
            try 
            {
                var localSession = clusterInfo.Session;

                var keyspace = "TestKeyspace";
                var table = "TestTable";

                localSession.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
                localSession.ChangeKeyspace(keyspace);
                localSession.Execute(String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table));

                localSession.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, "1", "foo", 42, 24.03f)));
                localSession.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, "2", "foo", 42, 24.03f)));
                var rs = localSession.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetPageSize(1));
                if (Options.Default.CassandraVersion.Major < 2)
                {
                    //Paging should be ignored in 1.x
                    //But setting the page size should work
                    Assert.AreEqual(2, rs.InnerQueueCount);
                    return;
                }
                Assert.AreEqual(1, rs.InnerQueueCount);

                localSession.Dispose();
                //It should not fail, just do nothing
                rs.FetchMoreResults();
                Assert.AreEqual(1, rs.InnerQueueCount);
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }
    }
}
