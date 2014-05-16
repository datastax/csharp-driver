//
//      Copyright (C) 2012 DataStax Inc.
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
        public void AlreadyExistsExceptionCCM()
        {
            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                var session = clusterInfo.Session;
                String keyspace = "TestKeyspace";
                String table = "TestTable";

                String[] cqlCommands =
                {
                    String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1),
                    "USE " + keyspace,
                    String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table)
                };

                // Create the schema once
                session.Execute(cqlCommands[0]);
                session.Execute(cqlCommands[1]);
                session.Execute(cqlCommands[2]);

                // Try creating the keyspace again
                try
                {
                    session.Execute(cqlCommands[0]);
                }
                catch (AlreadyExistsException e)
                {
                    String expected = String.Format("Keyspace {0} already exists", keyspace.ToLower());

                    Assert.AreEqual(e.Message, expected);
                    Assert.AreEqual(e.Keyspace, keyspace.ToLower());
                    Assert.AreEqual(e.Table, null);
                    Assert.AreEqual(e.WasTableCreation, false);
                }

                session.Execute(cqlCommands[1]);

                // Try creating the table again
                try
                {
                    session.Execute(cqlCommands[2]);
                }
                catch (AlreadyExistsException e)
                {
                    Assert.AreEqual(e.Keyspace, keyspace.ToLower());
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
            String errorMessage = "Test Message";

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
            String errorMessage = "Test Message";

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
            String ipAddress = "255.255.255.255";
            var errorsHashMap = new Dictionary<IPAddress, Exception>();
            errorsHashMap.Add(IPAddress.Parse(ipAddress), null);

            try
            {
                Cluster cluster = Cluster.Builder().AddContactPoint("255.255.255.255").Build();
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
        public void ReadTimeoutExceptionCCM()
        {
            var clusterInfo = TestUtils.CcmSetup(3);
            try
            {
                var session = clusterInfo.Session;

                String keyspace = "TestKeyspace";
                String table = "TestTable";
                int replicationFactor = 3;
                String key = "1";

                session.WaitForSchemaAgreement(
                    session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor)));
                session.Execute("USE " + keyspace);
                session.WaitForSchemaAgreement(
                    session.Execute(String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table)));

                session.Execute(
                    new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(
                        ConsistencyLevel.All));
                session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));

                TestUtils.CcmStopForce(clusterInfo, 2);
                try
                {
                    session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));
                }
                catch (ReadTimeoutException e)
                {
                    Assert.AreEqual(e.ConsistencyLevel, ConsistencyLevel.All);
                    Assert.AreEqual(e.ReceivedAcknowledgements, 2);
                    Assert.AreEqual(e.RequiredAcknowledgements, 3);
                    Assert.AreEqual(e.WasDataRetrieved, true);
                }
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
            String errorMessage = "Test Message";

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
            String errorMessage = "Test Message";

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
            String errorMessage = "Test Message";

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
            String errorMessage = "Test Message";

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

                String keyspace = "TestKeyspace";
                String table = "TestTable";
                int replicationFactor = 3;
                String key = "1";

                session.WaitForSchemaAgreement(
                    session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor))
                    );
                session.Execute("USE " + keyspace);
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

                String keyspace = "TestKeyspace";
                String table = "TestTable";
                int replicationFactor = 3;
                String key = "1";

                session.WaitForSchemaAgreement(
                    session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor))
                    );
                session.Execute("USE " + keyspace);
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
        public void RowsetIteratedTwice()
        {
            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                var session = clusterInfo.Session;

                String keyspace = "TestKeyspace";
                String table = "TestTable";
                String key = "1";

                session.WaitForSchemaAgreement(
                    session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1))
                    );
                session.Execute("USE " + keyspace);
                session.WaitForSchemaAgreement(
                    session.Execute(String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table))
                    );

                session.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)));
                IEnumerable<Row> rowset = session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table))).GetRows();
                //Linq Count interates
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

                String keyspace = "TestKeyspace";
                String table = "TestTable";

                localSession.WaitForSchemaAgreement(
                    localSession.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1)));
                localSession.Execute("USE " + keyspace);
                localSession.WaitForSchemaAgreement(
                    localSession.Execute(String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table)));

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