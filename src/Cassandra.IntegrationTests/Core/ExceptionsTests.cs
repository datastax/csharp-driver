﻿//
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

namespace Cassandra.IntegrationTests.Core
{
    [TestClass]
    public class ExceptionsTests
    {
        /// <summary>
        ///  Tests the AlreadyExistsException. Create a keyspace twice and a table twice.
        ///  Catch and test all the exception methods.
        /// </summary>
        [TestMethod]
        [WorksForMe]
        public void alreadyExistsExceptionCCM()
        {
            Builder builder = Cluster.Builder();
            CCMBridge.CCMCluster cluster = CCMBridge.CCMCluster.Create(1, builder);
            try
            {
                Session session = cluster.Session;
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

                    Assert.Equal(e.Message, expected);
                    Assert.Equal(e.Keyspace, keyspace.ToLower());
                    Assert.Equal(e.Table, null);
                    Assert.Equal(e.WasTableCreation, false);
                }

                session.Execute(cqlCommands[1]);

                // Try creating the table again
                try
                {
                    session.Execute(cqlCommands[2]);
                }
                catch (AlreadyExistsException e)
                {
                    Assert.Equal(e.Keyspace, keyspace.ToLower());
                    Assert.Equal(e.Table, table.ToLower());
                    Assert.Equal(e.WasTableCreation, true);
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                cluster.Discard();
            }
        }


        public void authenticationExceptionCCM()
        {
            // TODO: Modify CCM to accept authenticated sessions
        }

        /// <summary>
        ///  Tests DriverInternalError. Tests basic message, rethrow, and copy abilities.
        /// </summary>
        [TestMethod]
        [WorksForMe]
        public void driverInternalError()
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
                    Assert.Equal(e2.InnerException.Message, errorMessage);

                    //DriverInternalError copy = (DriverInternalError)e2.copy();
                    //Assert.Equal(copy.Message, e2.Message);
                }
            }
        }


        /// <summary>
        ///  Tests InvalidConfigurationInQueryException. Tests basic message abilities.
        /// </summary>
        [TestMethod]
        [WorksForMe]
        public void invalidConfigurationInQueryExceptionCCM()
        {
            String errorMessage = "Test Message";

            try
            {
                throw new InvalidConfigurationInQueryException(errorMessage);
            }
            catch (InvalidConfigurationInQueryException e)
            {
                Assert.Equal(e.Message, errorMessage);
            }
        }


        /// <summary>
        ///  Tests InvalidConfigurationInQueryException. Tests basic message and copy abilities.
        /// </summary>
        [TestMethod]
        [WorksForMe]
        public void InvalidConfigurationInQueryExceptionCCM()
        {
            string errorMessage = "Test Message";

            try
            {
                throw new InvalidConfigurationInQueryException(errorMessage);
            }
            catch (InvalidQueryException e)
            {
                Assert.Equal(e.Message, errorMessage);

                //InvalidConfigurationInQueryException copy = (InvalidConfigurationInQueryException)e.copy();
                //Assert.Equal(copy.Message, e.Message);
            }
        }

        ///// <summary>
        /////  Tests InvalidTypeException. Tests basic message and copy abilities.
        ///// </summary>

        //[TestMethod]
        //public void invalidTypeException()
        //{
        //    String errorMessage = "Test Message";

        //    try
        //    {
        //        throw new InvalidTypeException(errorMessage);
        //    }
        //    catch (InvalidTypeException e)
        //    {
        //        Assert.Equal(e.Message, errorMessage);

        //        InvalidTypeException copy = (InvalidTypeException)e.copy();
        //        Assert.Equal(copy.Message, e.Message);
        //    }
        //}

        /// <summary>
        ///  Tests the NoHostAvailableException. by attempting to build a cluster using
        ///  the IP address "255.255.255.255" and test all available exception methods.
        /// </summary>
        [TestMethod]
        [WorksForMe]
        public void noHostAvailableExceptionCCM()
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
                Assert.Equal(e.Message, String.Format("All host tried for query are in error (tried: {0})", ipAddress));
                Assert.ArrEqual(e.Errors.Keys.ToArray(), errorsHashMap.Keys.ToArray());

                //NoHostAvailableException copy = (NoHostAvailableException)e.copy();
                //Assert.Equal(copy.Message, e.Message);
                //Assert.Equal(copy.Errors, e.Errors);
            }
        }

        /// <summary>
        ///  Tests the ReadTimeoutException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then forcibly kill single node and attempt a read of
        ///  the key at CL.ALL. Catch and test all available exception methods.
        /// </summary>
        [TestMethod]
        [WorksForMe]
        public void readTimeoutExceptionCCM()
        {
            Builder builder = Cluster.Builder();
            CCMBridge.CCMCluster cluster = CCMBridge.CCMCluster.Create(3, builder);
            try
            {
                Session session = cluster.Session;
                CCMBridge bridge = cluster.CCMBridge;

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
                session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All))
                       .Dispose();

                bridge.ForceStop(2);
                try
                {
                    session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All))
                           .Dispose();
                }
                catch (ReadTimeoutException e)
                {
                    Assert.Equal(e.ConsistencyLevel, ConsistencyLevel.All);
                    Assert.Equal(e.ReceivedAcknowledgements, 2);
                    Assert.Equal(e.RequiredAcknowledgements, 3);
                    Assert.Equal(e.WasDataRetrieved, true);

                    //ReadTimeoutException copy = (ReadTimeoutException)e.copy();
                    //Assert.Equal(copy.Message, e.Message);
                    //Assert.Equal(copy.WasDataRetrieved, e.WasDataRetrieved);
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                cluster.Discard();
            }
        }

        /// <summary>
        ///  Tests SyntaxError. Tests basic message and copy abilities.
        /// </summary>
        [TestMethod]
        [WorksForMe]
        public void syntaxErrorCCM()
        {
            String errorMessage = "Test Message";

            try
            {
                throw new SyntaxError(errorMessage);
            }
            catch (SyntaxError e)
            {
                Assert.Equal(e.Message, errorMessage);

                //SyntaxError copy = (SyntaxError)e.copy();
                //Assert.Equal(copy.Message, e.Message);
            }
        }

        /// <summary>
        ///  Tests TraceRetrievalException. Tests basic message.
        /// </summary>
        [TestMethod]
        [WorksForMe]
        public void traceRetrievalExceptionCCM()
        {
            String errorMessage = "Test Message";

            try
            {
                throw new TraceRetrievalException(errorMessage);
            }
            catch (TraceRetrievalException e)
            {
                Assert.Equal(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests TruncateException. Tests basic message and copy abilities.
        /// </summary>
        [TestMethod]
        [WorksForMe]
        public void truncateExceptionCCM()
        {
            String errorMessage = "Test Message";

            try
            {
                throw new TruncateException(errorMessage);
            }
            catch (TruncateException e)
            {
                Assert.Equal(e.Message, errorMessage);

                //TruncateException copy = (TruncateException)e.copy();
                //Assert.Equal(copy.Message, e.Message);
            }
        }

        /// <summary>
        ///  Tests UnauthorizedException. Tests basic message and copy abilities.
        /// </summary>
        [TestMethod]
        [WorksForMe]
        public void unauthorizedExceptionCCM()
        {
            String errorMessage = "Test Message";

            try
            {
                throw new UnauthorizedException(errorMessage);
            }
            catch (UnauthorizedException e)
            {
                Assert.Equal(e.Message, errorMessage);

                //UnauthorizedException copy = (UnauthorizedException)e.copy();
                //Assert.Equal(copy.Message, e.Message);
            }
        }

        /// <summary>
        ///  Tests the UnavailableException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then kill single node, wait for gossip to propogate the
        ///  new state, and attempt to read and write the same key at CL.ALL. Catch and
        ///  test all available exception methods.
        /// </summary>
        [TestMethod]
        [WorksForMe]
        public void unavailableExceptionCCM()
        {
            Builder builder = Cluster.Builder();
            CCMBridge.CCMCluster cluster = CCMBridge.CCMCluster.Create(3, builder);
            try
            {
                Session session = cluster.Session;
                CCMBridge bridge = cluster.CCMBridge;

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
                session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All))
                       .Dispose();

                bridge.Stop(2);
                // Ensure that gossip has reported the node as down.
                Thread.Sleep(1000);

                try
                {
                    session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All))
                           .Dispose();
                }
                catch (UnavailableException e)
                {
                    Assert.Equal(e.Consistency, ConsistencyLevel.All);
                    Assert.Equal(e.RequiredReplicas, replicationFactor);
                    Assert.Equal(e.AliveReplicas, replicationFactor - 1);
                }

                try
                {
                    session.Execute(
                        new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(
                            ConsistencyLevel.All));
                }
                catch (UnavailableException e)
                {
                    String expectedError = String.Format(
                        "Not enough replica available for query at consistency {0} ({1} required but only {2} alive)", ConsistencyLevel.All, 3, 2);
                    Assert.Equal(e.Message, expectedError);
                    Assert.Equal(e.Consistency, ConsistencyLevel.All);
                    Assert.Equal(e.RequiredReplicas, replicationFactor);
                    Assert.Equal(e.AliveReplicas, replicationFactor - 1);
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                cluster.Discard();
            }
        }

        /// <summary>
        ///  Tests the WriteTimeoutException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then forcibly kill single node and attempt to write the
        ///  same key at CL.ALL. Catch and test all available exception methods.
        /// </summary>
        [TestMethod]
        [WorksForMe]
        public void writeTimeoutExceptionCCM()
        {
            Builder builder = Cluster.Builder();
            CCMBridge.CCMCluster cluster = CCMBridge.CCMCluster.Create(3, builder);
            try
            {
                Session session = cluster.Session;
                CCMBridge bridge = cluster.CCMBridge;

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
                session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All))
                       .Dispose();

                bridge.ForceStop(2);
                try
                {
                    session.Execute(
                        new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(
                            ConsistencyLevel.All));
                }
                catch (WriteTimeoutException e)
                {
                    Assert.Equal(e.ConsistencyLevel, ConsistencyLevel.All);
                    Assert.Equal(e.ReceivedAcknowledgements, 2);
                    Assert.Equal(e.RequiredAcknowledgements, 3);
                    Assert.Equal(e.WriteType, "SIMPLE");
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                cluster.Discard();
            }
        }

        [TestMethod]
        [WorksForMe]
        public void rowsetIteratedTwice()
        {
            Builder builder = Cluster.Builder();
            CCMBridge.CCMCluster cluster = CCMBridge.CCMCluster.Create(1, builder);
            try
            {
                Session session = cluster.Session;
                CCMBridge bridge = cluster.CCMBridge;

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
                int cnt = rowset.Count();
                try
                {
                    foreach (Row r in rowset)
                        Console.Write(r.GetValue<string>("k"));
                    Assert.Fail();
                }
                catch (InvalidOperationException)
                {
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                cluster.Discard();
            }
        }
    }
}