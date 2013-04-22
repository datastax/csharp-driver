using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MyTest;
using System.Threading;
using System.Net;


namespace Cassandra.MSTest
{
    
    [TestClass]
    public class ExceptionsTests
    {
        /// <summary>
        ///  Tests the AlreadyExistsException. Create a keyspace twice and a table twice.
        ///  Catch and test all the exception methods.
        /// </summary>
        [TestMethod]
        public void alreadyExistsException()
        {
            var builder = Cluster.Builder();
            CCMBridge.CCMCluster cluster = CCMBridge.CCMCluster.Create(1, builder);
            try
            {
                Session session = cluster.Session;
                String keyspace = "TestKeyspace";
                String table = "TestTable";

                String[] cqlCommands = new String[]{
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

                    String expected = String.Format("Cannot add existing keyspace \"{0}\"", keyspace.ToLower());
                    Assert.Equal(e.Message, expected);
                    Assert.Equal(e.Keyspace, keyspace.ToLower());
                    Assert.Equal(e.Table, "");
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


        public void authenticationException()
        {
            // TODO: Modify CCM to accept authenticated sessions
        }

        /// <summary>
        ///  Tests DriverInternalError. Tests basic message, rethrow, and copy abilities.
        /// </summary>

        [TestMethod]
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
                    throw new DriverInternalError("",e1);
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
        public void invalidConfigurationInQueryException()
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
        public void InvalidConfigurationInQueryException()
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
        public void noHostAvailableException()
        {
            String ipAddress = "255.255.255.255";
            Dictionary<IPAddress, Exception> errorsHashMap = new Dictionary<IPAddress, Exception>();
            errorsHashMap.Add(IPAddress.Parse(ipAddress), null);

            try
            {
                Cluster cluster = Cluster.Builder().AddContactPoint("255.255.255.255").Build();
            }
            catch (NoHostAvailableException e)
            {
                Assert.Equal(e.Message, String.Format("All host tried for query are in error (tried: {0})", ipAddress));
                Assert.Equal(e.Errors, errorsHashMap);

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
        [Priority]
        public void readTimeoutException()
        {
            var builder = Cluster.Builder();
            CCMBridge.CCMCluster cluster = CCMBridge.CCMCluster.Create(3, builder);
            try
            {
                Session session = cluster.Session;
                CCMBridge bridge = cluster.CassandraCluster;

                String keyspace = "TestKeyspace";
                String table = "TestTable";
                int replicationFactor = 3;
                String key = "1";

                session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor));
                session.Execute("USE " + keyspace);
                session.Execute(String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table));

                session.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(ConsistencyLevel.All));
                session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));

                bridge.ForceStop(2);
                try
                {
                    session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));
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
        public void syntaxError()
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
        public void traceRetrievalException()
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
        public void truncateException()
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
        public void unauthorizedException()
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
        public void unavailableException()
        {
            var builder = Cluster.Builder();
            CCMBridge.CCMCluster cluster = CCMBridge.CCMCluster.Create(3, builder);
            try
            {
                Session session = cluster.Session;
                CCMBridge bridge = cluster.CassandraCluster;

                String keyspace = "TestKeyspace";
                String table = "TestTable";
                int replicationFactor = 3;
                String key = "1";

                session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor));
                session.Execute("USE " + keyspace);
                session.Execute(String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table));

                session.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(ConsistencyLevel.All));
                session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));

                bridge.Stop(2);
                // Ensure that gossip has reported the node as down.
                Thread.Sleep(1000);

                try
                {
                    session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));
                }
                catch (UnavailableException e)
                {
                    String expectedError = String.Format("Not enough replica available for query at consistency {0} ({1} required but only {2} alive)", "ALL", 3, 2);
                    Assert.Equal(e.Message, expectedError);
                    Assert.Equal(e.Consistency, ConsistencyLevel.All);
                    Assert.Equal(e.RequiredReplicas, replicationFactor);
                    Assert.Equal(e.AliveReplicas, replicationFactor - 1);
                }

                try
                {
                    session.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(ConsistencyLevel.All));
                }
                catch (UnavailableException e)
                {
                    String expectedError = String.Format("Not enough replica available for query at consistency {0} ({1} required but only {2} alive)", "ALL", 3, 2);
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
        public void writeTimeoutException()
        {
            var builder = Cluster.Builder().AddContactPoint("cassi.cloudapp.net");
            CCMBridge.CCMCluster cluster = CCMBridge.CCMCluster.Create(3, builder);
            try
            {
                Session session = cluster.Session;
                CCMBridge bridge = cluster.CassandraCluster;

                String keyspace = "TestKeyspace";
                String table = "TestTable";
                int replicationFactor = 3;
                String key = "1";

                session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor));
                session.Execute("USE " + keyspace);
                session.Execute(String.Format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table));

                session.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(ConsistencyLevel.All));
                session.Execute(new SimpleStatement(String.Format(TestUtils.SELECT_ALL_FORMAT, table)).SetConsistencyLevel(ConsistencyLevel.All));

                bridge.ForceStop(2);
                try
                {
                    session.Execute(new SimpleStatement(String.Format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).SetConsistencyLevel(ConsistencyLevel.All));
                }
                catch (WriteTimeoutException e)
                {
                    Assert.Equal(e.ConsistencyLevel, ConsistencyLevel.All);
                    Assert.Equal(e.ReceivedAcknowledgements, 2);
                    Assert.Equal(e.RequiredAcknowledgements, 3);
                    //Assert.Equal(e.WriteType, WriteType.SIMPLE);
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
