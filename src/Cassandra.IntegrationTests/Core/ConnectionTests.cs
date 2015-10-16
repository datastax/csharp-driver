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

using Cassandra.IntegrationTests.TestBase;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tasks;
using Cassandra.Tests;
using Cassandra.Requests;
using Microsoft.IO;

namespace Cassandra.IntegrationTests.Core
{
    [Timeout(600000), Category("short")]
    public class ConnectionTests : TestGlobals
    {
        [TestFixtureSetUp]
        public void SetupFixture()
        {
            // we just need to make sure that there is a query-able cluster
            TestClusterManager.GetTestCluster(1, DefaultMaxClusterCreateRetries, true, false);
        }

        public ConnectionTests()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            Diagnostics.CassandraStackTraceIncluded = true;
        }

        [Test]
        public void Basic_Startup_Test()
        {
            using (var connection = CreateConnection())
            {
                Assert.DoesNotThrow(connection.Open().Wait);
            }
        }

        [Test]
        public void Basic_Query_Test()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                //Start a query
                var request = new QueryRequest(connection.ProtocolVersion, "SELECT * FROM system.schema_keyspaces", false, QueryProtocolOptions.Default);
                var task = connection.Send(request);
                task.Wait();
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                //Result from Cassandra
                var output = ValidateResult<OutputRows>(task.Result);
                var rs = output.RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
            }
        }

        [Test]
        public void Prepare_Query()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                var request = new PrepareRequest(connection.ProtocolVersion, "SELECT * FROM system.schema_keyspaces");
                var task = connection.Send(request);
                task.Wait();
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                ValidateResult<OutputPrepared>(task.Result);
            }
        }

        [Test]
        public void Prepare_ResponseError_Faults_Task()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                var request = new PrepareRequest(connection.ProtocolVersion, "SELECT WILL FAIL");
                var task = connection.Send(request);
                task.ContinueWith(t =>
                {
                    Assert.AreEqual(TaskStatus.Faulted, t.Status);
                    Assert.IsInstanceOf<SyntaxError>(t.Exception.InnerException);
                }, TaskContinuationOptions.ExecuteSynchronously).Wait();
            }
        }

        [Test]
        public void Execute_Prepared_Test()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();

                //Prepare a query
                var prepareRequest = new PrepareRequest(connection.ProtocolVersion, "SELECT * FROM system.schema_keyspaces");
                var task = connection.Send(prepareRequest);
                var prepareOutput = ValidateResult<OutputPrepared>(task.Result);
                
                //Execute the prepared query
                var executeRequest = new ExecuteRequest(connection.ProtocolVersion, prepareOutput.QueryId, null, false, QueryProtocolOptions.Default);
                task = connection.Send(executeRequest);
                var output = ValidateResult<OutputRows>(task.Result);
                var rs = output.RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
            }
        }

        [Test]
        public void Execute_Prepared_With_Param_Test()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();

                var prepareRequest = new PrepareRequest(connection.ProtocolVersion, "SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = ?");
                var task = connection.Send(prepareRequest);
                var prepareOutput = ValidateResult<OutputPrepared>(task.Result);

                var options = new QueryProtocolOptions(ConsistencyLevel.One, new[] { "system" }, false, 100, null, ConsistencyLevel.Any);

                var executeRequest = new ExecuteRequest(connection.ProtocolVersion, prepareOutput.QueryId, null, false, options);
                task = connection.Send(executeRequest);
                var output = ValidateResult<OutputRows>(task.Result);

                var rows = output.RowSet.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("columnfamily_name") != null, "It should contain a column family name");
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Query_Compression_LZ4_Test()
        {
            var protocolOptions = new ProtocolOptions().SetCompression(CompressionType.LZ4);
            using (var connection = CreateConnection(protocolOptions))
            {
                connection.Open().Wait();

                //Start a query
                var task = Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default);
                task.Wait(360000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                var output = ValidateResult<OutputRows>(task.Result);
                var rs = output.RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
            }
        }

        [Test]
        public void Query_Compression_Snappy_Test()
        {
            var protocolOptions = new ProtocolOptions().SetCompression(CompressionType.Snappy);
            using (var connection = CreateConnection(protocolOptions, null))
            {
                connection.Open().Wait();

                //Start a query
                var task = Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default);
                task.Wait(360000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                var output = ValidateResult<OutputRows>(task.Result);
                var rs = output.RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
            }
        }

        /// <summary>
        /// Test that a Response error from Cassandra results in a faulted task
        /// </summary>
        [Test]
        public void Query_ResponseError_Faults_Task()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();

                //Start a query
                var task = Query(connection, "SELECT WILL FAIL", QueryProtocolOptions.Default);
                task.ContinueWith(t =>
                {
                    Assert.AreEqual(TaskStatus.Faulted, t.Status);
                    Assert.IsInstanceOf<SyntaxError>(t.Exception.InnerException);
                }, TaskContinuationOptions.ExecuteSynchronously).Wait();
            }
        }

        [Test]
        public void Query_Multiple_Async_Test()
        {
            //Try to fragment the message
            var socketOptions = new SocketOptions().SetReceiveBufferSize(128);
            using (var connection = CreateConnection(null, socketOptions))
            {
                connection.Open().Wait();
                var taskList = new List<Task<AbstractResponse>>();
                //Run a query multiple times
                for (var i = 0; i < 16; i++)
                {
                    //schema_columns
                    taskList.Add(Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default));
                }
                Task.WaitAll(taskList.ToArray());
                foreach (var t in taskList)
                {
                    Assert.AreEqual(TaskStatus.RanToCompletion, t.Status);
                    Assert.NotNull(t.Result);
                }
            }
        }

        [Test]
        public void Query_Multiple_Async_Consume_All_StreamIds_Test()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                var createKeyspaceTask = Query(connection, "CREATE KEYSPACE ks_conn_consume WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}");
                TaskHelper.WaitToComplete(createKeyspaceTask, 3000);
                var createTableTask = Query(connection, "CREATE TABLE ks_conn_consume.tbl1 (id uuid primary key)");
                TaskHelper.WaitToComplete(createTableTask, 3000);
                var id = Guid.NewGuid().ToString("D");
                var insertTask = Query(connection, "INSERT INTO ks_conn_consume.tbl1 (id) VALUES (" + id + ")");
                TaskHelper.WaitToComplete(insertTask, 3000);
                Assert.AreEqual(TaskStatus.RanToCompletion, createTableTask.Status);
                var taskList = new List<Task>();
                //Run the query more times than the max allowed
                var selectQuery = "SELECT id FROM ks_conn_consume.tbl1 WHERE id = " + id;
                for (var i = 0; i < connection.MaxConcurrentRequests * 2; i++)
                {
                    taskList.Add(Query(connection, selectQuery, QueryProtocolOptions.Default));
                }
                Task.WaitAll(taskList.ToArray());
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion), "Not all task completed");
            }
        }

        [Test]
        public void Query_Multiple_Sync_Test()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                //Run a query multiple times
                for (var i = 0; i < 8; i++)
                {
                    var task = Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default);
                    task.Wait(1000);
                    Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                    Assert.NotNull(task.Result);
                }
            }
        }

        [Test]
        public void Register_For_Events()
        {
            var eventHandle = new AutoResetEvent(false);
            CassandraEventArgs eventArgs = null;
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                var eventTypes = CassandraEventType.TopologyChange | CassandraEventType.StatusChange | CassandraEventType.SchemaChange;
                var task = connection.Send(new RegisterForEventRequest(connection.ProtocolVersion, eventTypes));
                TaskHelper.WaitToComplete(task, 1000);
                Assert.IsInstanceOf<ReadyResponse>(task.Result);
                connection.CassandraEventResponse += (o, e) =>
                {
                    eventArgs = e;
                    eventHandle.Set();
                };
                //create a keyspace and check if gets received as an event
                Query(connection, String.Format(TestUtils.CreateKeyspaceSimpleFormat, "test_events_kp", 1)).Wait(1000);
                eventHandle.WaitOne(2000);
                Assert.IsNotNull(eventArgs);
                Assert.IsInstanceOf<SchemaChangeEventArgs>(eventArgs);
                Assert.AreEqual(SchemaChangeEventArgs.Reason.Created, (eventArgs as SchemaChangeEventArgs).What);
                Assert.AreEqual("test_events_kp", (eventArgs as SchemaChangeEventArgs).Keyspace);
                Assert.IsNullOrEmpty((eventArgs as SchemaChangeEventArgs).Table);

                //create a table and check if gets received as an event
                Query(connection, String.Format(TestUtils.CreateTableAllTypes, "test_events_kp.test_table", 1)).Wait(1000);
                eventHandle.WaitOne(2000);
                Assert.IsNotNull(eventArgs);
                Assert.IsInstanceOf<SchemaChangeEventArgs>(eventArgs);

                Assert.AreEqual(SchemaChangeEventArgs.Reason.Created, (eventArgs as SchemaChangeEventArgs).What);
                Assert.AreEqual("test_events_kp", (eventArgs as SchemaChangeEventArgs).Keyspace);
                Assert.AreEqual("test_table", (eventArgs as SchemaChangeEventArgs).Table);

                if (connection.ProtocolVersion >= 3)
                {
                    //create a udt type
                    Query(connection, "CREATE TYPE test_events_kp.test_type (street text, city text, zip int);").Wait(1000);
                    eventHandle.WaitOne(2000);
                    Assert.IsNotNull(eventArgs);
                    Assert.IsInstanceOf<SchemaChangeEventArgs>(eventArgs);

                    Assert.AreEqual(SchemaChangeEventArgs.Reason.Created, (eventArgs as SchemaChangeEventArgs).What);
                    Assert.AreEqual("test_events_kp", (eventArgs as SchemaChangeEventArgs).Keyspace);
                    Assert.AreEqual("test_type", (eventArgs as SchemaChangeEventArgs).Type);
                }
            }
        }

        [Test, Timeout(5000)]
        public void Send_And_Wait()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                const string query = "SELECT * FROM system.schema_columns";
                Query(connection, query).
                    ContinueWith((t) =>
                    {
                        //Try to deadlock
                        Query(connection, query).Wait();
                    }, TaskContinuationOptions.ExecuteSynchronously).Wait();
            }

        }

        [Test]
        public void StreamMode_Read_And_Write()
        {
            using (var connection = CreateConnection(new ProtocolOptions(), new SocketOptions().SetStreamMode(true)))
            {
                connection.Open().Wait();

                var taskList = new List<Task<AbstractResponse>>();
                //Run the query multiple times
                for (var i = 0; i < 129; i++)
                {
                    taskList.Add(Query(connection, "SELECT * FROM system.schema_columns", QueryProtocolOptions.Default));
                }
                Task.WaitAll(taskList.ToArray());
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion), "Not all task completed");

                //One last time
                var task = Query(connection, "SELECT * FROM system.schema_keyspaces");
                Assert.True(task.Result != null);
            }
        }

        [Test]
        [Explicit]
        public void Ssl_Test()
        {
            var certs = new X509CertificateCollection();
            RemoteCertificateValidationCallback callback = (s, cert, chain, policyErrors) =>
            {
                if (policyErrors == SslPolicyErrors.None)
                {
                    return true;
                }
                if ((policyErrors & SslPolicyErrors.RemoteCertificateChainErrors) == SslPolicyErrors.RemoteCertificateChainErrors &&
                    chain.ChainStatus.Length == 1 &&
                    chain.ChainStatus[0].Status == X509ChainStatusFlags.UntrustedRoot)
                {
                    //self issued
                    return true;
                }
                return false;
            };
            var sslOptions = new SSLOptions().SetCertificateCollection(certs).SetRemoteCertValidationCallback(callback);
            using (var connection = CreateConnection(new ProtocolOptions(ProtocolOptions.DefaultPort, sslOptions)))
            {
                connection.Open().Wait();

                var taskList = new List<Task<AbstractResponse>>();
                //Run the query multiple times
                for (var i = 0; i < 129; i++)
                {
                    taskList.Add(Query(connection, "SELECT * FROM system.schema_columns", QueryProtocolOptions.Default));
                }
                Task.WaitAll(taskList.ToArray());
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion), "Not all task completed");

                //One last time
                var task = Query(connection, "SELECT * FROM system.schema_keyspaces");
                Assert.True(task.Result != null);
            }
        }
        
        [Test]
        public void SetKeyspace_Test()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                Assert.Null(connection.Keyspace);
                connection.SetKeyspace("system").Wait();
                //If it was executed correctly, it should be set
                Assert.AreEqual("system", connection.Keyspace);
                //Execute a query WITHOUT the keyspace prefix
                TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM schema_keyspaces", QueryProtocolOptions.Default));
            }
        }

        [Test]
        public void SetKeyspace_Wrong_Name_Test()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                Assert.Null(connection.Keyspace);
                Assert.Throws<InvalidQueryException>(() => TaskHelper.WaitToComplete(connection.SetKeyspace("KEYSPACE_Y_DOES_NOT_EXISTS")));
                //The keyspace should still be null
                Assert.Null(connection.Keyspace);
                //Execute a query WITH the keyspace prefix still works
                TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default));
            }
        }

        [Test]
        public void SetKeyspace_Parallel_Calls_Serially_Executes()
        {
            const string queryKs1 = "create keyspace ks_to_switch_p1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
            const string queryKs2 = "create keyspace ks_to_switch_p2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                Assert.Null(connection.Keyspace);
                TaskHelper.WaitToComplete(Query(connection, queryKs1));
                TaskHelper.WaitToComplete(Query(connection, queryKs2));
                var counter = 0;
                connection.WriteCompleted += () => Interlocked.Increment(ref counter);
                TestHelper.ParallelInvoke(new Action[]
                {
                    // ReSharper disable AccessToDisposedClosure
                    () => connection.SetKeyspace("ks_to_switch_p1").Wait(),
                    () => connection.SetKeyspace("ks_to_switch_p2").Wait(),
                    () => connection.SetKeyspace("system").Wait()
                    // ReSharper enable AccessToDisposedClosure
                });
                CollectionAssert.Contains(new[] { "ks_to_switch_p1", "ks_to_switch_p2", "system" }, connection.Keyspace);
                Assert.AreEqual(3, counter);
            }
        }

        [Test]
        public void SetKeyspace_Serial_Calls_Serially_Executes()
        {
            const string queryKs1 = "create keyspace ks_to_switch_s1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
            const string queryKs2 = "create keyspace ks_to_switch_s2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                Assert.Null(connection.Keyspace);
                TaskHelper.WaitToComplete(Query(connection, queryKs1));
                TaskHelper.WaitToComplete(Query(connection, queryKs2));
                var counter = 0;
                connection.WriteCompleted += () => counter++;
                var tasks = new Task[]
                {
                    connection.SetKeyspace("system"),
                    connection.SetKeyspace("ks_to_switch_s1"),
                    connection.SetKeyspace("ks_to_switch_s2"),
                };
                Task.WaitAll(tasks);
                CollectionAssert.Contains(new[] { "ks_to_switch_s1", "ks_to_switch_s2", "system" }, connection.Keyspace);
                Assert.AreEqual(3, counter);
            }
        }

        [Test]
        public void SetKeyspace_After_Disposing_Faults_Task()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                Assert.Null(connection.Keyspace);
                connection.Dispose();
                //differentiate the task creation from the waiting
                var task = connection.SetKeyspace("system");
                Assert.Throws<SocketException>(() => TaskHelper.WaitToComplete(task));
            }
        }

        [Test]
        public void SetKeyspace_When_Disposing_Faults_Task()
        {
            //Invoke multiple times, as it involves different threads and can be scheduled differently
            const string queryKs = "create keyspace ks_to_switch_when1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                Assert.Null(connection.Keyspace);
                TaskHelper.WaitToComplete(Query(connection, queryKs));
            }
            TestHelper.Invoke(() =>
            {
                using (var connection = CreateConnection())
                {
                    connection.Open().Wait();
                    Assert.Null(connection.Keyspace);
                    var tasks = new Task[10];
                    TestHelper.ParallelInvoke(new Action[]
                    {
                        () => tasks[0] = connection.SetKeyspace("system"),
                        () => connection.Dispose()
                    });
                    for (var i = 1; i < 10; i++)
                    {
                        tasks[i] = connection.SetKeyspace("ks_to_switch_when1");
                    }
                    var ex = Assert.Throws<AggregateException>(() => Task.WaitAll(tasks));
                    var unexpectedException = ex.InnerExceptions.FirstOrDefault(e => !(e is SocketException));
                    Assert.Null(unexpectedException);
                }
            }, 10);
        }

        [Test]
        public void Wrong_Ip_Init_Throws_Exception()
        {
            var socketOptions = new SocketOptions();
            socketOptions.SetConnectTimeoutMillis(1000);
            var config = new Configuration(
                new Cassandra.Policies(), 
                new ProtocolOptions(), 
                new PoolingOptions(), 
                socketOptions, 
                new ClientOptions(), 
                NoneAuthProvider.Instance,
                null,
                new QueryOptions(),
                new DefaultAddressTranslator());
            using (var connection = new Connection(1, new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 1 }), 9042), config))
            {
                var ex = Assert.Throws<SocketException>(() => TaskHelper.WaitToComplete(connection.Open()));
                Assert.AreEqual(SocketError.TimedOut, ex.SocketErrorCode);
            }
            using (var connection = new Connection(1, new IPEndPoint(new IPAddress(new byte[] { 255, 255, 255, 255 }), 9042), config))
            {
                Assert.Throws<SocketException>(() => TaskHelper.WaitToComplete(connection.Open()));
            }
        }

        [Test]
        public void Connection_Close_Faults_AllPending_Tasks()
        {
            var connection = CreateConnection();
            connection.Open().Wait();
            //Queue a lot of read and writes
            var taskList = new List<Task<AbstractResponse>>();
            for (var i = 0; i < 1024; i++)
            {
                taskList.Add(Query(connection, "SELECT * FROM system.schema_keyspaces"));
            }
            for (var i = 0; i < 1000; i++)
            {
                if (connection.InFlight > 0)
                {
                    Trace.TraceInformation("Inflight {0}", connection.InFlight);
                    break;
                }
                //Wait until there is an operation in flight
                Thread.Sleep(50);
            }
            //Close the socket, this would trigger all pending ops to be called back
            connection.Dispose();
            try
            {
                Task.WaitAll(taskList.ToArray());
            }
            catch (AggregateException)
            {
                //Its alright, it will fail
            }

            Assert.True(!taskList.Any(t => t.Status != TaskStatus.RanToCompletion && t.Status != TaskStatus.Faulted), "Must be only completed and faulted task");

            //A new call to write will be called back immediately with an exception
            var task = Query(connection, "SELECT * FROM system.schema_keyspaces");
            //It will throw
            Assert.Throws<AggregateException>(() => task.Wait(50));
        }

        /// <summary>
        /// It checks that the connection startup method throws an exception when using a greater protocol version
        /// </summary>
        [Test]
        [TestCassandraVersion(2, 0, Comparison.LessThan)]
        public void Startup_Greater_Protocol_Version_Throws()
        {
            const byte protocolVersion = 2;
            using (var connection = CreateConnection(protocolVersion, new Configuration()))
            {
                Assert.Throws<UnsupportedProtocolVersionException>(() => TaskHelper.WaitToComplete(connection.Open()));
            }
        }

        [Test]
        public void With_Heartbeat_Enabled_Should_Send_Request()
        {
            using (var connection = CreateConnection(null, null, new PoolingOptions().SetHeartBeatInterval(500)))
            {
                connection.Open().Wait();
                //execute a dummy query
                TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default));

                var writeCounter = 0;
                connection.WriteCompleted += () => writeCounter++;
                Thread.Sleep(2200);
                Assert.AreEqual(4, writeCounter);
            }
        }

        [Test]
        public void With_Heartbeat_Disabled_Should_Not_Send_Request()
        {
            using (var connection = CreateConnection(null, null, new PoolingOptions().SetHeartBeatInterval(0)))
            {
                connection.Open().Wait();
                //execute a dummy query
                TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default));

                Thread.Sleep(500);
                var writeCounter = 0;
                connection.WriteCompleted += () => writeCounter++;
                Thread.Sleep(2200);
                Assert.AreEqual(0, writeCounter);
            }
        }

        [Test]
        public void With_HeartbeatEnabled_Should_Raise_When_Connection_Closed()
        {
            using (var connection = CreateConnection(null, null, new PoolingOptions().SetHeartBeatInterval(500)))
            {
                connection.Open().Wait();
                //execute a dummy query
                TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default));
                var called = 0;
                connection.OnIdleRequestException += (_) => called++;
                connection.Kill();
                Thread.Sleep(2000);
                Assert.AreEqual(1, called);
            }
        }

        private Connection CreateConnection(ProtocolOptions protocolOptions = null, SocketOptions socketOptions = null, PoolingOptions poolingOptions = null)
        {
            if (socketOptions == null)
            {
                socketOptions = new SocketOptions();
            }
            if (protocolOptions == null)
            {
                protocolOptions = new ProtocolOptions();
            }
            var config = new Configuration(
                new Cassandra.Policies(),
                protocolOptions,
                poolingOptions,
                socketOptions,
                new ClientOptions(false, 20000, null),
                NoneAuthProvider.Instance,
                null,
                new QueryOptions(),
                new DefaultAddressTranslator());
            config.BufferPool = new RecyclableMemoryStreamManager();
            config.Timer = new HashedWheelTimer();
            return CreateConnection(GetLatestProtocolVersion(), config);
        }

        /// <summary>
        /// Gets the latest protocol depending on the Cassandra Version running the tests
        /// </summary>
        private byte GetLatestProtocolVersion()
        {
            var cassandraVersion = CassandraVersion;
            byte protocolVersion = 1;
            if (cassandraVersion >= Version.Parse("2.1"))
            {
                protocolVersion = 3;
            }
            else if (cassandraVersion > Version.Parse("2.0"))
            {
                protocolVersion = 2;
            }
            return protocolVersion;
        }

        private Connection CreateConnection(byte protocolVersion, Configuration config)
        {
            Trace.TraceInformation("Creating test connection using protocol v{0}", protocolVersion);
            return new Connection(protocolVersion, new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 9042), config);
        }

        private static Task<AbstractResponse> Query(Connection connection, string query, QueryProtocolOptions options = null)
        {
            if (options == null)
            {
                options = QueryProtocolOptions.Default;
            }
            var request = new QueryRequest(connection.ProtocolVersion, query, false, options);
            return connection.Send(request);
        }

        private static T ValidateResult<T>(AbstractResponse response)
        {
            Assert.IsInstanceOf<ResultResponse>(response);
            Assert.IsInstanceOf<T>(((ResultResponse)response).Output);
            return (T)((ResultResponse)response).Output;
        }
    }
}
