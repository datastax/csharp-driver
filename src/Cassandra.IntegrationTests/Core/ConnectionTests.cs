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
using Cassandra.Responses;
using Cassandra.Serialization;
using Microsoft.IO;

namespace Cassandra.IntegrationTests.Core
{
    [TestTimeout(600000), Category("short")]
    public class ConnectionTests : TestGlobals
    {
        [OneTimeSetUp]
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
                var task = Query(connection, "SELECT * FROM system.local");
                task.Wait();
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                //Result from Cassandra
                var output = ValidateResult<OutputRows>(task.Result);
                var rs = output.RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.NotNull(rows[0].GetValue<string>("key"), "It should contain a value for key column");
            }
        }

        [Test]
        public void Prepare_Query()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                var request = new PrepareRequest("SELECT * FROM system.local");
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
                var request = new PrepareRequest("SELECT WILL FAIL");
                var task = connection.Send(request);
                task.ContinueWith(t =>
                {
                    Assert.AreEqual(TaskStatus.Faulted, t.Status);
                    Assert.NotNull(t.Exception);
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
                var prepareRequest = new PrepareRequest("SELECT * FROM system.local");
                var task = connection.Send(prepareRequest);
                var prepareOutput = ValidateResult<OutputPrepared>(task.Result);
                
                //Execute the prepared query
                var executeRequest = new ExecuteRequest(GetLatestProtocolVersion(), prepareOutput.QueryId, null, false, QueryProtocolOptions.Default);
                task = connection.Send(executeRequest);
                var output = ValidateResult<OutputRows>(task.Result);
                var rs = output.RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("key") != null, "It should contain a key column");
            }
        }

        [Test]
        public void Execute_Prepared_With_Param_Test()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();

                var prepareRequest = new PrepareRequest("SELECT * FROM system.local WHERE key = ?");
                var task = connection.Send(prepareRequest);
                var prepareOutput = ValidateResult<OutputPrepared>(task.Result);

                var options = new QueryProtocolOptions(ConsistencyLevel.One, new object[] { "local" }, false, 100, null, ConsistencyLevel.Any);

                var executeRequest = new ExecuteRequest(GetLatestProtocolVersion(), prepareOutput.QueryId, null, false, options);
                task = connection.Send(executeRequest);
                var output = ValidateResult<OutputRows>(task.Result);

                var rows = output.RowSet.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.NotNull(rows[0].GetValue<string>("key"), "It should contain a key column value");
            }
        }

#if !NETCORE
        [Test]
        [TestCassandraVersion(2, 0)]
        public void Query_Compression_LZ4_Test()
        {
            var protocolOptions = new ProtocolOptions().SetCompression(CompressionType.LZ4);
            using (var connection = CreateConnection(protocolOptions))
            {
                connection.Open().Wait();

                //Start a query
                var task = Query(connection, "SELECT * FROM system.local", QueryProtocolOptions.Default);
                task.Wait(360000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                var output = ValidateResult<OutputRows>(task.Result);
                var rs = output.RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Query_Compression_LZ4_With_Parallel_Queries()
        {
            var protocolOptions = new ProtocolOptions().SetCompression(CompressionType.LZ4);
            using (var connection = CreateConnection(protocolOptions))
            {
                connection.Open().Wait();

                var tasks = new Task<Response>[16];
                for (var i = 0; i < tasks.Length; i++)
                {
                    //schema_columns
                    // ReSharper disable once AccessToDisposedClosure
                    tasks[i] = Task.Factory.StartNew(() => Query(connection, "SELECT * FROM system.local", QueryProtocolOptions.Default)).Unwrap();
                }
                // ReSharper disable once CoVariantArrayConversion
                Task.WaitAll(tasks);
                foreach (var t in tasks)
                {
                    var output = ValidateResult<OutputRows>(t.Result);
                    var rs = output.RowSet;
                    var rows = rs.ToList();
                    Assert.Greater(rows.Count, 0);
                    Assert.True(rows[0].GetValue<string>("key") != null, "It should contain a key");
                }
            }
        }
#endif

        [Test]
        public void Query_Compression_Snappy_Test()
        {
            var protocolOptions = new ProtocolOptions().SetCompression(CompressionType.Snappy);
            using (var connection = CreateConnection(protocolOptions, null))
            {
                connection.Open().Wait();

                //Start a query
                var task = Query(connection, "SELECT * FROM system.local", QueryProtocolOptions.Default);
                task.Wait(360000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                var output = ValidateResult<OutputRows>(task.Result);
                var rs = output.RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
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
                var taskList = new List<Task<Response>>();
                //Run a query multiple times
                for (var i = 0; i < 16; i++)
                {
                    taskList.Add(Query(connection, "SELECT * FROM system.local", QueryProtocolOptions.Default));
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
                for (var i = 0; i < connection.MaxConcurrentRequests * 1.2; i++)
                {
                    taskList.Add(Query(connection, selectQuery, QueryProtocolOptions.Default));
                }
                try
                {
                    Task.WaitAll(taskList.ToArray());
                }
                catch (AggregateException)
                {
                    
                }
                Assert.True(taskList.All(t => 
                    t.Status == TaskStatus.RanToCompletion ||
                    (t.Exception != null && t.Exception.InnerException is ReadTimeoutException)), "Not all task completed");
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
                    var task = Query(connection, "SELECT * FROM system.local", QueryProtocolOptions.Default);
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
                var task = connection.Send(new RegisterForEventRequest(eventTypes));
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
                Assert.That((eventArgs as SchemaChangeEventArgs).Table, Is.Null.Or.Empty);

                //create a table and check if gets received as an event
                Query(connection, String.Format(TestUtils.CreateTableAllTypes, "test_events_kp.test_table", 1)).Wait(1000);
                eventHandle.WaitOne(2000);
                Assert.IsNotNull(eventArgs);
                Assert.IsInstanceOf<SchemaChangeEventArgs>(eventArgs);

                Assert.AreEqual(SchemaChangeEventArgs.Reason.Created, (eventArgs as SchemaChangeEventArgs).What);
                Assert.AreEqual("test_events_kp", (eventArgs as SchemaChangeEventArgs).Keyspace);
                Assert.AreEqual("test_table", (eventArgs as SchemaChangeEventArgs).Table);

                if (CassandraVersion >= Version.Parse("2.1"))
                {
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

        [Test, TestTimeout(5000)]
        public void Send_And_Wait()
        {
            using (var connection = CreateConnection())
            {
                connection.Open().Wait();
                const string query = "SELECT * FROM system.local";
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

                var taskList = new List<Task<Response>>();
                //Run the query multiple times
                for (var i = 0; i < 129; i++)
                {
                    taskList.Add(Query(connection, "SELECT * FROM system.local", QueryProtocolOptions.Default));
                }
                Task.WaitAll(taskList.ToArray());
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion), "Not all task completed");

                //One last time
                var task = Query(connection, "SELECT * FROM system.local");
                Assert.True(task.Result != null);
            }
        }

        /// Tests that a ssl connection to a host with ssl disabled fails (not hangs)
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-336
        /// 
        /// @test_category conection:ssl
        [Test]
        public void Ssl_Connect_With_Ssl_Disabled_Host()
        {
            var config = new Configuration(Cassandra.Policies.DefaultPolicies, 
                new ProtocolOptions(ProtocolOptions.DefaultPort, new SSLOptions()),
                new PoolingOptions(),
                 new SocketOptions().SetConnectTimeoutMillis(200),
                 new ClientOptions(),
                 NoneAuthProvider.Instance,
                 null,
                 new QueryOptions(),
                 new DefaultAddressTranslator());
            config.BufferPool = new RecyclableMemoryStreamManager();
            using (var connection = CreateConnection(GetLatestProtocolVersion(), config))
            {
                var ex = Assert.Throws<AggregateException>(() => connection.Open().Wait(10000));
                if (ex.InnerException is TimeoutException)
                {
                    //Under .NET, SslStream.BeginAuthenticateAsClient Method() never calls back
                    //So we throw a TimeoutException
                    StringAssert.IsMatch("SSL", ex.InnerException.Message);
                }
                else if (ex.InnerException is System.IO.IOException)
                {
                    //Under Mono and others, it throws a IOException
                }
                else
                {
                    throw new AssertionException(string.Format("Expected TimeoutException or IOException, obtained {0}", ex.InnerException.GetType()));
                }
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
                TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM local", QueryProtocolOptions.Default));
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
                TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM system.local", QueryProtocolOptions.Default));
            }
        }

        [Test]
        public async Task SetKeyspace_Parallel_Calls_Serially_Executes()
        {
            const string queryKs1 = "create keyspace if not exists ks_to_switch_p1 WITH replication = " +
                                    "{'class': 'SimpleStrategy', 'replication_factor' : 1}";
            const string queryKs2 = "create keyspace if not exists ks_to_switch_p2 WITH replication = " +
                                    "{'class': 'SimpleStrategy', 'replication_factor' : 1}";
            // ReSharper disable AccessToDisposedClosure, AccessToModifiedClosure
            using (var connection = CreateConnection())
            {
                await connection.Open();
                Assert.Null(connection.Keyspace);
                await Query(connection, queryKs1);
                await Query(connection, queryKs2);
                await Task.Delay(100);
                var counter = 0;
                connection.WriteCompleted += () => Interlocked.Increment(ref counter);
                TestHelper.ParallelInvoke(new Action[]
                {
                    () => connection.SetKeyspace("ks_to_switch_p1").Wait(),
                    () => connection.SetKeyspace("ks_to_switch_p2").Wait(),
                    () => connection.SetKeyspace("system").Wait()
                });
                CollectionAssert.Contains(new[] { "ks_to_switch_p1", "ks_to_switch_p2", "system" }, connection.Keyspace);
                await Task.Delay(200);
                Assert.AreEqual(3, Volatile.Read(ref counter));
            }
            // ReSharper enable AccessToDisposedClosure, AccessToModifiedClosure
        }

        [Test]
        public async Task SetKeyspace_Parallel_Calls_With_Same_Name_Executes_Once()
        {
            using (var connection = CreateConnection(null, null, new PoolingOptions().SetHeartBeatInterval(0)))
            {
                await connection.Open();
                Assert.Null(connection.Keyspace);
                var actions = new Action[100]
                    .Select<Action, Action>(_ => () => connection.SetKeyspace("system").Wait())
                    .ToArray();
                await Task.Delay(100);
                var counter = 0;
                connection.WriteCompleted += () => Interlocked.Increment(ref counter);
                TestHelper.ParallelInvoke(actions);
                Assert.AreEqual("system", connection.Keyspace);
                await Task.Delay(200);
                Assert.AreEqual(1, Volatile.Read(ref counter));
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
                connection.WriteCompleted += () => Interlocked.Increment(ref counter);
                var tasks = new Task[]
                {
                    connection.SetKeyspace("system"),
                    connection.SetKeyspace("ks_to_switch_s1"),
                    connection.SetKeyspace("ks_to_switch_s2"),
                };
                Task.WaitAll(tasks);
                CollectionAssert.Contains(new[] { "ks_to_switch_s1", "ks_to_switch_s2", "system" }, connection.Keyspace);
                Thread.Sleep(400);
                Assert.GreaterOrEqual(counter, 3);
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
            config.BufferPool = new RecyclableMemoryStreamManager();
            using (var connection = new Connection(new Serializer(GetLatestProtocolVersion()), new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 1 }), 9042), config))
            {
                var ex = Assert.Throws<SocketException>(() => TaskHelper.WaitToComplete(connection.Open()));
                Assert.AreEqual(SocketError.TimedOut, ex.SocketErrorCode);
            }
            using (var connection = new Connection(new Serializer(GetLatestProtocolVersion()), new IPEndPoint(new IPAddress(new byte[] { 255, 255, 255, 255 }), 9042), config))
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
            var taskList = new List<Task<Response>>();
            for (var i = 0; i < 1024; i++)
            {
                taskList.Add(Query(connection, "SELECT * FROM system.local"));
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
            var task = Query(connection, "SELECT * FROM system.local");
            //It will throw
            Assert.Throws<AggregateException>(() => task.Wait(50));
        }

        /// <summary>
        /// It checks that the connection startup method throws an exception when using a greater protocol version
        /// </summary>
        [Test]
        [TestCassandraVersion(2, 2, Comparison.LessThan)]
        public void Startup_Greater_Protocol_Version_Throws()
        {
            const byte protocolVersion = 4;
            var config = new Configuration
            {
                BufferPool = new RecyclableMemoryStreamManager()
            };
            using (var connection = CreateConnection(protocolVersion, config))
            {
                var ex = Assert.Throws<UnsupportedProtocolVersionException>(() => TaskHelper.WaitToComplete(connection.Open()));
                StringAssert.Contains(string.Format("Protocol version {0} not supported", protocolVersion), ex.Message);
            }
        }

        [Test]
        public void With_Heartbeat_Enabled_Should_Send_Request()
        {
            using (var connection = CreateConnection(null, null, new PoolingOptions().SetHeartBeatInterval(500)))
            {
                connection.Open().Wait();
                //execute a dummy query
                TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM system.local", QueryProtocolOptions.Default));

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
                TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM system.local", QueryProtocolOptions.Default));

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
                TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM system.local", QueryProtocolOptions.Default));
                var called = 0;
                connection.OnIdleRequestException += _ => called++;
                connection.Kill();
                Thread.Sleep(2000);
                Assert.AreEqual(1, called);
            }
        }

        /// Tests that connection heartbeats are enabled by default
        ///
        /// Heartbeat_Should_Be_Enabled_By_Default tests that connection heartbeats are enabled by default. It creates a default
        /// connection and verfies in the poolingoptions configuration that the hearbeat interval is set to 30 seconds. It then performs
        /// a sample query, kills the connection, and verfieis that one OnIdleRequestException is raised due to the connection being closed
        /// and the heartbeat is not returned.
        ///
        /// @since 3.0.0
        /// @jira_ticket CSHARP-375
        /// @expected_result Connection heartbeat should be enabled by default
        ///
        /// @test_category connection:heartbeat
        [Test]
        public void Heartbeat_Should_Be_Enabled_By_Default()
        {
            const int defaultHeartbeatInterval = 30000;
            using (var connection = CreateConnection(null, null, new PoolingOptions()))
            {
                connection.Open().Wait();
                Assert.AreEqual(defaultHeartbeatInterval, connection.Configuration.PoolingOptions.GetHeartBeatInterval());

                //execute a dummy query
                TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM system.local", QueryProtocolOptions.Default));
                var called = 0;
                connection.OnIdleRequestException += _ => called++;
                connection.Kill();
                Thread.Sleep(defaultHeartbeatInterval + 2000);
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
            if (cassandraVersion >= Version.Parse("2.2"))
            {
                protocolVersion = 4;
            }
            else if (cassandraVersion >= Version.Parse("2.1"))
            {
                protocolVersion = 3;
            }
            else if (cassandraVersion >= Version.Parse("2.0"))
            {
                protocolVersion = 2;
            }
            return protocolVersion;
        }

        private Connection CreateConnection(byte protocolVersion, Configuration config)
        {
            Trace.TraceInformation("Creating test connection using protocol v{0}", protocolVersion);
            return new Connection(new Serializer(protocolVersion), new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 9042), config);
        }

        private Task<Response> Query(Connection connection, string query, QueryProtocolOptions options = null)
        {
            if (options == null)
            {
                options = QueryProtocolOptions.Default;
            }
            var request = new QueryRequest(GetLatestProtocolVersion(), query, false, options);
            return connection.Send(request);
        }

        private static T ValidateResult<T>(Response response)
        {
            Assert.IsInstanceOf<ResultResponse>(response);
            Assert.IsInstanceOf<T>(((ResultResponse)response).Output);
            return (T)((ResultResponse)response).Output;
        }
    }
}
