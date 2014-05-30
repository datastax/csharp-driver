using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture]
    [Timeout(600000)]
    public class ConnectionTests
    {
        private TraceLevel _originalTraceLevel;
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            _originalTraceLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Error;
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            Diagnostics.CassandraTraceSwitch.Level = _originalTraceLevel;
        }

        [Test]
        public void BasicStartupTest()
        {
            using (var connection = CreateConnection())
            {
                connection.Init();
                var task = connection.Startup();
                task.Wait(3000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
            }
        }

        [Test]
        public void BasicQueryTest()
        {
            using (var connection = CreateConnection())
            {
                connection.Init();
                var startupTask = connection.Startup();
                startupTask.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, startupTask.Status);

                //Start a query
                var task = connection.Query("SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default);
                task.Wait();
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                //Result status from Cassandra
                Assert.IsInstanceOf<OutputRows>(task.Result.Output);
                var rs = ((OutputRows)task.Result.Output).RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
            }
        }

        [Test]
        public void PrepareQuery()
        {
            using (var connection = CreateConnection())
            {
                connection.Init();
                var startupTask = connection.Startup();
                startupTask.Wait();
                Assert.AreEqual(TaskStatus.RanToCompletion, startupTask.Status);
                var task = connection.Prepare("SELECT * FROM system.schema_keyspaces");
                task.Wait();
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                Assert.IsInstanceOf<OutputPrepared>(task.Result.Output);
            }
        }

        [Test]
        public void PrepareResponseErrorFaultsTask()
        {
            using (var connection = CreateConnection())
            {
                connection.Init();
                var startupTask = connection.Startup();
                startupTask.Wait();
                Assert.AreEqual(TaskStatus.RanToCompletion, startupTask.Status);
                var task = connection.Prepare("SELECT WILL FAIL");
                task.ContinueWith(t =>
                {
                    Assert.AreEqual(TaskStatus.Faulted, t.Status);
                    Assert.IsInstanceOf<SyntaxError>(t.Exception.InnerException);
                }, TaskContinuationOptions.ExecuteSynchronously).Wait();
            }
        }

        [Test]
        public void ExecutePreparedTest()
        {
            using (var connection = CreateConnection())
            {
                connection.Init();
                var startupTask = connection.Startup();
                startupTask.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, startupTask.Status);

                //Prepare a query
                var task = connection.Prepare("SELECT * FROM system.schema_keyspaces");
                var queryId = ((OutputPrepared)task.Result.Output).QueryId;
                task = connection.Execute(queryId, QueryProtocolOptions.Default);
                var rs = ((OutputRows)task.Result.Output).RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
            }
        }

        [Test]
        public void ExecutePreparedWithParamTest()
        {
            using (var connection = CreateConnection())
            {
                connection.Init();
                var startupTask = connection.Startup();
                startupTask.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, startupTask.Status);

                //Prepare a query
                var task = connection.Prepare("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = ?");
                var queryId = ((OutputPrepared)task.Result.Output).QueryId;
                var options = new QueryProtocolOptions(ConsistencyLevel.One, new [] {"system"}, false, 100, null, ConsistencyLevel.Any);
                task = connection.Execute(queryId, options);
                var rs = ((OutputRows)task.Result.Output).RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("columnfamily_name") != null, "It should contain a column family name");
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void QueryCompressionLZ4Test()
        {
            var protocolOptions = new ProtocolOptions().SetCompression(CompressionType.LZ4);
            using (var connection = CreateConnection(protocolOptions))
            {
                connection.Init();
                var startupTask = connection.Startup();
                startupTask.Wait(360000);
                Assert.AreEqual(TaskStatus.RanToCompletion, startupTask.Status);

                //Start a query
                var task = connection.Query("SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default);
                task.Wait(360000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                var rs = ((OutputRows)task.Result.Output).RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
            }
        }

        [Test]
        public void QueryCompressionSnappyTest()
        {
            var protocolOptions = new ProtocolOptions().SetCompression(CompressionType.Snappy);
            using (var connection = CreateConnection(protocolOptions))
            {
                connection.Init();
                var startupTask = connection.Startup();
                startupTask.Wait(360000);
                Assert.AreEqual(TaskStatus.RanToCompletion, startupTask.Status);

                //Start a query
                var task = connection.Query("SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default);
                task.Wait(360000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                var rs = ((OutputRows)task.Result.Output).RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
            }
        }

        /// <summary>
        /// Test that a Response error from Cassandra results in a faulted task
        /// </summary>
        [Test]
        public void QueryResponseErrorFaultsTask()
        {
            using (var connection = CreateConnection())
            {
                connection.Init();
                var startupTask = connection.Startup();
                startupTask.Wait();
                Assert.AreEqual(TaskStatus.RanToCompletion, startupTask.Status);

                //Start a query
                var task = connection.Query("SELECT WILL FAIL", QueryProtocolOptions.Default);
                task.ContinueWith(t =>
                {
                    Assert.AreEqual(TaskStatus.Faulted, t.Status);
                    Assert.IsInstanceOf<SyntaxError>(t.Exception.InnerException);
                }, TaskContinuationOptions.ExecuteSynchronously).Wait();
            }
        }

        [Test]
        public void QueryMultipleAsyncTest()
        {
            //Try to fragment the message
            var socketOptions = new SocketOptions().SetReceiveBufferSize(128);
            using (var connection = CreateConnection(null, socketOptions))
            {
                connection.Init();
                var startupTask = connection.Startup();
                startupTask.Wait();
                Assert.AreEqual(TaskStatus.RanToCompletion, startupTask.Status);
                var taskList = new List<Task<ResultResponse>>();
                //Run a query multiple times
                for (var i = 0; i < 16; i++)
                {
                    //schema_columns
                    taskList.Add(connection.Query("SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default));
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
        public void QueryMultipleAsyncConsumeAllStreamIdsTest()
        {
            using (var connection = CreateConnection())
            {
                connection.Init();
                var task = connection.Startup();
                task.Wait(5000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                var taskList = new List<Task>();
                //Run the query multiple times
                for (var i = 0; i < 129; i++)
                {
                    taskList.Add(connection.Query("SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default));
                }
                Task.WaitAll(taskList.ToArray());
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion), "Not all task completed");
                //Run the query a lot more times
                for (var i = 0; i < 1024; i++)
                {
                    taskList.Add(connection.Query("SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default));
                }
                Task.WaitAll(taskList.ToArray());
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion), "Not all task completed");
            }
        }

        [Test]
        public void QueryMultipleSyncTest()
        {
            using (var connection = CreateConnection())
            {
                connection.Init();
                var startupTask = connection.Startup();
                startupTask.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, startupTask.Status);
                //Run a query multiple times
                for (var i = 0; i < 8; i++)
                {
                    var task = connection.Query("SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default);
                    task.Wait(1000);
                    Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                    Assert.NotNull(task.Result);
                }
            }
        }



        [Test]
        public void UseKeyspaceTest()
        {
            using (var connection = CreateConnection())
            {
                connection.Init();
                var task = connection.Startup();
                TaskHelper.WaitToComplete(task);
                Assert.Null(connection.Keyspace);
                connection.Keyspace = "system";
                //If it was executed correctly, it should be set
                Assert.AreEqual("system", connection.Keyspace);
                //Execute a query WITHOUT the keyspace prefix
                TaskHelper.WaitToComplete(connection.Query("SELECT * FROM schema_keyspaces", QueryProtocolOptions.Default));
            }
        }

        [Test]
        public void WrongIpInitThrowsException()
        {
            var socketOptions = new SocketOptions();
            socketOptions.SetConnectTimeoutMillis(1000);
            try
            {
                using (var connection = new Connection(1, new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 1 }), 9042), new ProtocolOptions(), socketOptions))
                {
                    connection.Init();
                    Assert.Fail("It must throw an exception");
                }
            }
            catch (SocketException ex)
            {
                //It should have timed out
                Assert.AreEqual(SocketError.TimedOut, ex.SocketErrorCode);
            }
            try
            {
                using (var connection = new Connection(1, new IPEndPoint(new IPAddress(new byte[] { 255, 255, 255, 255 }), 9042), new ProtocolOptions(), socketOptions))
                {
                    connection.Init();
                    Assert.Fail("It must throw an exception");
                }
            }
            catch (SocketException)
            {
                //Socket exception is just fine.
            }
        }

        [Test]
        public void ConnectionCloseFaultsAllPendingTasks()
        {
            throw new NotImplementedException();
        }

        private Connection CreateConnection(ProtocolOptions protocolOptions = null, SocketOptions socketOptions = null)
        {
            var protocolVersion = (byte) Options.Default.CassandraVersion.Major;
            if (protocolVersion > 2)
            {
                protocolVersion = 2;
            }
            if (socketOptions == null)
            {
                socketOptions = new SocketOptions();
            }
            if (protocolOptions == null)
            {
                protocolOptions = new ProtocolOptions();
            }
            return new Connection(protocolVersion, new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 9042), protocolOptions, socketOptions);
        }
    }
}
