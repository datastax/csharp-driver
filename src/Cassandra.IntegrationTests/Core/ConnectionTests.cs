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
                task.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
            }
        }

        [Test]
        public void QueryBasicTest()
        {
            using (var connection = CreateConnection())
            {
                connection.Init();
                var startupTask = connection.Startup();
                startupTask.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, startupTask.Status);

                //Start a query
                var task = connection.Query();
                task.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                //Result status from Cassandra
                Assert.IsInstanceOf<ResultResponse>(task.Result);
                var result = (ResultResponse)task.Result;
                Assert.IsInstanceOf<OutputRows>(result.Output);
                var rs = ((OutputRows)result.Output).RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
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
                var task = connection.Query();
                task.Wait(360000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                Assert.IsInstanceOf<ResultResponse>(task.Result);
                var result = (ResultResponse)task.Result;
                var rs = ((OutputRows)result.Output).RowSet;
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
                var task = connection.Query();
                task.Wait(360000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                Assert.IsInstanceOf<ResultResponse>(task.Result);
                var result = (ResultResponse)task.Result;
                var rs = ((OutputRows)result.Output).RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
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
                startupTask.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, startupTask.Status);
                var taskList = new List<Task<AbstractResponse>>();
                //Run a query multiple times
                for (var i = 0; i < 16; i++)
                {
                    taskList.Add(connection.Query());
                }
                Task.WaitAll(taskList.ToArray(), 3000);
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
                    taskList.Add(connection.Query());
                }
                Task.WaitAll(taskList.ToArray(), 120000);
                Assert.AreEqual(taskList.Count, taskList.Select(t => t.Status == TaskStatus.RanToCompletion).Count());
                //Run the query a lot more times
                for (var i = 0; i < 1024; i++)
                {
                    taskList.Add(connection.Query());
                }
                Task.WaitAll(taskList.ToArray(), 360000);
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
                    var task = connection.Query();
                    task.Wait(1000);
                    Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                    Assert.NotNull(task.Result);
                }
            }
        }

        [Test]
        public void InitOnWrongIpThrowsException()
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
