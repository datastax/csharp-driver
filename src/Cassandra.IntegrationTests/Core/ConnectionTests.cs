using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
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
        [Test]
        public void StartupTest()
        {
            using (var connection = new Connection(new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 9042), new ProtocolOptions(), new SocketOptions()))
            {
                connection.Init();
                var task = connection.Startup();
                task.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                //Ready status from Cassandra
                Assert.IsInstanceOf<ReadyResponse>(task.Result);
            }
        }

        [Test]
        public void QueryTest()
        {
            using (var connection = new Connection(new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 9042), new ProtocolOptions(), new SocketOptions()))
            {
                connection.Init();
                var task = connection.Startup();
                task.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                Assert.IsInstanceOf<ReadyResponse>(task.Result);

                //Start a query
                task = connection.Query();
                task.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                //Result status from Cassandra
                Assert.IsInstanceOf<ResultResponse>(task.Result);
            }
        }

        [Test]
        public void QueryMultipleAsyncTest()
        {
            using (var connection = new Connection(new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 9042), new ProtocolOptions(), new SocketOptions()))
            {
                connection.Init();
                var task = connection.Startup();
                task.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                Assert.IsInstanceOf<ReadyResponse>(task.Result);
                var taskList = new List<Task>();
                //Run a query multiple times
                for (var i = 0; i < 8; i++)
                {
                    taskList.Add(connection.Query());
                }
                Task.WaitAll(taskList.ToArray(), 3000);
                foreach (var t in taskList)
                {
                    Assert.AreEqual(TaskStatus.RanToCompletion, t.Status);
                }
            }
        }

        [Test]
        public void QueryMultipleAsyncConsumeAllStreamIdsTest()
        {
            using (var connection = new Connection(new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 9042), new ProtocolOptions(), new SocketOptions()))
            {
                connection.Init();
                var task = connection.Startup();
                task.Wait(500);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                Assert.IsInstanceOf<ReadyResponse>(task.Result);
                var taskList = new List<Task>();
                //Run the query multiple times
                for (var i = 0; i < 129; i++)
                {
                    taskList.Add(connection.Query());
                }
                Task.WaitAll(taskList.ToArray(), 2000);
                Assert.AreEqual(taskList.Count, taskList.Select(t => t.Status == TaskStatus.RanToCompletion).Count());
                //Run the query a lot more times
                for (var i = 0; i < 2048; i++)
                {
                    taskList.Add(connection.Query());
                }
                Task.WaitAll(taskList.ToArray(), 5000);
                Assert.AreEqual(taskList.Count, taskList.Select(t => t.Status == TaskStatus.RanToCompletion).Count());
            }
        }

        [Test]
        public void QueryMultipleSyncTest()
        {
            using (var connection = new Connection(new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 9042), new ProtocolOptions(), new SocketOptions()))
            {
                connection.Init();
                var task = connection.Startup();
                task.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                Assert.IsInstanceOf<ReadyResponse>(task.Result);
                //Run a query multiple times
                for (var i = 0; i < 8; i++)
                {
                    task = connection.Query();
                    task.Wait(1000);
                    Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                }
                //Result status from Cassandra
                Assert.IsInstanceOf<ResultResponse>(task.Result);
            }
        }

        [Test]
        public void InitOnWrongIpThrowsException()
        {
            var socketOptions = new SocketOptions();
            socketOptions.SetConnectTimeoutMillis(1000);
            try
            {
                using (var connection = new Connection(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 1 }), 9042), new ProtocolOptions(), socketOptions))
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
                using (var connection = new Connection(new IPEndPoint(new IPAddress(new byte[] { 255, 255, 255, 255 }), 9042), new ProtocolOptions(), socketOptions))
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

        /// <summary>
        /// Basic unit test for the append buffer
        /// </summary>
        [Test]
        public void OperationStateAppendsReadBuffer()
        {
            var operationState = new OperationState();
            operationState.AddBuffer(new byte[] { 1, 2, 3, 4, 5 });
            Assert.AreEqual(new byte[] { 1, 2, 3, 4, 5 }, operationState.ReadBuffer);
            operationState.AddBuffer(new byte[] { 6, 7, 8, 9, 10, 11, 12});
            Assert.AreEqual(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, operationState.ReadBuffer);
        }

        [Test]
        public void SendConcurrentTest()
        {
            throw new NotImplementedException();
        }

        [Test]
        public void ConnectionCloseFaultsAllPendingTasks()
        {
            throw new NotImplementedException();
        }
    }
}
