using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
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
                Assert.AreEqual(2, ((byte[])task.Result)[3]);
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
                //Ready status from Cassandra
                Assert.AreEqual(2, ((byte[])task.Result)[3]);
                task = connection.Query();
                task.Wait(1000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                //Result status from Cassandra
                Assert.AreEqual(8, ((byte[])task.Result)[3]);
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
                //Ready status from Cassandra
                Assert.AreEqual(2, ((byte[])task.Result)[3]);
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

        [Test, Timeout(10000)]
        public void QueryMultipleAsyncConsumeAllStreamIdsTest()
        {
            using (var connection = new Connection(new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 9042), new ProtocolOptions(), new SocketOptions()))
            {
                connection.Init();
                var task = connection.Startup();
                task.Wait(500);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                //Ready status from Cassandra
                Assert.AreEqual(2, ((byte[])task.Result)[3]);
                var taskList = new List<Task>();
                //Run the query multiple times
                for (var i = 0; i < 129; i++)
                {
                    taskList.Add(connection.Query());
                }
                Task.WaitAll(taskList.ToArray(), 2000);
                Assert.AreEqual(taskList.Count, taskList.Select(t => t.Status == TaskStatus.RanToCompletion).Count());
                //Run the query a lot more times
                for (var i = 0; i < 1024; i++)
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
                //Ready status from Cassandra
                Assert.AreEqual(2, ((byte[])task.Result)[3]);
                //Run a query multiple times
                for (var i = 0; i < 8; i++)
                {
                    task = connection.Query();
                    task.Wait(1000);
                    Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                }
                //Result status from Cassandra
                Assert.AreEqual(8, ((byte[])task.Result)[3]);
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
    }
}
