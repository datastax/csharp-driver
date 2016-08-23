#if !NETCORE
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Serialization;
using Cassandra.Tasks;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class HostConnectionPoolTests
    {
        private static readonly IPEndPoint Address = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1000);
        private const byte ProtocolVersion = 2;
        private static readonly Host Host1 = TestHelper.CreateHost("127.0.0.1");

        private static IPEndPoint GetIpEndPoint(byte lastByte = 1)
        {
            return new IPEndPoint(IPAddress.Parse("127.0.0." + lastByte), 9042);
        }

        private static Connection CreateConnection(byte lastIpByte = 1, Configuration config = null)
        {
            if (config == null)
            {
                config = GetConfig();
            }
            return new Connection(new Serializer(ProtocolVersion), GetIpEndPoint(lastIpByte), config);
        }

        private static Mock<HostConnectionPool> GetPoolMock(Host host = null, Configuration config = null)
        {
            if (host == null)
            {
                host = Host1;
            }
            if (config == null)
            {
                config = GetConfig();
            }
            return new Mock<HostConnectionPool>(host, HostDistance.Local, config, new Serializer(ProtocolVersion));
        }

        private static Configuration GetConfig(int coreConnections = 3, int maxConnections = 8)
        {
            var pooling = new PoolingOptions()
                .SetCoreConnectionsPerHost(HostDistance.Local, coreConnections)
                .SetMaxConnectionsPerHost(HostDistance.Local, maxConnections);
            var config = new Configuration(Policies.DefaultPolicies,
                 new ProtocolOptions(),
                 pooling,
                 new SocketOptions(),
                 new ClientOptions(),
                 NoneAuthProvider.Instance,
                 null,
                 new QueryOptions(),
                 new DefaultAddressTranslator());
            config.BufferPool = new Microsoft.IO.RecyclableMemoryStreamManager();
            return config;
        }

        private static Connection GetConnectionMock(int inflight, int timedOutOperations = 0)
        {
            var config = new Configuration
            {
                BufferPool = new Microsoft.IO.RecyclableMemoryStreamManager()
            };
            var connectionMock = new Mock<Connection>(MockBehavior.Loose, new Serializer(4), Address, config);
            connectionMock.Setup(c => c.InFlight).Returns(inflight);
            connectionMock.Setup(c => c.TimedOutOperations).Returns(timedOutOperations);
            return connectionMock.Object;
        }

        public HostConnectionPoolTests()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
        }

        [Test]
        public void MaybeCreateFirstConnection_Should_Yield_The_First_Connection_Opened()
        {
            var mock = GetPoolMock();
            var lastByte = 1;
            //use different addresses for same hosts to differentiate connections: for test only
            //different connections to same hosts should use the same address
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(CreateConnection((byte)lastByte++), 200 - lastByte * 50));
            var pool = mock.Object;
            var creation = pool.MaybeCreateFirstConnection();
            creation.Wait();
            Assert.AreEqual(1, creation.Result.Length);
            //yield the third connection first
            CollectionAssert.AreEqual(new[] {GetIpEndPoint()}, creation.Result.Select(c => c.Address));
        }

        [Test]
        public void MaybeCreateFirstConnection_Should_Yield_A_Connection_If_Any_Fails()
        {
            var mock = GetPoolMock();
            var counter = 0;
            //use different addresses for same hosts to differentiate connections: for test only
            //different connections to same hosts should use the same address
            mock.Setup(p => p.CreateConnection()).Returns(() =>
            {
                if (++counter == 2)
                {
                    return TaskHelper.FromException<Connection>(new Exception("Dummy exception"));
                }
                return TaskHelper.ToTask(CreateConnection());
            });
            var pool = mock.Object;
            var creation = pool.MaybeCreateFirstConnection();
            creation.Wait();
            Assert.AreEqual(creation.Result.Length, 1);
        }

        [Test]
        public void MaybeCreateFirstConnection_Serial_Calls_Should_Yield_First()
        {
            var mock = GetPoolMock();
            var lastByte = 1;
            mock.Setup(p => p.CreateConnection()).Returns(() =>
            {
                var c = CreateConnection((byte) lastByte++);
                if (lastByte == 2)
                {
                    return TestHelper.DelayedTask(c, 500);   
                }
                return TaskHelper.ToTask(c);
            });
            var pool = mock.Object;
            var creationTasks = new Task<Connection[]>[4];
            creationTasks[0] = pool.MaybeCreateFirstConnection();
            creationTasks[1] = pool.MaybeCreateFirstConnection();
            creationTasks[2] = pool.MaybeCreateFirstConnection();
            creationTasks[3] = pool.MaybeCreateFirstConnection();
            // ReSharper disable once CoVariantArrayConversion
            Task.WaitAll(creationTasks);
            Assert.AreEqual(1, TaskHelper.WaitToComplete(creationTasks[0]).Length);
            for (var i = 1; i < creationTasks.Length; i++)
            {
                Assert.AreEqual(1, TaskHelper.WaitToComplete(creationTasks[i]).Length);   
            }
        }

        [Test]
        public void MaybeCreateFirstConnection_Parallel_Calls_Should_Yield_First()
        {
            var mock = GetPoolMock();
            var lastByte = 1;
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(CreateConnection((byte)lastByte++), 1000));
            var pool = mock.Object;
            var creationTasks = new Task<Connection[]>[10];
            var counter = -1;
            var initialCreate = pool.MaybeCreateFirstConnection();
            TestHelper.ParallelInvoke(() =>
            {
                creationTasks[Interlocked.Increment(ref counter)] = pool.MaybeCreateFirstConnection();
            }, 10);
            // ReSharper disable once CoVariantArrayConversion
            Task.WaitAll(creationTasks);
            Assert.AreEqual(1, TaskHelper.WaitToComplete(initialCreate).Length);

            foreach (var t in creationTasks)
            {
                Assert.AreEqual(1, TaskHelper.WaitToComplete(t).Length);
            }
        }

        [Test]
        public void MaybeCreateFirstConnection_Fail_To_Open_All_Connections_Should_Fault_Task()
        {
            var mock = GetPoolMock();
            var testException = new Exception("Dummy exception");
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask<Connection>(() =>
            {
                throw testException;
            }));
            var pool = mock.Object;
            var task = pool.MaybeCreateFirstConnection();
            var ex = Assert.Throws<Exception>(() => TaskHelper.WaitToComplete(task));
            Assert.AreEqual(testException, ex);
        }

        [Test]
        public void MaybeCreateFirstConnection_Recreates_After_CheckHealth_Removes_Connection()
        {
            var host = TestHelper.CreateHost("127.0.0.1");
            var connections = new []
            {
                GetConnectionMock(100, SocketOptions.DefaultDefunctReadTimeoutThreshold + 10),
                GetConnectionMock(0)
            };
            var mock = GetPoolMock(host, connections[0].Configuration);
            var counter = 0;
            mock.Setup(p => p.CreateConnection()).Returns(() => TaskHelper.ToTask(connections[counter++]));
            var pool = mock.Object;
            pool.MaybeCreateFirstConnection().Wait();
            Assert.AreEqual(1, pool.OpenConnections.Count());
            var c = pool.OpenConnections.FirstOrDefault();
            Assert.NotNull(c);
            pool.CheckHealth(c);
            Assert.AreEqual(0, pool.OpenConnections.Count());
            //Recreate
            pool.MaybeCreateFirstConnection().Wait();
            Assert.AreEqual(1, pool.OpenConnections.Count());
        }

        [Test]
        public void MaybeCreateFirstConnection_After_Reconnection_Attempt_Waits_Existing()
        {
            //MaybeCreateFirstConnection() may be called meanwhile there is a reconnection attempt (is marked UP).
            var mock = GetPoolMock();
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(CreateConnection(), 300));
            var pool = mock.Object;
            Assert.AreEqual(0, pool.OpenConnections.Count());
            //A new connection is being created
            pool.AttemptReconnection();
            Assert.AreEqual(0, pool.OpenConnections.Count());
            pool.MaybeCreateFirstConnection().Wait();
            Thread.Sleep(500);
            //One and not 2
            Assert.AreEqual(1, pool.OpenConnections.Count());
        }

        [Test]
        public void MaybeIncreasePoolSize_Should_Increase_One_At_A_Time_Until_Core_Connections()
        {
            var mock = GetPoolMock();
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(CreateConnection(), 200));
            var pool = mock.Object;
            pool.MaybeCreateFirstConnection().Wait(1000);
            Assert.AreEqual(1, pool.OpenConnections.Count());
            var creatingNew = pool.MaybeIncreasePoolSize(0);
            Assert.True(creatingNew);
            //No connections added yet
            Assert.AreEqual(1, pool.OpenConnections.Count());
            Thread.Sleep(500);
            Assert.AreEqual(2, pool.OpenConnections.Count());
            creatingNew = pool.MaybeIncreasePoolSize(0);
            Assert.True(creatingNew);
            Thread.Sleep(500);
            Assert.AreEqual(3, pool.OpenConnections.Count());
            creatingNew = pool.MaybeIncreasePoolSize(0);
            Assert.False(creatingNew);
            Thread.Sleep(500);
            //Still core
            Assert.AreEqual(3, pool.OpenConnections.Count());
        }

        [Test]
        public void MaybeIncreasePoolSize_Should_Increase_One_At_A_Time_Until_Max_Connections()
        {
            var mock = GetPoolMock(null, GetConfig(1, 3));
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(CreateConnection(1, GetConfig(1, 3)), 200));
            var pool = mock.Object;
            pool.MaybeCreateFirstConnection().Wait(1000);
            Assert.AreEqual(1, pool.OpenConnections.Count());
            //inflight is low
            var creatingNew = pool.MaybeIncreasePoolSize(0);
            Assert.False(creatingNew);
            Assert.AreEqual(1, pool.OpenConnections.Count());
            Thread.Sleep(500);
            Assert.AreEqual(1, pool.OpenConnections.Count());
            creatingNew = pool.MaybeIncreasePoolSize(128);
            Assert.True(creatingNew);
            Thread.Sleep(500);
            Assert.AreEqual(2, pool.OpenConnections.Count());
            creatingNew = pool.MaybeIncreasePoolSize(128);
            Assert.True(creatingNew);
            Thread.Sleep(500);
            Assert.AreEqual(3, pool.OpenConnections.Count());
            creatingNew = pool.MaybeIncreasePoolSize(128);
            Assert.False(creatingNew);
            Thread.Sleep(500);
            //Still max
            Assert.AreEqual(3, pool.OpenConnections.Count());
        }

        [Test]
        public void MaybeIncreasePoolSize_Should_Not_Increase_When_Lower_Than_One_Connection()
        {
            var mock = GetPoolMock();
            mock.Setup(p => p.CreateConnection()).Returns(() => TaskHelper.ToTask(CreateConnection()));
            var pool = mock.Object;
            Assert.AreEqual(0, pool.OpenConnections.Count());
            Assert.False(pool.MaybeIncreasePoolSize(0));
            Thread.SpinWait(5);
            //Still 0 connections
            Assert.AreEqual(0, pool.OpenConnections.Count());
        }

        [Test]
        public void MaybeIncreasePoolSize_Should_Only_Increase_One_By_One()
        {
            var mock = GetPoolMock();
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(CreateConnection(), 200));
            var pool = mock.Object;
            pool.MaybeCreateFirstConnection().Wait(1000);
            Assert.AreEqual(1, pool.OpenConnections.Count());
            var connectionSpawned1 = pool.MaybeIncreasePoolSize(0);
            var connectionSpawned2 = pool.MaybeIncreasePoolSize(0);
            //No connections added yet
            Assert.AreEqual(1, pool.OpenConnections.Count());
            Assert.True(connectionSpawned1);
            Assert.True(connectionSpawned2);
            Thread.Sleep(500);
            Assert.AreEqual(2, pool.OpenConnections.Count());
        }

        [Test]
        public void MinInFlight_Returns_The_Min_Inflight_From_Two_Connections()
        {
            var connections = new[]
            {
                GetConnectionMock(0),
                GetConnectionMock(1),
                GetConnectionMock(1),
                GetConnectionMock(10),
                GetConnectionMock(1),
            };
            var index = 1;
            var c = HostConnectionPool.MinInFlight(connections, ref index);
            Assert.AreEqual(index, 2);
            Assert.AreSame(connections[2], c);
            c = HostConnectionPool.MinInFlight(connections, ref index);
            Assert.AreEqual(index, 3);
            //previous had less in flight
            Assert.AreSame(connections[2], c);
            c = HostConnectionPool.MinInFlight(connections, ref index);
            Assert.AreEqual(index, 4);
            Assert.AreSame(connections[4], c);
            c = HostConnectionPool.MinInFlight(connections, ref index);
            Assert.AreEqual(index, 5);
            Assert.AreSame(connections[0], c);
            c = HostConnectionPool.MinInFlight(connections, ref index);
            Assert.AreEqual(index, 6);
            Assert.AreSame(connections[0], c);
            index = 9;
            c = HostConnectionPool.MinInFlight(connections, ref index);
            Assert.AreEqual(index, 10);
            Assert.AreSame(connections[0], c);
        }

        [Test]
        public void AttemptReconnection_Should_Reconnect_In_The_Background()
        {
            var mock = GetPoolMock();
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(CreateConnection(), 20));
            var pool = mock.Object;
            Assert.AreEqual(0, pool.OpenConnections.Count());
            pool.AttemptReconnection();
            Thread.Sleep(100);
            Assert.AreEqual(1, pool.OpenConnections.Count());
        }

        [Test]
        public void AttemptReconnection_Should_Not_Create_A_New_Connection_If_There_Is_An_Open_Connection()
        {
            var mock = GetPoolMock();
            mock.Setup(p => p.CreateConnection()).Returns(() => TaskHelper.ToTask(CreateConnection()));
            var pool = mock.Object;
            //Create 1 connection
            TaskHelper.WaitToComplete(pool.MaybeCreateFirstConnection());
            Thread.SpinWait(1);
            Assert.AreEqual(1, pool.OpenConnections.Count());
            pool.AttemptReconnection();
            Thread.SpinWait(5);
            //Pool remains the same
            Assert.AreEqual(1, pool.OpenConnections.Count());
        }

        [Test]
        public void AttemptReconnection_After_Dispose_Should_Not_Create_New_Connections()
        {
            var mock = GetPoolMock();
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(CreateConnection(), 20));
            var pool = mock.Object;
            Assert.AreEqual(0, pool.OpenConnections.Count());
            pool.Dispose();
            pool.AttemptReconnection();
            Thread.Sleep(100);
            Assert.AreEqual(0, pool.OpenConnections.Count());
        }

        [Test]
        public void Dispose_Should_Cancel_Reconnection_Attempts()
        {
            var mock = GetPoolMock();
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(CreateConnection(), 200));
            var pool = mock.Object;
            Assert.AreEqual(0, pool.OpenConnections.Count());
            pool.AttemptReconnection();
            pool.Dispose();
            Thread.Sleep(500);
            Assert.AreEqual(0, pool.OpenConnections.Count());
        }

        [Test]
        public void OnHostCheckedAsDown_Should_Not_Schedule_Reconnection_When_Host_Is_Already_Reconnecting()
        {
            var host = TestHelper.CreateHost("127.0.0.1");
            var config = GetConfig(2);
            config.Timer = new HashedWheelTimer(100, 10);
            var mock = GetPoolMock(host, config);
            mock.Setup(p => p.CreateConnection()).Returns(() => TaskHelper.ToTask(CreateConnection()));
            var pool = mock.Object;
            Assert.AreEqual(0, pool.OpenConnections.Count());
            //fake other pool getting the flag
            host.SetAttemptingReconnection();
            //attempt reconnection should exit
            pool.OnHostCheckedAsDown(host, 10);
            Thread.Sleep(400);
            //Pool remains the same
            Assert.AreEqual(0, pool.OpenConnections.Count());
            config.Timer.Dispose();
        }

        [Test]
        public void OnHostCheckedAsDown_Should_Schedule_Reconnection()
        {
            var host = TestHelper.CreateHost("127.0.0.1");
            var config = GetConfig(2);
            config.Timer = new HashedWheelTimer(100, 10);
            var mock = GetPoolMock(host, config);
            mock.Setup(p => p.CreateConnection()).Returns(() => TaskHelper.ToTask(CreateConnection()));
            var pool = mock.Object;
            Assert.AreEqual(0, pool.OpenConnections.Count());
            //attempt reconnection should exit
            pool.OnHostCheckedAsDown(host, 10);
            Thread.Sleep(400);
            //Pool should grow
            Assert.AreEqual(1, pool.OpenConnections.Count());
            config.Timer.Dispose();
        }

        [Test]
        public void CheckHealth_Removes_Connection()
        {
            var host = TestHelper.CreateHost("127.0.0.1");
            var connection1 = GetConnectionMock(100, SocketOptions.DefaultDefunctReadTimeoutThreshold + 10);
            var mock = GetPoolMock(host, connection1.Configuration);
            mock.Setup(p => p.CreateConnection()).Returns(() => TaskHelper.ToTask(connection1));
            var pool = mock.Object;
            pool.MaybeCreateFirstConnection().Wait();
            Assert.AreEqual(1, pool.OpenConnections.Count());
            var c = pool.OpenConnections.FirstOrDefault();
            Assert.NotNull(c);
            pool.CheckHealth(c);
            Assert.AreEqual(0, pool.OpenConnections.Count());
        }
    }
}
#endif