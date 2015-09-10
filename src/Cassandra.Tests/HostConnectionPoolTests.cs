using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tasks;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class HostConnectionPoolTests
    {
        private const byte ProtocolVersion = 2;
        private static readonly Host Host1 = TestHelper.CreateHost("127.0.0.1");

        private static IPEndPoint GetIpEndPoint(byte lastByte = 1)
        {
            return new IPEndPoint(IPAddress.Parse("127.0.0." + lastByte), 9042);
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
            return config;
        }

        public HostConnectionPoolTests()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
        }

        [Test]
        public void MaybeCreateCorePool_Should_Yield_The_First_Connection_Opened()
        {
            var mock = new Mock<HostConnectionPool>(Host1, HostDistance.Local, GetConfig());
            var lastByte = 1;
            //use different addresses for same hosts to differentiate connections: for test only
            //different connections to same hosts should use the same address
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(new Connection(ProtocolVersion, GetIpEndPoint((byte)lastByte++), GetConfig()), 200 - lastByte * 50));
            var pool = mock.Object;
            var creation = pool.MaybeCreateCorePool();
            creation.Wait();
            Assert.AreEqual(1, creation.Result.Length);
            //yield the third connection first
            CollectionAssert.AreEqual(new[] {GetIpEndPoint(3)}, creation.Result.Select(c => c.Address));
            //Wait for all connections
            Thread.Sleep(200);
            Assert.AreEqual(3, pool.OpenConnections.Count());
            //following calls to create should yield 3 connections
            Assert.AreEqual(3, TaskHelper.WaitToComplete(pool.MaybeCreateCorePool()).Length);
        }

        [Test]
        public void MaybeCreateCorePool_Should_Yield_A_Connection_If_Any_Fails()
        {
            var mock = new Mock<HostConnectionPool>(Host1, HostDistance.Local, GetConfig());
            var counter = 0;
            //use different addresses for same hosts to differentiate connections: for test only
            //different connections to same hosts should use the same address
            mock.Setup(p => p.CreateConnection()).Returns(() =>
            {
                if (++counter == 2)
                {
                    return TaskHelper.FromException<Connection>(new Exception("Dummy exception"));
                }
                return TaskHelper.ToTask(new Connection(ProtocolVersion, GetIpEndPoint(), GetConfig()));
            });
            var pool = mock.Object;
            var creation = pool.MaybeCreateCorePool();
            creation.Wait();
            Assert.Greater(creation.Result.Length, 0);
            //Wait for all connections
            Thread.Sleep(50);
            //1 failed, 2 successfully opened
            Assert.AreEqual(2, pool.OpenConnections.Count());
        }

        [Test]
        public void MaybeCreateCorePool_Serial_Calls_Should_Yield_First()
        {
            var mock = new Mock<HostConnectionPool>(Host1, HostDistance.Local, GetConfig());
            var lastByte = 1;
            mock.Setup(p => p.CreateConnection()).Returns(() =>
            {
                var c = new Connection(ProtocolVersion, GetIpEndPoint((byte) lastByte++), GetConfig());
                if (lastByte == 2)
                {
                    return TestHelper.DelayedTask(c, 500);   
                }
                return TaskHelper.ToTask(c);
            });
            var pool = mock.Object;
            var creationTasks = new Task<Connection[]>[4];
            creationTasks[0] = pool.MaybeCreateCorePool();
            creationTasks[1] = pool.MaybeCreateCorePool();
            creationTasks[2] = pool.MaybeCreateCorePool();
            creationTasks[3] = pool.MaybeCreateCorePool();
            // ReSharper disable once CoVariantArrayConversion
            Task.WaitAll(creationTasks);
            Assert.AreEqual(1, TaskHelper.WaitToComplete(creationTasks[0]).Length);
            for (var i = 1; i < creationTasks.Length; i++)
            {
                Assert.AreEqual(1, TaskHelper.WaitToComplete(creationTasks[i]).Length);   
            }
        }

        [Test]
        public void MaybeCreateCorePool_Parallel_Calls_Should_Yield_First()
        {
            var mock = new Mock<HostConnectionPool>(Host1, HostDistance.Local, GetConfig());
            var lastByte = 1;
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(new Connection(ProtocolVersion, GetIpEndPoint((byte)lastByte++), GetConfig()), 1000));
            var pool = mock.Object;
            var creationTasks = new Task<Connection[]>[10];
            var counter = -1;
            var initialCreate = pool.MaybeCreateCorePool();
            TestHelper.ParallelInvoke(() =>
            {
                creationTasks[Interlocked.Increment(ref counter)] = pool.MaybeCreateCorePool();
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
        public void MaybeCreateCorePool_Fail_To_Open_All_Connections_Should_Fault_Task()
        {
            var mock = new Mock<HostConnectionPool>(Host1, HostDistance.Local, GetConfig());
            var testException = new Exception("Dummy exception");
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask<Connection>(() =>
            {
                throw testException;
            }));
            var pool = mock.Object;
            var task = pool.MaybeCreateCorePool();
            var ex = Assert.Throws<Exception>(() => TaskHelper.WaitToComplete(task));
            Assert.AreEqual(testException, ex);
        }

        [Test]
        public void MaybeCreateCorePool_Fail_To_Open_Single_Connection_Should_Yield_Valid()
        {
            var mock = new Mock<HostConnectionPool>(Host1, HostDistance.Local, GetConfig());
            var testException = new Exception("Dummy exception");
            var counter = 0;
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(() =>
            {
                if (counter++ == 0)
                {
                    throw testException;
                }
                return new Connection(ProtocolVersion, GetIpEndPoint((byte)counter++), GetConfig());
            }));
            var pool = mock.Object;
            var connections = TaskHelper.WaitToComplete(pool.MaybeCreateCorePool());
            //1 or 2 valid connections still
            Assert.LessOrEqual(connections.Length, 2);
            //next attempts creates all
            Thread.Sleep(100);
            connections = TaskHelper.WaitToComplete(pool.MaybeCreateCorePool());
            //The one recently created
            Assert.AreEqual(1, connections.Length);
            Thread.Sleep(500);
            connections = TaskHelper.WaitToComplete(pool.MaybeCreateCorePool());
            //Return all the pool
            Assert.AreEqual(3, connections.Length);
        }

        [Test]
        public void MaybeCreateCorePool_Fail_To_Grow_To_CoreConnections_Yields_Existing()
        {
            var mock = new Mock<HostConnectionPool>(Host1, HostDistance.Local, GetConfig());
            var testException = new Exception("Dummy exception");
            var counter = 0;
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(() =>
            {
                var i = counter++;
                if (i == 0 || i > 2)
                {
                    throw testException;
                }
                return new Connection(ProtocolVersion, GetIpEndPoint((byte)counter), GetConfig());
            }, 50 * counter));
            var pool = mock.Object;
            Assert.DoesNotThrow(() => TaskHelper.WaitToComplete(pool.MaybeCreateCorePool()));
            //next attempts creates to create all, still fails
            Thread.Sleep(100);
            var connections = TaskHelper.WaitToComplete(pool.MaybeCreateCorePool());
            //2 valid still
            Assert.AreEqual(2, connections.Length);
        }

        [Test]
        public void MaybeSpawnNewConnection_Should_Not_Block_When_Creating()
        {
            var mock = new Mock<HostConnectionPool>(Host1, HostDistance.Local, GetConfig());
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(new Connection(ProtocolVersion, GetIpEndPoint(), GetConfig()), 200));
            var pool = mock.Object;
            var connectionSpawned1 = pool.MaybeSpawnNewConnection(129);
            var connectionSpawned2 = pool.MaybeSpawnNewConnection(129);
            //No connections added yet
            Assert.AreEqual(0, pool.OpenConnections.Count());
            Assert.True(connectionSpawned1);
            Assert.False(connectionSpawned2);
            Thread.Sleep(500);
            Assert.AreEqual(1, pool.OpenConnections.Count());
        }

        [Test]
        public void AttemptReconnection_Should_Reconnect_In_The_Background()
        {
            var mock = new Mock<HostConnectionPool>(Host1, HostDistance.Local, GetConfig());
            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(new Connection(ProtocolVersion, GetIpEndPoint(), GetConfig()), 20));
            var pool = mock.Object;
            Assert.AreEqual(0, pool.OpenConnections.Count());
            pool.AttemptReconnection();
            Thread.Sleep(100);
            Assert.AreEqual(1, pool.OpenConnections.Count());
        }

        [Test]
        public void AttemptReconnection_Should_Not_Create_A_New_Connection_If_There_Is_An_Open_Connection()
        {
            var mock = new Mock<HostConnectionPool>(Host1, HostDistance.Local, GetConfig(2));
            mock.Setup(p => p.CreateConnection()).Returns(() => TaskHelper.ToTask(new Connection(ProtocolVersion, GetIpEndPoint(), GetConfig())));
            var pool = mock.Object;
            //Create 1 connection
            TaskHelper.WaitToComplete(pool.MaybeCreateCorePool());
            Thread.SpinWait(1);
            Assert.AreEqual(2, pool.OpenConnections.Count());
            pool.AttemptReconnection();
            Thread.SpinWait(5);
            //Pool remains the same
            Assert.AreEqual(2, pool.OpenConnections.Count());
        }
    }
}
