using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.ExecutionProfiles;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

using Moq;

using NUnit.Framework;

// ReSharper disable AccessToModifiedClosure

namespace Cassandra.Tests
{
    [TestFixture]
    public class HostConnectionPoolTests
    {
        private static readonly IPEndPoint Address = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1000);
        private static readonly Host Host1 = TestHelper.CreateHost("127.0.0.1");
        private static readonly HashedWheelTimer Timer = new HashedWheelTimer();

        [OneTimeSetUp]
        public void OnTimeSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            Timer.Dispose();
        }

        private static IPEndPoint GetIpEndPoint(byte lastByte = 1)
        {
            return new IPEndPoint(IPAddress.Parse("127.0.0." + lastByte), 9042);
        }

        private static IConnection CreateConnection(byte lastIpByte = 1, Configuration config = null)
        {
            if (config == null)
            {
                config = GetConfig();
            }
            return new Connection(new Serializer(ProtocolVersion.MaxSupported), GetIpEndPoint(lastIpByte), config);
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
            return new Mock<HostConnectionPool>(host, config, new Serializer(ProtocolVersion.MaxSupported));
        }

        private static Configuration GetConfig(int coreConnections = 3, int maxConnections = 8, IReconnectionPolicy rp = null)
        {
            var pooling = new PoolingOptions()
                .SetCoreConnectionsPerHost(HostDistance.Local, coreConnections)
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 1500)
                .SetMaxConnectionsPerHost(HostDistance.Local, maxConnections);
            var policies = new Policies(null, rp, null, null, null);
            var config = new Configuration(
                policies,
                new ProtocolOptions(),
                pooling,
                new SocketOptions(),
                new ClientOptions(),
                NoneAuthProvider.Instance,
                null,
                new QueryOptions(),
                new DefaultAddressTranslator(),
                Mock.Of<IStartupOptionsFactory>(),
                new SessionFactoryBuilder(),
                new Dictionary<string, IExecutionProfile>(),
                new RequestOptionsMapper(),
                null);
            return config;
        }

        private static IConnection GetConnectionMock(int inflight, int timedOutOperations = 0)
        {
            var connectionMock = new Mock<Connection>(
                MockBehavior.Loose, new Serializer(ProtocolVersion.MaxSupported), Address, new Configuration());
            connectionMock.Setup(c => c.InFlight).Returns(inflight);
            connectionMock.Setup(c => c.TimedOutOperations).Returns(timedOutOperations);
            return connectionMock.Object;
        }

        public HostConnectionPoolTests()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
        }

        [Test]
        public void MaybeCreateFirstConnection_Should_Yield_The_First_Connection_Opened()
        {
            var mock = GetPoolMock();
            var lastByte = 1;
            //use different addresses for same hosts to differentiate connections: for test only
            //different connections to same hosts should use the same address
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() => TestHelper.DelayedTask(CreateConnection((byte)lastByte++), 200 - lastByte * 50));
            var pool = mock.Object;
            var creation = pool.EnsureCreate();
            creation.Wait();
            Assert.AreEqual(1, creation.Result.Length);
            //yield the third connection first
            CollectionAssert.AreEqual(new[] { GetIpEndPoint() }, creation.Result.Select(c => c.Address));
        }

        [Test]
        public async Task EnsureCreate_Should_Yield_A_Connection_If_Any_Fails()
        {
            var mock = GetPoolMock();
            var counter = 0;
            //use different addresses for same hosts to differentiate connections: for test only
            //different connections to same hosts should use the same address
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                if (++counter == 2)
                {
                    return TaskHelper.FromException<IConnection>(new Exception("Dummy exception"));
                }
                return TaskHelper.ToTask(CreateConnection());
            });
            var pool = mock.Object;
            var connections = await pool.EnsureCreate().ConfigureAwait(false);
            Assert.AreEqual(connections.Length, 1);
        }

        [Test]
        public void EnsureCreate_Serial_Calls_Should_Yield_First()
        {
            var mock = GetPoolMock();
            var lastByte = 1;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                var c = CreateConnection((byte)lastByte++);
                if (lastByte == 2)
                {
                    return TestHelper.DelayedTask(c, 500);
                }
                return TaskHelper.ToTask(c);
            });
            var pool = mock.Object;
            var creationTasks = new Task<IConnection[]>[4];
            creationTasks[0] = pool.EnsureCreate();
            creationTasks[1] = pool.EnsureCreate();
            creationTasks[2] = pool.EnsureCreate();
            creationTasks[3] = pool.EnsureCreate();
            // ReSharper disable once CoVariantArrayConversion
            Task.WaitAll(creationTasks);
            Assert.AreEqual(1, TaskHelper.WaitToComplete(creationTasks[0]).Length);
            for (var i = 1; i < creationTasks.Length; i++)
            {
                Assert.AreEqual(1, TaskHelper.WaitToComplete(creationTasks[i]).Length);
            }
        }

        [Test]
        public void EnsureCreate_Parallel_Calls_Should_Yield_First()
        {
            var mock = GetPoolMock();
            var lastByte = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() => TestHelper.DelayedTask(CreateConnection((byte)++lastByte), 100 + (lastByte > 1 ? 10000 : 0)));
            var pool = mock.Object;
            var creationTasks = new Task<IConnection[]>[10];
            var counter = -1;
            var initialCreate = pool.EnsureCreate();
            TestHelper.ParallelInvoke(() =>
            {
                creationTasks[Interlocked.Increment(ref counter)] = pool.EnsureCreate();
            }, 10);
            // ReSharper disable once CoVariantArrayConversion
            Task.WaitAll(creationTasks);
            Assert.AreEqual(1, TaskHelper.WaitToComplete(initialCreate).Length);

            foreach (var t in creationTasks)
            {
                Assert.AreEqual(1, TaskHelper.WaitToComplete(t).Length);
            }
            Assert.AreEqual(1, lastByte);
        }

        [Test]
        public void EnsureCreate_Parallel_Calls_Failing_Should_Only_Attempt_Creation_Once()
        {
            // Use a reconnection policy that never attempts
            var mock = GetPoolMock(null, GetConfig(3, 3, new ConstantReconnectionPolicy(int.MaxValue)));
            var openConnectionAttempts = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref openConnectionAttempts);
                return TaskHelper.FromException<IConnection>(new Exception("Test Exception"));
            });
            var pool = mock.Object;
            const int times = 5;
            var creationTasks = new Task[times];
            var counter = -1;
            var initialCreate = pool.EnsureCreate();
            TestHelper.ParallelInvoke(() =>
            {
                creationTasks[Interlocked.Increment(ref counter)] = pool.EnsureCreate();
            }, times);
            Assert.Throws<AggregateException>(() => initialCreate.Wait());

            var aggregateException = Assert.Throws<AggregateException>(() => Task.WaitAll(creationTasks));
            Assert.AreEqual(times, aggregateException.InnerExceptions.Count);
            Assert.AreEqual(1, Volatile.Read(ref openConnectionAttempts));

            // Serially, attempt calls to create
            Interlocked.Exchange(ref counter, -1);
            TestHelper.ParallelInvoke(() =>
            {
                creationTasks[Interlocked.Increment(ref counter)] = pool.EnsureCreate();
            }, times);

            aggregateException = Assert.Throws<AggregateException>(() => Task.WaitAll(creationTasks));
            Assert.AreEqual(times, aggregateException.InnerExceptions.Count);
            Assert.AreEqual(1, Volatile.Read(ref openConnectionAttempts));
        }

        [Test]
        public void EnsureCreate_Fail_To_Open_All_Connections_Should_Fault_Task()
        {
            var mock = GetPoolMock();
            var testException = new Exception("Dummy exception");
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() => TestHelper.DelayedTask<IConnection>(() =>
            {
                throw testException;
            }));
            var pool = mock.Object;
            var task = pool.EnsureCreate();
            var ex = Assert.Throws<Exception>(() => TaskHelper.WaitToComplete(task));
            Assert.AreEqual(testException, ex);
        }

        [Test]
        public async Task OnHostUp_Recreates_Pool_In_The_Background()
        {
            var mock = GetPoolMock(null, GetConfig(2, 2));
            var creationCounter = 0;
            var isCreating = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref creationCounter);
                Interlocked.Exchange(ref isCreating, 1);
                return TestHelper.DelayedTask(CreateConnection(), 30, () => Interlocked.Exchange(ref isCreating, 0));
            });
            var pool = mock.Object;
            pool.SetDistance(HostDistance.Local);
            Assert.AreEqual(0, pool.OpenConnections);
            Assert.AreEqual(0, Volatile.Read(ref creationCounter));
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
            pool.OnHostUp(null);
            await TestHelper.WaitUntilAsync(() => pool.OpenConnections == 2).ConfigureAwait(false);
            Assert.AreEqual(2, pool.OpenConnections);
            Assert.AreEqual(2, Volatile.Read(ref creationCounter));
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
        }

        [Test]
        public void OnHostUp_Does_Not_Recreates_Pool_For_Ignored_Hosts()
        {
            var mock = GetPoolMock(null, GetConfig(2, 2));
            var creationCounter = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref creationCounter);
                return TaskHelper.ToTask(CreateConnection());
            });
            var pool = mock.Object;
            pool.SetDistance(HostDistance.Ignored);
            Assert.AreEqual(0, pool.OpenConnections);
            Assert.AreEqual(0, Volatile.Read(ref creationCounter));
            pool.OnHostUp(null);
            Thread.Sleep(200);
            Assert.AreEqual(0, pool.OpenConnections);
            Assert.AreEqual(0, Volatile.Read(ref creationCounter));
        }

        [Test]
        public async Task EnsureCreate_After_Reconnection_Attempt_Waits_Existing()
        {
            var mock = GetPoolMock(null, GetConfig(2, 2));
            var creationCounter = 0;
            var isCreating = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref creationCounter);
                Interlocked.Exchange(ref isCreating, 1);
                return TestHelper.DelayedTask(CreateConnection(), 300, () => Interlocked.Exchange(ref isCreating, 0));
            });
            var pool = mock.Object;
            pool.SetDistance(HostDistance.Local);
            Assert.AreEqual(0, pool.OpenConnections);
            Thread.Sleep(100);
            pool.OnHostUp(null);
            await pool.EnsureCreate().ConfigureAwait(false);
            Assert.AreEqual(1, pool.OpenConnections);
            await TestHelper.WaitUntilAsync(() => pool.OpenConnections == 2, 200, 30).ConfigureAwait(false);
            Assert.AreEqual(2, Volatile.Read(ref creationCounter));
            Assert.AreEqual(2, pool.OpenConnections);
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
        }

        [Test]
        public async Task EnsureCreate_Can_Handle_Multiple_Concurrent_Calls()
        {
            var mock = GetPoolMock(null, GetConfig(3, 3));
            var creationCounter = 0;
            var isCreating = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref creationCounter);
                Interlocked.Exchange(ref isCreating, 1);
                return TestHelper.DelayedTask(CreateConnection(), 50, () => Interlocked.Exchange(ref isCreating, 0));
            });
            var pool = mock.Object;
            pool.SetDistance(HostDistance.Local);
            Assert.AreEqual(0, pool.OpenConnections);
            var tasks = new Task[100];
            for (var i = 0; i < 100; i++)
            {
                tasks[i] = Task.Run(async () => await pool.EnsureCreate().ConfigureAwait(false));
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);
            Assert.Greater(pool.OpenConnections, 0);
            Assert.LessOrEqual(pool.OpenConnections, 3);
            await TestHelper.WaitUntilAsync(() => Volatile.Read(ref creationCounter) == 3, 200, 20).ConfigureAwait(false);
            Assert.AreEqual(3, Volatile.Read(ref creationCounter));
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
            Assert.AreEqual(3, pool.OpenConnections);
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
                GetConnectionMock(1)
            };
            var index = 0;
            int inFlight;
            var c = HostConnectionPool.MinInFlight(connections, ref index, 100, out inFlight);
            Assert.AreEqual(index, 1);
            Assert.AreSame(connections[1], c);
            Assert.AreEqual(1, inFlight);
            c = HostConnectionPool.MinInFlight(connections, ref index, 100, out inFlight);
            Assert.AreEqual(index, 2);
            //previous had less in flight
            Assert.AreSame(connections[2], c);
            Assert.AreEqual(1, inFlight);
            c = HostConnectionPool.MinInFlight(connections, ref index, 100, out inFlight);
            Assert.AreEqual(index, 3);
            Assert.AreSame(connections[4], c);
            Assert.AreEqual(1, inFlight);
            c = HostConnectionPool.MinInFlight(connections, ref index, 100, out inFlight);
            Assert.AreEqual(index, 4);
            Assert.AreSame(connections[0], c);
            Assert.AreEqual(0, inFlight);
            c = HostConnectionPool.MinInFlight(connections, ref index, 100, out inFlight);
            Assert.AreEqual(index, 5);
            Assert.AreSame(connections[0], c);
            Assert.AreEqual(0, inFlight);
            index = 9;
            c = HostConnectionPool.MinInFlight(connections, ref index, 100, out inFlight);
            Assert.AreEqual(index, 10);
            Assert.AreSame(connections[0], c);
            Assert.AreEqual(0, inFlight);
        }

        [Test]
        public void MinInFlight_Goes_Through_All_The_Connections_When_Over_Threshold()
        {
            var connections = new[]
            {
                GetConnectionMock(10),
                GetConnectionMock(1),
                GetConnectionMock(201),
                GetConnectionMock(200),
                GetConnectionMock(210)
            };
            var index = 0;
            int inFlight;

            var c = HostConnectionPool.MinInFlight(connections, ref index, 100, out inFlight);
            Assert.AreEqual(index, 1);
            Assert.AreSame(connections[1], c);
            Assert.AreEqual(1, inFlight);

            c = HostConnectionPool.MinInFlight(connections, ref index, 100, out inFlight);
            Assert.AreEqual(index, 2);
            // Should pick the first below the threshold
            Assert.AreSame(connections[0], c);
            Assert.AreEqual(10, inFlight);
        }

        [Test]
        public void ScheduleReconnection_Should_Raise_AllConnectionClosed()
        {
            var mock = GetPoolMock(null, GetConfig(1, 1, new ConstantReconnectionPolicy(100)));
            var openConnectionsAttempts = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref openConnectionsAttempts);
                return TaskHelper.FromException<IConnection>(new Exception("Test Exception"));
            });
            var pool = mock.Object;
            var eventRaised = 0;
            pool.AllConnectionClosed += (_, __) => Interlocked.Increment(ref eventRaised);
            pool.ScheduleReconnection();
            Thread.Sleep(1500);
            Assert.AreEqual(0, pool.OpenConnections);
            Assert.AreEqual(1, Interlocked.CompareExchange(ref eventRaised, 0, 0));
            // Should not retry to reconnect, should relay on external consumer
            Assert.AreEqual(1, Interlocked.CompareExchange(ref openConnectionsAttempts, 0, 0));
        }

        [Test]
        public void ScheduleReconnection_Should_Not_Raise_AllConnectionClosed_When_Host_Is_Down()
        {
            var host = TestHelper.CreateHost("127.0.0.1");
            host.SetDown();
            var mock = GetPoolMock(host, GetConfig(1, 1, new ConstantReconnectionPolicy(100)));
            var openConnectionsAttempts = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref openConnectionsAttempts);
                return TaskHelper.FromException<IConnection>(new Exception("Test Exception"));
            });
            var pool = mock.Object;
            var eventRaised = 0;
            pool.AllConnectionClosed += (_, __) => Interlocked.Increment(ref eventRaised);
            pool.ScheduleReconnection();
            Thread.Sleep(600);
            Assert.AreEqual(0, pool.OpenConnections);
            Assert.AreEqual(0, Volatile.Read(ref eventRaised));
            // Should continue to reconnect
            Assert.Greater(Volatile.Read(ref openConnectionsAttempts), 1);
            pool.Dispose();
        }

        [Test]
        public async Task CheckHealth_Removes_Connection()
        {
            var mock = GetPoolMock();
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() => TaskHelper.ToTask(GetConnectionMock(0, int.MaxValue)));
            var pool = mock.Object;
            pool.SetDistance(HostDistance.Local);
            Assert.AreEqual(0, pool.OpenConnections);
            await pool.EnsureCreate().ConfigureAwait(false);
            // Wait for the pool to be created
            await Task.Delay(100).ConfigureAwait(false);
            Assert.AreEqual(3, pool.OpenConnections);
            var c = await pool.BorrowConnection().ConfigureAwait(false);
            pool.CheckHealth(c);
            Assert.AreEqual(2, pool.OpenConnections);
        }

        [Test, Repeat(10)]
        public async Task Pool_Increasing_Size_And_Closing_Should_Not_Leave_Connections_Open([Range(0, 29)] int delay)
        {
            var mock = GetPoolMock(null, GetConfig(50, 50));
            mock.Setup(p => p.DoCreateAndOpen()).Returns(async () =>
            {
                await Task.Yield();
                var spinWait = new SpinWait();
                spinWait.SpinOnce();
                return await Task.Run(() => CreateConnection()).ConfigureAwait(false);
            });
            var pool = mock.Object;
            Assert.AreEqual(0, pool.OpenConnections);
            pool.SetDistance(HostDistance.Local);
            await pool.EnsureCreate().ConfigureAwait(false);
            Assert.Greater(pool.OpenConnections, 0);
            // Wait for the pool to be gaining size
            await Task.Delay(delay).ConfigureAwait(false);
            if (delay > 20)
            {
                Assert.Greater(pool.OpenConnections, 1);
            }
            await Task.Run(() =>
            {
                pool.Dispose();
            }).ConfigureAwait(false);
            await Task.Delay(100).ConfigureAwait(false);
            Assert.AreEqual(0, pool.OpenConnections);
        }

        [Test]
        public async Task Dispose_Should_Not_Raise_AllConnections_Closed()
        {
            var mock = GetPoolMock(null, GetConfig(4, 4));
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() => TaskHelper.ToTask(CreateConnection()));
            var pool = mock.Object;
            Assert.AreEqual(0, pool.OpenConnections);
            pool.SetDistance(HostDistance.Local);
            var eventRaised = 0;
            pool.AllConnectionClosed += (_, __) => Interlocked.Increment(ref eventRaised);
            await pool.EnsureCreate().ConfigureAwait(false);
            Assert.Greater(pool.OpenConnections, 0);
            pool.Dispose();
            await Task.Delay(20).ConfigureAwait(false);
            Assert.AreEqual(0, Volatile.Read(ref eventRaised));
        }

        [Test]
        public async Task Dispose_Should_Cancel_Reconnection_Attempts()
        {
            var mock = GetPoolMock(null, GetConfig(4, 4, new ConstantReconnectionPolicy(200)));
            var openConnectionAttempts = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref openConnectionAttempts);
                return TaskHelper.ToTask(CreateConnection());
            });
            var pool = mock.Object;
            Assert.AreEqual(0, pool.OpenConnections);
            pool.SetDistance(HostDistance.Local);
            pool.ScheduleReconnection();
            pool.Dispose();
            await Task.Delay(400).ConfigureAwait(false);
            Assert.AreEqual(0, Volatile.Read(ref openConnectionAttempts));
            Assert.AreEqual(0, pool.OpenConnections);
        }

        [Test]
        public void Warmup_Should_Throw_When_The_First_Connection_Can_Not_Be_Opened()
        {
            var mock = GetPoolMock(null, GetConfig(4, 4, new ConstantReconnectionPolicy(200)));
            var openConnectionAttempts = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                var index = Interlocked.Increment(ref openConnectionAttempts);
                if (index == 1)
                {
                    throw new SocketException();
                }
                return TaskHelper.ToTask(CreateConnection());
            });

            var pool = mock.Object;
            pool.SetDistance(HostDistance.Local);
            Assert.ThrowsAsync<SocketException>(async () => await pool.Warmup().ConfigureAwait(false));
            Assert.AreEqual(1, Volatile.Read(ref openConnectionAttempts));
        }

        [Test]
        public void Warmup_Should_Succeed_When_The_Second_Connection_Can_Not_Be_Opened()
        {
            var mock = GetPoolMock(null, GetConfig(4, 4, new ConstantReconnectionPolicy(200)));
            var openConnectionAttempts = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                var index = Interlocked.Increment(ref openConnectionAttempts);
                if (index == 2)
                {
                    throw new SocketException();
                }
                return TaskHelper.ToTask(CreateConnection());
            });

            var pool = mock.Object;
            pool.SetDistance(HostDistance.Local);
            Assert.DoesNotThrowAsync(async () => await pool.Warmup().ConfigureAwait(false));
            Assert.AreEqual(2, Volatile.Read(ref openConnectionAttempts));
        }

        [Test]
        public void Warmup_Should_Succeed_When_All_Connections_Can_Be_Opened()
        {
            var mock = GetPoolMock(null, GetConfig(4, 4, new ConstantReconnectionPolicy(200)));
            var openConnectionAttempts = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref openConnectionAttempts);
                return TaskHelper.ToTask(CreateConnection());
            });

            var pool = mock.Object;
            pool.SetDistance(HostDistance.Local);
            Assert.DoesNotThrowAsync(async () => await pool.Warmup().ConfigureAwait(false));
            Assert.AreEqual(4, Volatile.Read(ref openConnectionAttempts));
        }
    }
}