//
//      Copyright (C) DataStax Inc.
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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class ClientTimeoutTests : TestGlobals
    {
        private SimulacronCluster _testCluster;
        private const string IdleQuery = "SELECT key FROM system.local WHERE key='local'";

        [TearDown]
        public void TearDown()
        {
            _testCluster?.Dispose();
            _testCluster = null;
        }

        [Test]
        public void Should_Move_To_Next_Host_For_Simple_Queries()
        {
            _testCluster = SimulacronCluster.CreateNew(2);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(500);
            var builder = ClusterBuilder().AddContactPoint(_testCluster.InitialContactPoint)
                                      .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute(IdleQuery), 10);
                var nodes = _testCluster.GetNodes().ToList();
                var node = nodes[0];
                node.PrimeFluent(b => b
                    .WhenQuery(IdleQuery)
                    .ThenRowsSuccess(new[] { ("key", DataType.Ascii) }, rows => rows.WithRow("123"))
                    .WithDelayInMs(2000));
                TestHelper.Invoke(() =>
                {
                    var rs = session.Execute(IdleQuery);
                    Assert.AreEqual(nodes[1].ContactPoint, rs.Info.QueriedHost.ToString());
                }, 10);
            }
        }

        [Test]
        public void Should_Move_To_Next_Host_For_Bound_Statements()
        {
            _testCluster = SimulacronCluster.CreateNew(2);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(500);
            var builder = ClusterBuilder().AddContactPoint(_testCluster.InitialContactPoint)
                                          .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                var ps = session.Prepare(IdleQuery);
                //warmup
                TestHelper.Invoke(() => session.Execute(IdleQuery), 10);
                var nodes = _testCluster.GetNodes().ToList();
                var node = nodes[0];
                node.PrimeFluent(
                    b => b.WhenQuery(IdleQuery)
                          .ThenRowsSuccess(new[] { ("key", DataType.Ascii) }, rows => rows.WithRow("123"))
                          .WithDelayInMs(2000));
                TestHelper.Invoke(() =>
                {
                    var rs = session.Execute(ps.Bind());
                    Assert.AreEqual(nodes[1].ContactPoint, rs.Info.QueriedHost.ToString());
                }, 10);
            }
        }

        [Test]
        public void Should_Move_To_Next_Host_For_Prepare_Requests()
        {
            _testCluster = SimulacronCluster.CreateNew(2);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(3000);
            var builder = ClusterBuilder().AddContactPoint(_testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute(IdleQuery), 10);
                var node = _testCluster.GetNodes().Skip(1).First();
                node.Stop().GetAwaiter().GetResult();
                TestHelper.Invoke(() =>
                {
                    session.Prepare(IdleQuery);
                }, 10);
                node.Start().GetAwaiter().GetResult();
            }
        }

        [Test]
        public void Should_Throw_OperationTimedOutException_When_Retry_Is_False()
        {
            _testCluster = SimulacronCluster.CreateNew(2);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(500);
            var queryOptions = new QueryOptions().SetRetryOnTimeout(false);
            var builder = ClusterBuilder().AddContactPoint(_testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions)
                .WithQueryOptions(queryOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute(IdleQuery), 10);
                var nodes = _testCluster.GetNodes().ToList();
                var node = nodes[1];
                node.PrimeFluent(
                    b => b.WhenQuery(IdleQuery)
                          .ThenRowsSuccess(new[] { ("key", DataType.Ascii) }, rows => rows.WithRow("123"))
                          .WithDelayInMs(2000));
                var coordinators = new HashSet<string>();
                var exceptions = new List<OperationTimedOutException>();
                TestHelper.Invoke(() =>
                {
                    try
                    {
                        var rs = session.Execute(IdleQuery);
                        coordinators.Add(rs.Info.QueriedHost.ToString());
                    }
                    catch (OperationTimedOutException ex)
                    {
                        exceptions.Add(ex);
                    }
                }, 10);
                Assert.AreEqual(1, coordinators.Count);
                Assert.AreEqual(5, exceptions.Count);
                Assert.AreEqual(nodes[0].ContactPoint, coordinators.First());
            }
        }

        [Test]
        public void Should_Wait_When_ReadTimeout_Is_Zero()
        {
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(0);
            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                const string cql = IdleQuery;
                simulacronCluster.PrimeFluent(
                    b => b.WhenQuery(cql)
                          .ThenRowsSuccess(new[] { ("key", DataType.Ascii) }, rows => rows.WithRow("123"))
                          .WithDelayInMs(30000));

                using (var cluster = ClusterBuilder().AddContactPoint(simulacronCluster.InitialContactPoint).WithSocketOptions(socketOptions).Build())
                {
                    var session = cluster.Connect();
                    var query = new SimpleStatement(cql);
                    var task = session.ExecuteAsync(query);
                    Thread.Sleep(15000);
                    Assert.AreEqual(TaskStatus.WaitingForActivation, task.Status);
                    Thread.Sleep(15000);
                    TestHelper.RetryAssert(
                        () => { Assert.AreEqual(TaskStatus.RanToCompletion, task.Status, task.Exception?.ToString() ?? "no exception"); },
                        100,
                        50);
                }
            }
        }

        /// Tests the priority of statement specific timeout over general timeout
        ///
        /// @jira_ticket CSHARP-415
        /// @expected_result A OperationTimedOutException if timeout expires.
        ///
        /// @test_category connection:timeout
        [Test]
        public void Should_Use_Statement_ReadTimeout()
        {
            const int generalReadTimeout = 1000;
            const int statementReadTimeout = 8000;
            _testCluster = SimulacronCluster.CreateNew(1);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(generalReadTimeout);
            var queryOptions = new QueryOptions().SetRetryOnTimeout(false);
            var builder = ClusterBuilder().AddContactPoint(_testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions)
                .WithPoolingOptions(PoolingOptions.Create().SetHeartBeatInterval(0))
                .WithQueryTimeout(Timeout.Infinite)
                .WithQueryOptions(queryOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute(IdleQuery), 10);
                var nodes = _testCluster.GetNodes().ToList();
                var node = nodes[0];
                node.PrimeFluent(
                    b => b.WhenQuery(IdleQuery)
                          .ThenRowsSuccess(new[] { ("key", DataType.Ascii) }, rows => rows.WithRow("123"))
                          .WithDelayInMs(30000));
                var stopWatch = new Stopwatch();
                stopWatch.Start();
                Assert.Throws<OperationTimedOutException>(() => session.Execute(IdleQuery));
                stopWatch.Stop();
                //precision of the timer is not guaranteed
                Assert.Greater(stopWatch.ElapsedMilliseconds, generalReadTimeout - 500);
                Assert.Less(stopWatch.ElapsedMilliseconds, generalReadTimeout + 3000);

                //Try with an specified timeout at Statement level
                var stmt = new SimpleStatement(IdleQuery)
                    .SetReadTimeoutMillis(statementReadTimeout);
                stopWatch.Restart();
                Assert.Throws<OperationTimedOutException>(() => session.Execute(stmt));
                stopWatch.Stop();
                //precision of the timer is not guaranteed
                Assert.Greater(stopWatch.ElapsedMilliseconds, statementReadTimeout - 2000);
                Assert.Less(stopWatch.ElapsedMilliseconds, statementReadTimeout + 6000);
            }
        }

        /// Tests a NoHostAvailableException is raised when all hosts down with read timeout
        ///
        /// Should_Throw_NoHostAvailableException_When_All_Hosts_Down tests that the driver quickly throws a NoHostAvailableException
        /// when all nodes in the cluster is down, given a set ReadTimeoutMillis of 3 seconds.
        ///
        /// @since 2.7.0
        /// @jira_ticket CSHARP-332
        /// @expected_result A NoHostAvailableException should be raised after 3 seconds.
        ///
        /// @test_category connection:timeout
        [Test]
        public void Should_Throw_NoHostAvailableException_When_All_Hosts_Down()
        {
            _testCluster = SimulacronCluster.CreateNew(2);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(500);
            var builder = ClusterBuilder().AddContactPoint(_testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute(IdleQuery), 10);
                _testCluster.PrimeFluent(
                    b => b.WhenQuery(IdleQuery)
                          .ThenRowsSuccess(new[] { ("key", DataType.Ascii) }, rows => rows.WithRow("123"))
                          .WithDelayInMs(10000));
                var ex = Assert.Throws<NoHostAvailableException>(() => session.Execute(IdleQuery));
                Assert.AreEqual(2, ex.Errors.Count);
                foreach (var innerException in ex.Errors.Values)
                {
                    Assert.IsInstanceOf<OperationTimedOutException>(innerException);
                }
            }
        }

        [Test]
        public void Should_Throw_NoHostAvailable_When_Startup_Times_out()
        {
            _testCluster = SimulacronCluster.CreateNew(1);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(1000).SetConnectTimeoutMillis(1000);
            var builder = ClusterBuilder()
                          .AddContactPoint(_testCluster.InitialContactPoint)
                          .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                _testCluster.GetNodes().First().DisableConnectionListener(0, "reject_startup").GetAwaiter().GetResult();
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.AreEqual(1, ex.Errors.Count);
                foreach (var innerException in ex.Errors.Values)
                {
                    Assert.IsInstanceOf<OperationTimedOutException>(innerException);
                }
            }
        }

        [Test]
        public void Should_Not_Leak_Connections_Test()
        {
            var listeners = new List<TraceListener>();
            foreach (var l in Trace.Listeners)
            {
                listeners.Add((TraceListener)l);
            }

            Trace.Listeners.Clear();
            try
            {
                _testCluster = SimulacronCluster.CreateNew(1);
                var socketOptions = new SocketOptions().SetReadTimeoutMillis(1).SetConnectTimeoutMillis(1);

                var node = _testCluster.GetNodes().First();
                node.DisableConnectionListener(0, "reject_startup").GetAwaiter().GetResult();
                var clusters = Enumerable.Range(0, 100).Select(
                    b => ClusterBuilder()
                         .AddContactPoint(_testCluster.InitialContactPoint)
                         .WithSocketOptions(socketOptions)
                         .Build()).ToList();

                try
                {
                    var tasks = clusters.Select(c => Task.Run(async () =>
                    {
                        try
                        {
                            await c.ConnectAsync().ConfigureAwait(false);
                        }
                        catch (NoHostAvailableException ex)
                        {
                            Assert.AreEqual(1, ex.Errors.Count);
                            return;
                        }

                        Assert.Fail();
                    })).ToArray();

                    Task.WaitAll(tasks);

                    foreach (var t in tasks)
                    {
                        t.Dispose();
                    }

                    tasks = null;

                    GC.Collect();
                    Thread.Sleep(1000);

                    decimal initialMemory = GC.GetTotalMemory(true);

                    const int length = 100;

                    tasks = clusters.Select(c => Task.Run(async () =>
                    {
                        for (var i = 0; i < length; i++)
                        {
                            try
                            {
                                await c.ConnectAsync().ConfigureAwait(false);
                            }
                            catch (NoHostAvailableException ex)
                            {
                                Assert.AreEqual(1, ex.Errors.Count);
                                continue;
                            }

                            Assert.Fail();
                        }
                    })).ToArray();

                    Task.WaitAll(tasks);

                    foreach (var t in tasks)
                    {
                        t.Dispose();
                    }

                    tasks = null;

                    GC.Collect();
                    Thread.Sleep(1000);
                    Assert.Less(GC.GetTotalMemory(true) / initialMemory, 1.5M,
                        "Should not exceed a 50% (1.5) more than was previously allocated");

                }
                finally
                {
                    foreach (var c in clusters)
                    {
                        c.Dispose();
                    }
                }
            }
            finally
            {
                foreach (var l in listeners)
                {
                    Trace.Listeners.Add(l);
                }
            }
        }

        /// <summary>
        /// CSHARP-958
        /// </summary>
        [Test]
        public async Task Should_Not_Leak_Connections_With_InvalidKeyspace_Test()
        {
            var listeners = new List<TraceListener>();
            foreach (var l in Trace.Listeners)
            {
                listeners.Add((TraceListener)l);
            }

            Trace.Listeners.Clear();
            try
            {
                var numberOfDefaultKsClusters = 10;
                _testCluster = await SimulacronCluster.CreateNewAsync(1).ConfigureAwait(false);
                var defaultKsClusters = Enumerable.Range(0, numberOfDefaultKsClusters).Select(
                    b =>
                    {
                        if (b % 2 == 0)
                        {
                            _testCluster.PrimeFluent(
                                c => c.WhenQuery($"USE \"keyspace_{b}\"")
                                      .ThenServerError(ServerError.Invalid, "invalid keyspace"));
                        }
                        return ClusterBuilder()
                               .AddContactPoint(_testCluster.InitialContactPoint)
                               .WithDefaultKeyspace($"keyspace_{b}")
                               .Build();
                    }).ToList();
                var clustersWithoutDefaultKs = Enumerable.Range(numberOfDefaultKsClusters, 10).Select(
                    b =>
                    {
                        if (b % 2 == 0)
                        {
                            _testCluster.PrimeFluent(
                                c => c.WhenQuery($"USE \"keyspace_{b}\"")
                                      .ThenServerError(ServerError.Invalid, "invalid keyspace"));
                        }
                        return ClusterBuilder()
                               .AddContactPoint(_testCluster.InitialContactPoint)
                               .Build();
                    }).ToList();

                try
                {

                    Func<int, Task> connectClustersFunc = async iterations =>
                    {
                        var tasks = defaultKsClusters.Select((c, i) => Task.Run(async () =>
                        {
                            for (var j = 0; j < iterations; j++)
                            {
                                Exception ex = null;
                                try
                                {
                                    using (await c.ConnectAsync().ConfigureAwait(false))
                                    {
                                    }
                                }
                                catch (Exception ex1)
                                {
                                    ex = ex1;
                                }

                                if (i % 2 == 0)
                                {
                                    Assert.IsNotNull(ex);
                                }
                                else
                                {
                                    Assert.IsNull(ex);
                                }
                            }
                        })).Concat(clustersWithoutDefaultKs.Select((c, i) => Task.Run(async () =>
                        {
                            for (var j = 0; j < iterations; j++)
                            {
                                Exception ex = null;
                                try
                                {
                                    using (await c.ConnectAsync($"keyspace_{i + numberOfDefaultKsClusters}").ConfigureAwait(false))
                                    {
                                    }
                                }
                                catch (Exception ex1)
                                {
                                    ex = ex1;
                                }

                                if (i % 2 == 0)
                                {
                                    Assert.IsNotNull(ex);
                                }
                                else
                                {
                                    Assert.IsNull(ex);
                                }
                            }
                        }))).ToArray();

                        await Task.WhenAll(tasks).ConfigureAwait(false);

                        foreach (var t in tasks)
                        {
                            t.Dispose();
                        }

                        tasks = null;
                    };

                    await connectClustersFunc(1).ConfigureAwait(false);

                    GC.Collect();
                    await Task.Delay(1000).ConfigureAwait(false);

                    decimal initialMemory = GC.GetTotalMemory(true);

                    await connectClustersFunc(100).ConfigureAwait(false);

                    GC.Collect();
                    await Task.Delay(1000).ConfigureAwait(false);

                    var connectedPorts = await _testCluster.GetConnectedPortsAsync().ConfigureAwait(false);
                    Assert.AreEqual(20, connectedPorts.Count); // control connections

                    Assert.Less(GC.GetTotalMemory(true) / initialMemory, 1.75M,
                        "Should not exceed 75% (1.75) more than was previously allocated");
                }
                finally
                {
                    foreach (var c in defaultKsClusters.Concat(clustersWithoutDefaultKs))
                    {
                        c.Dispose();
                    }
                }
            }
            finally
            {
                foreach (var l in listeners)
                {
                    Trace.Listeners.Add(l);
                }
            }
        }
    }
}