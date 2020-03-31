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

        public ClientTimeoutTests()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            Diagnostics.CassandraStackTraceIncluded = true;
        }

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
            var builder = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute("SELECT key FROM system.local"), 10);
                var nodes = _testCluster.GetNodes().ToList();
                var node = nodes[0];
                node.PrimeFluent(b => b
                    .WhenQuery("SELECT key FROM system.local")
                    .ThenRowsSuccess(new [] { ("key", DataType.Ascii) }, rows => rows.WithRow("123"))
                    .WithDelayInMs(2000));
                TestHelper.Invoke(() =>
                {
                    var rs = session.Execute("SELECT key FROM system.local");
                    Assert.AreEqual(nodes[1].ContactPoint, rs.Info.QueriedHost.ToString());
                }, 10);
            }
        }

        [Test]
        public void Should_Move_To_Next_Host_For_Bound_Statements()
        {
            _testCluster = SimulacronCluster.CreateNew(2);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(500);
            var builder = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                var ps = session.Prepare("SELECT key FROM system.local");
                //warmup
                TestHelper.Invoke(() => session.Execute("SELECT key FROM system.local"), 10);
                var nodes = _testCluster.GetNodes().ToList();
                var node = nodes[0];
                node.PrimeFluent(
                    b => b.WhenQuery("SELECT key FROM system.local")
                          .ThenRowsSuccess(new [] { ("key", DataType.Ascii) }, rows => rows.WithRow("123"))
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
            var builder = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute("SELECT key FROM system.local"), 10);
                var node = _testCluster.GetNodes().Skip(1).First();
                node.Stop().GetAwaiter().GetResult();
                TestHelper.Invoke(() =>
                {
                    session.Prepare("SELECT key FROM system.local");
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
            var builder = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions)
                .WithQueryOptions(queryOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute("SELECT key FROM system.local"), 10);
                var nodes = _testCluster.GetNodes().ToList();
                var node = nodes[1];
                node.PrimeFluent(
                    b => b.WhenQuery("SELECT key FROM system.local")
                          .ThenRowsSuccess(new [] { ("key", DataType.Ascii) }, rows => rows.WithRow("123"))
                          .WithDelayInMs(2000));
                var coordinators = new HashSet<string>();
                var exceptions = new List<OperationTimedOutException>();
                TestHelper.Invoke(() =>
                {
                    try
                    {
                        var rs = session.Execute("SELECT key FROM system.local");
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
                const string cql = "SELECT key FROM system.local";
                simulacronCluster.PrimeFluent(
                    b => b.WhenQuery(cql)
                          .ThenRowsSuccess(new [] { ("key", DataType.Ascii) }, rows => rows.WithRow("123"))
                          .WithDelayInMs(30000));

                using (var cluster = Cluster.Builder().AddContactPoint(simulacronCluster.InitialContactPoint).WithSocketOptions(socketOptions).Build())
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
            const int statementReadTimeout = 6000;
            _testCluster = SimulacronCluster.CreateNew(1);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(generalReadTimeout);
            var queryOptions = new QueryOptions().SetRetryOnTimeout(false);
            var builder = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions)
                .WithPoolingOptions(PoolingOptions.Create().SetHeartBeatInterval(0))
                .WithQueryTimeout(Timeout.Infinite)
                .WithQueryOptions(queryOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute("SELECT key FROM system.local"), 10);
                var nodes = _testCluster.GetNodes().ToList();
                var node = nodes[0];
                node.PrimeFluent(
                    b => b.WhenQuery("SELECT key FROM system.local")
                          .ThenRowsSuccess(new [] { ("key", DataType.Ascii) }, rows => rows.WithRow("123"))
                          .WithDelayInMs(30000));
                var stopWatch = new Stopwatch();
                stopWatch.Start();
                Assert.Throws<OperationTimedOutException>(() => session.Execute("SELECT key FROM system.local"));
                stopWatch.Stop();
                //precision of the timer is not guaranteed
                Assert.Greater(stopWatch.ElapsedMilliseconds, generalReadTimeout - 2000);
                Assert.Less(stopWatch.ElapsedMilliseconds, generalReadTimeout + 2000);

                //Try with an specified timeout at Statement level
                var stmt = new SimpleStatement("SELECT key FROM system.local")
                    .SetReadTimeoutMillis(statementReadTimeout);
                stopWatch.Restart();
                Assert.Throws<OperationTimedOutException>(() => session.Execute(stmt));
                stopWatch.Stop();
                //precision of the timer is not guaranteed
                Assert.Greater(stopWatch.ElapsedMilliseconds, statementReadTimeout - 2500);
                Assert.Less(stopWatch.ElapsedMilliseconds, statementReadTimeout + 2500);
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
            var builder = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute("SELECT key FROM system.local"), 10);
                _testCluster.PrimeFluent(
                    b => b.WhenQuery("SELECT key FROM system.local")
                          .ThenRowsSuccess(new [] { ("key", DataType.Ascii) }, rows => rows.WithRow("123"))
                          .WithDelayInMs(10000));
                var ex = Assert.Throws<NoHostAvailableException>(() => session.Execute("SELECT key FROM system.local"));
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
            var builder = Cluster.Builder()
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
            _testCluster = SimulacronCluster.CreateNew(1);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(1).SetConnectTimeoutMillis(1);

            var node = _testCluster.GetNodes().First();
            node.DisableConnectionListener(0, "reject_startup").GetAwaiter().GetResult();
            var clusters = Enumerable.Range(0, 100).Select(
                b => Cluster.Builder()
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
    }
}