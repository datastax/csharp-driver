using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class ClientTimeoutTests : TestGlobals
    {
        public ClientTimeoutTests()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            Diagnostics.CassandraStackTraceIncluded = true;    
        }

        [Test]
        public void Should_Move_To_Next_Host_For_Simple_Queries()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(2, 1, true, false);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(3000);
            var builder = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute("SELECT key FROM system.local"), 10);
                testCluster.PauseNode(2);
                TestHelper.Invoke(() =>
                {
                    var rs = session.Execute("SELECT key FROM system.local");
                    Assert.AreEqual(1, TestHelper.GetLastAddressByte(rs.Info.QueriedHost));
                }, 10);
                testCluster.ResumeNode(2);
            }
        }

        [Test]
        public void Should_Move_To_Next_Host_For_Bound_Statements()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(2, 1, true, false);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(3000);
            var builder = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                var ps = session.Prepare("SELECT key FROM system.local");
                //warmup
                TestHelper.Invoke(() => session.Execute("SELECT key FROM system.local"), 10);
                testCluster.PauseNode(2);
                TestHelper.Invoke(() =>
                {
                    var rs = session.Execute(ps.Bind());
                    Assert.AreEqual(1, TestHelper.GetLastAddressByte(rs.Info.QueriedHost));
                }, 10);
                testCluster.ResumeNode(2);
            }
        }

        [Test]
        public void Should_Move_To_Next_Host_For_Prepare_Requests()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(2, 1, true, false);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(3000);
            var builder = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute("SELECT key FROM system.local"), 10);
                testCluster.PauseNode(2);
                TestHelper.Invoke(() =>
                {
                    session.Prepare("SELECT key FROM system.local");
                }, 10);
                testCluster.ResumeNode(2);
            }
        }

        [Test]
        public void Should_Throw_OperationTimedOutException_When_Retry_Is_False()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(2, 1, true, false);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(3000);
            var queryOptions = new QueryOptions().SetRetryOnTimeout(false);
            var builder = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions)
                .WithQueryOptions(queryOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute("SELECT key FROM system.local"), 10);
                testCluster.PauseNode(2);
                var coordinators = new HashSet<byte>();
                var exceptions = new List<OperationTimedOutException>();
                TestHelper.Invoke(() =>
                {
                    try
                    {
                        var rs = session.Execute("SELECT key FROM system.local");
                        coordinators.Add(TestHelper.GetLastAddressByte(rs.Info.QueriedHost));
                    }
                    catch (OperationTimedOutException ex)
                    {
                        exceptions.Add(ex);
                    }
                }, 10);
                testCluster.ResumeNode(2);
                Assert.AreEqual(1, coordinators.Count);
                Assert.AreEqual(5, exceptions.Count);
                Assert.AreEqual(1, coordinators.First());
            }
        }

        [Test]
        public void Should_Wait_When_ReadTimeout_Is_Zero()
        {
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(0);
            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                const string cql = "SELECT key FROM system.local";
                simulacronCluster.Prime(new
                {
                    when = new { query = cql },
                    then = new
                    {
                        result = "success",
                        delay_in_ms = 30000,
                        rows = new [] { new { key = "123" } },
                        column_types = new { key = "ascii" },
                        ignore_on_prepare = false
                    }
                });
                
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
            const int generalReadTimeout = 1500;
            const int statementReadTimeout = 12000;
            var testCluster = TestClusterManager.CreateNew();
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(generalReadTimeout);
            var queryOptions = new QueryOptions().SetRetryOnTimeout(false);
            var builder = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions)
                .WithPoolingOptions(PoolingOptions.Create().SetHeartBeatInterval(0))
                .WithQueryTimeout(Timeout.Infinite)
                .WithQueryOptions(queryOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute("SELECT key FROM system.local"), 10);
                testCluster.PauseNode(1);
                var stopWatch = new Stopwatch();
                stopWatch.Start();
                Assert.Throws<OperationTimedOutException>(() => session.Execute("SELECT key FROM system.local"));
                stopWatch.Stop();
                //precision of the timer is not guaranteed
                Assert.Greater(stopWatch.ElapsedMilliseconds, generalReadTimeout - 1000);
                Assert.Less(stopWatch.ElapsedMilliseconds, generalReadTimeout + 1000);

                //Try with an specified timeout at Statement level
                var stmt = new SimpleStatement("SELECT key FROM system.local")
                    .SetReadTimeoutMillis(statementReadTimeout);
                stopWatch.Restart();
                Assert.Throws<OperationTimedOutException>(() => session.Execute(stmt));
                stopWatch.Stop();
                //precision of the timer is not guaranteed
                Assert.Greater(stopWatch.ElapsedMilliseconds, statementReadTimeout - 3000);
                Assert.Less(stopWatch.ElapsedMilliseconds, statementReadTimeout + 3000);
                testCluster.ResumeNode(1);
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
            var testCluster = TestClusterManager.GetNonShareableTestCluster(2, 1, true, false);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(3000);
            var builder = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint)
                .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                //warmup
                TestHelper.Invoke(() => session.Execute("SELECT key FROM system.local"), 10);
                testCluster.PauseNode(1);
                testCluster.PauseNode(2);
                var ex = Assert.Throws<NoHostAvailableException>(() => session.Execute("SELECT key FROM system.local"));
                Assert.AreEqual(2, ex.Errors.Count);
                foreach (var innerException in ex.Errors.Values)
                {
                    Assert.IsInstanceOf<OperationTimedOutException>(innerException);
                }
                testCluster.ResumeNode(1);
                testCluster.ResumeNode(2);
            }
        }

        [Test]
        public void Should_Throw_NoHostAvailable_When_Startup_Times_out()
        {
            var testCluster = TestClusterManager.CreateNew();
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(1000).SetConnectTimeoutMillis(1000);
            var builder = Cluster.Builder()
                                 .AddContactPoint(testCluster.InitialContactPoint)
                                 .WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                testCluster.PauseNode(1);
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                Assert.AreEqual(1, ex.Errors.Count);
                foreach (var innerException in ex.Errors.Values)
                {
                    Assert.IsInstanceOf<OperationTimedOutException>(innerException);
                }
                testCluster.ResumeNode(1);
            }
        }

        [Test]
        public void Should_Not_Leak_Connections_Test()
        {
            var testCluster = TestClusterManager.CreateNew();
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(1).SetConnectTimeoutMillis(1);
            var builder = Cluster.Builder()
                                 .AddContactPoint(testCluster.InitialContactPoint)
                                 .WithSocketOptions(socketOptions);

            testCluster.PauseNode(1);
            const int length = 1000;
            using (var cluster = builder.Build())
            {
                decimal initialLength = GC.GetTotalMemory(true);
                for (var i = 0; i < length; i++)
                {
                    var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                    Assert.AreEqual(1, ex.Errors.Count);
                }
                GC.Collect();
                Thread.Sleep(1000);
                testCluster.ResumeNode(1);
                Assert.Less(GC.GetTotalMemory(true) / initialLength, 1.3M,
                    "Should not exceed a 30% (1.3) more than was previously allocated");
            }
        }
    }
}
