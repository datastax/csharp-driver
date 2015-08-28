using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
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
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, true, false);
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(0);
            var builder = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).WithSocketOptions(socketOptions);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                var query = new SimpleStatement("SELECT key FROM system.local");
                //warmup
                TestHelper.Invoke(() => session.Execute(query), 5);
                var task = session.ExecuteAsync(query);
                Thread.Sleep(2000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                //Pause execute and nothing should happen until resume
                testCluster.PauseNode(1);
                task = session.ExecuteAsync(query);
                Thread.Sleep(15000);
                Assert.AreEqual(TaskStatus.WaitingForActivation, task.Status);
                testCluster.ResumeNode(1);
                Thread.Sleep(2000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
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
                Assert.Throws<NoHostAvailableException>(() => session.Execute("SELECT key FROM system.local"));
                testCluster.ResumeNode(1);
                testCluster.ResumeNode(2);
            }
        }
    }
}
