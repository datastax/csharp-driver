using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tasks;
using Cassandra.Tests;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long"), Ignore("tests that are not marked with 'short' need to be refactored/deleted")]
    public class SpeculativeExecutionLongTests : TestGlobals
    {
        private const string QueryLocal = "SELECT key FROM system.local";
        private readonly List<ICluster> _clusters = new List<ICluster>();
        private IPAddress _addressNode1;
        private ITestCluster _testCluster;

        private ISession GetSession(
            ISpeculativeExecutionPolicy speculativeExecutionPolicy = null, bool warmup = true,
            ILoadBalancingPolicy lbp = null, PoolingOptions pooling = null)
        {
            if (_testCluster == null)
            {
                throw new Exception("Test cluster not initialized");
            }
            var builder = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .WithSpeculativeExecutionPolicy(speculativeExecutionPolicy)
                .WithLoadBalancingPolicy(lbp ?? Cassandra.Policies.DefaultLoadBalancingPolicy)
                .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
                .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(0));
            if (pooling != null)
            {
                builder.WithPoolingOptions(pooling);
            }
            var cluster = builder.Build();
            _clusters.Add(cluster);
            var session = cluster.Connect();
            if (warmup)
            {
                TestHelper.ParallelInvoke(() => session.Execute(QueryLocal), 10);
            }
            _addressNode1 = IPAddress.Parse(_testCluster.ClusterIpPrefix + "1");
            return session;
        }

        [TearDown]
        public void TestTearDown()
        {
            _testCluster.Remove();
            _testCluster = null;
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            foreach (var c in _clusters)
            {
                c.Dispose();
            }
        }

        [Test, TestTimeout(120000)]
        public void SpeculativeExecution_Pause_Using_All_Stream_Ids()
        {
            var maxProtocolVersion = Cluster.MaxProtocolVersion;
            _testCluster = TestClusterManager.GetNonShareableTestCluster(2, 1, true, false);
            Cluster.MaxProtocolVersion = 2;
            try
            {
                var pooling = PoolingOptions.Create();
                var session = GetSession(new ConstantSpeculativeExecutionPolicy(50L, 1), true, null, pooling);
                const int pauseThreshold = 140 * 2;
                var tasks = new List<Task<IPAddress>>();
                var semaphore = new SemaphoreSlim(150 * 2);
                for (var i = 0; i < 512; i++)
                {
                    //Pause after the stream ids are in use for the connections
                    if (i == pauseThreshold)
                    {
                        _testCluster.PauseNode(1);
                    }
                    semaphore.Wait();
                    tasks.Add(session
                        .ExecuteAsync(new SimpleStatement(QueryLocal).SetIdempotence(true))
                        .ContinueSync(rs =>
                        {
                            semaphore.Release();
                            return rs.Info.QueriedHost.Address;
                        }));
                }
                Task.WaitAll(tasks.Select(t => (Task)t).ToArray());
                _testCluster.ResumeNode(1);
                //There shouldn't be any query using node1 as coordinator passed the threshold.
                Assert.AreEqual(0, tasks.Skip(pauseThreshold).Count(t => t.Result.Equals(_addressNode1)));
                Thread.Sleep(1000);
            }
            finally
            {
                Cluster.MaxProtocolVersion = maxProtocolVersion;
            }
        }

        /// <summary>
        /// Tries to simulate GC pauses between 2 to 4 seconds
        /// </summary>
        [Test, TestTimeout(180000)]
        public void SpeculativeExecution_With_Multiple_Nodes_Pausing()
        {
            _testCluster = TestClusterManager.GetNonShareableTestCluster(3, 1, true, false);
            var session = GetSession(new ConstantSpeculativeExecutionPolicy(50L, 1));
            var timer = new HashedWheelTimer(1000, 64);
            timer.NewTimeout(_ => Task.Factory.StartNew(() => _testCluster.PauseNode(2)), null, 2000);
            //2 secs after resume node2
            timer.NewTimeout(_ => Task.Factory.StartNew(() => _testCluster.ResumeNode(2)), null, 4000);
            timer.NewTimeout(_ => Task.Factory.StartNew(() => _testCluster.PauseNode(1)), null, 6000);
            //4 secs after resume node1
            timer.NewTimeout(_ => Task.Factory.StartNew(() => _testCluster.ResumeNode(1)), null, 10000);
            var finished = false;
            timer.NewTimeout(_ => Task.Factory.StartNew(() => finished = true), null, 12000);
            //64 constant concurrent requests
            var semaphore = new SemaphoreSlim(64);
            while (!finished)
            {
                TestHelper.ParallelInvoke(() =>
                {
                    semaphore.Wait();
                    session.Execute(new SimpleStatement(QueryLocal).SetIdempotence(true));
                    semaphore.Release();
                }, 512);
            }
            Thread.Sleep(1000);
            timer.Dispose();
        }
    }
}
