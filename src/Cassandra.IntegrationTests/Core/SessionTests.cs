//
//      Copyright (C) 2012-2014 DataStax Inc.
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

using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using System.Diagnostics;
using Cassandra.Tests;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class SessionTests : TestGlobals
    {
        ISession _session;
        ITestCluster _testCluster;
        const int NodeCount = 2;

        [TestFixtureSetUp]
        public void SetupTest()
        {
            // we just want to make sure there's a local 2 node cluster
            _testCluster = TestClusterManager.GetTestCluster(NodeCount);
            _session = _testCluster.Session;
        }

        [Test]
        public void SessionCancelsPendingWhenDisposed()
        {
            Trace.TraceInformation("SessionCancelsPendingWhenDisposed");
            var localCluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build();
            try
            {
                var localSession = localCluster.Connect();
                var taskList = new List<Task>();
                for (var i = 0; i < 500; i++)
                {
                    taskList.Add(localSession.ExecuteAsync(new SimpleStatement("SELECT * FROM system.local")));
                }
                //Most task should be pending
                Assert.True(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "Most task should be pending");
                //Force it to close connections
                Trace.TraceInformation("Start Disposing localSession");
                localSession.Dispose();
                //Wait for the worker threads to cancel the rest of the operations.
                DateTime timeInTheFuture = DateTime.Now.AddSeconds(11);
                while (DateTime.Now < timeInTheFuture &&
                       (taskList.Any(t => t.Status == TaskStatus.WaitingForActivation) ||
                        taskList.All(t => t.Status == TaskStatus.RanToCompletion || t.Status == TaskStatus.Faulted)))
                {
                    int waitMs = 500;
                    Trace.TraceInformation(string.Format("In method: {0}, waiting {1} more MS ... ", System.Reflection.MethodBase.GetCurrentMethod().Name, waitMs));
                    Thread.Sleep(waitMs);
                }
                Assert.False(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "No more task should be pending");
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion || t.Status == TaskStatus.Faulted), "All task should be completed or faulted");
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionGracefullyWaitsPendingOperations()
        {
            Trace.TraceInformation("Starting SessionGracefullyWaitsPendingOperations");
            var localCluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build();
            try
            {
                var localSession = (Session)localCluster.Connect();

                //Create more async operations that can be finished
                var taskList = new List<Task>();
                for (var i = 0; i < 512; i++)
                {
                    taskList.Add(localSession.ExecuteAsync(new SimpleStatement("SELECT * FROM system.local")));
                }
                //Most task should be pending
                Assert.True(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "Most task should be pending");
                //Wait for finish
                Assert.True(localSession.WaitForAllPendingActions(60000), "All handles have received signal");

                Assert.False(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "All task should be completed (not pending)");

                if (taskList.Any(t => t.Status == TaskStatus.Faulted))
                {
                    throw taskList.First(t => t.Status == TaskStatus.Faulted).Exception;
                }
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion), "All task should be completed");

                localSession.Dispose();
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionKeyspaceDoesNotExistOnConnectThrows()
        {
            var localCluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build();
            try
            {
                var ex = Assert.Throws<InvalidQueryException>(() => localCluster.Connect("THIS_KEYSPACE_DOES_NOT_EXIST"));
                Assert.True(ex.Message.ToLower().Contains("keyspace"));
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionKeyspaceEmptyOnConnect()
        {
            var localCluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build();
            try
            {
                Assert.DoesNotThrow(() =>
                {
                    var localSession = localCluster.Connect("");
                    localSession.Execute("SELECT * FROM system.schema_keyspaces");
                });
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionKeyspaceDoesNotExistOnChangeThrows()
        {
            var localCluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build();
            try
            {
                var localSession = localCluster.Connect();
                var ex = Assert.Throws<InvalidQueryException>(() => localSession.ChangeKeyspace("THIS_KEYSPACE_DOES_NOT_EXIST_EITHER"));
                Assert.True(ex.Message.ToLower().Contains("keyspace"));
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionKeyspaceConnectCaseSensitive()
        {
            var localCluster = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .Build();
            try
            {
                Assert.Throws<InvalidQueryException>(() => localCluster.Connect("SYSTEM"));
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionUseStatementChangesKeyspace()
        {
            var localCluster = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .Build();
            try
            {
                var localSession = localCluster.Connect();
                localSession.Execute("USE system");
                //The session should be using the system keyspace now
                Assert.DoesNotThrow(() =>
                {
                    for (var i = 0; i < 5; i++)
                    {
                        localSession.Execute("select * from schema_keyspaces");
                    }
                });
                Assert.That(localSession.Keyspace, Is.EqualTo("system"));
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionUseStatementChangesKeyspaceCaseInsensitive()
        {
            var localCluster = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .Build();
            try
            {
                var localSession = localCluster.Connect();
                //The statement is case insensitive by default, as no quotes were specified
                localSession.Execute("USE SyStEm");
                //The session should be using the system keyspace now
                Assert.DoesNotThrow(() =>
                {
                    for (var i = 0; i < 5; i++)
                    {
                        localSession.Execute("select * from schema_keyspaces");
                    }
                });
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionKeyspaceCreateCaseSensitive()
        {
            var localCluster = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .Build();
            try
            {
                var localSession = localCluster.Connect();
                const string ks1 = "UPPER_ks";
                localSession.CreateKeyspace(ks1);
                localSession.ChangeKeyspace(ks1);
                localSession.Execute("CREATE TABLE test1 (k uuid PRIMARY KEY, v text)");
                TestUtils.WaitForSchemaAgreement(localCluster);

                //Execute multiple times a query on the newly created keyspace
                Assert.DoesNotThrow(() =>
                {
                    for (var i = 0; i < 5; i++)
                    {
                        localSession.Execute("SELECT * FROM test1");
                    }
                });
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void ShouldCreateTheRightAmountOfConnections()
        {
            var localCluster1 = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .WithPoolingOptions(new PoolingOptions().SetCoreConnectionsPerHost(HostDistance.Local, 3))
                .Build();
            Cluster localCluster2 = null;
            try
            {
                var localSession1 = (Session)localCluster1.Connect();
                var hosts1 = localCluster1.AllHosts().ToList();
                Assert.AreEqual(2, hosts1.Count);
                //Execute multiple times a query on the newly created keyspace
                for (var i = 0; i < 6; i++)
                {
                    localSession1.Execute("SELECT * FROM system.local");
                }
                var pool11 = localSession1.GetConnectionPool(hosts1[0], HostDistance.Local);
                var pool12 = localSession1.GetConnectionPool(hosts1[1], HostDistance.Local);
                Assert.That(pool11.OpenConnections.Count(), Is.EqualTo(3));
                Assert.That(pool12.OpenConnections.Count(), Is.EqualTo(3));
                
                localCluster2 = Cluster.Builder()
                    .AddContactPoint(_testCluster.InitialContactPoint)
                    .WithPoolingOptions(new PoolingOptions().SetCoreConnectionsPerHost(HostDistance.Local, 1))
                    .Build();
                var localSession2 = (Session)localCluster2.Connect();
                var hosts2 = localCluster2.AllHosts().ToList();
                Assert.AreEqual(2, hosts2.Count);
                //Execute multiple times a query on the newly created keyspace
                for (var i = 0; i < 6; i++)
                {
                    localSession2.Execute("SELECT * FROM system.local");
                }
                var pool21 = localSession2.GetConnectionPool(hosts2[0], HostDistance.Local);
                var pool22 = localSession2.GetConnectionPool(hosts2[1], HostDistance.Local);
                Assert.That(pool21.OpenConnections.Count(), Is.EqualTo(1));
                Assert.That(pool22.OpenConnections.Count(), Is.EqualTo(1));
            }
            finally
            {
                localCluster1.Shutdown(1000);
                if (localCluster2 != null)
                {
                    localCluster2.Shutdown(1000);
                }
            }
        }

        /// <summary>
        /// Checks that having a disposed Session created by the cluster does not affects other sessions
        /// </summary>
        [Test]
        public void SessionDisposedOnCluster()
        {
            var cluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build();
            var session1 = cluster.Connect();
            var session2 = cluster.Connect();
            TestHelper.ParallelInvoke(() => session1.Execute("SELECT * from system.local"), 5);
            TestHelper.ParallelInvoke(() => session2.Execute("SELECT * from system.local"), 5);
            //Dispose the first session
            Trace.TraceInformation("Dispose the first session");
            session1.Dispose();

            //All nodes should be up
            Assert.AreEqual(cluster.AllHosts().Count, cluster.AllHosts().Count(h => h.IsUp));
            //And session2 should be queryable
            TestHelper.ParallelInvoke(() => session2.Execute("SELECT * from system.local"), 5);
            Trace.TraceInformation("Disposing cluster");
            cluster.Dispose();
        }

        [Test, RequiresSTA]
        public void Session_Connect_And_ShutDown_SupportsSTA()
        {
            Assert.DoesNotThrow(() =>
            {
                using (var localCluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build())
                {
                    var localSession = localCluster.Connect();
                    var ps = localSession.Prepare("SELECT * FROM system.local");
                    TestHelper.Invoke(() => localSession.Execute(ps.Bind()), 10);
                }
            });
        }
    }
}
