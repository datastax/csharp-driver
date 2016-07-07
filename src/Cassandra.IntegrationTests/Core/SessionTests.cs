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
using Cassandra.Tasks;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class SessionTests : SharedClusterTest
    {
        public SessionTests() : base(2)
        {
            
        }

        [Test, Ignore("Needs refactor")]
        public void Session_Cancels_Pending_When_Disposed()
        {
            Trace.TraceInformation("SessionCancelsPendingWhenDisposed");
            var localCluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint).Build();
            try
            {
                var localSession = localCluster.Connect(KeyspaceName);
                localSession.Execute("CREATE TABLE tbl_cancel_pending (id uuid primary key)");
                Thread.Sleep(2000);
                var taskList = new List<Task>();
                for (var i = 0; i < 500; i++)
                {
                    taskList.Add(localSession.ExecuteAsync(new SimpleStatement("INSERT INTO tbl_cancel_pending (id) VALUES (uuid())")));
                }
                //Most task should be pending
                Assert.True(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "Most task should be pending");
                //Force it to close connections
                Trace.TraceInformation("Start Disposing localSession");
                localSession.Dispose();
                //Wait for the worker threads to cancel the rest of the operations.
                DateTime timeInTheFuture = DateTime.Now.AddSeconds(11);
                while (DateTime.Now < timeInTheFuture &&
                       taskList.Any(t => t.Status == TaskStatus.WaitingForActivation))
                {
                    int waitMs = 500;
                    Trace.TraceInformation(string.Format("Waiting {0} more MS ... ", waitMs));
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
        public void Session_Gracefully_Waits_Pending_Operations()
        {
            Trace.TraceInformation("Starting SessionGracefullyWaitsPendingOperations");
            var localCluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint).Build();
            try
            {
                var localSession = (Session)localCluster.Connect(KeyspaceName);
                localSession.Execute("CREATE TABLE tbl_wait_pending (id uuid primary key)");
                //Create more async operations that can be finished
                var taskList = new List<Task>();
                for (var i = 0; i < 20; i++)
                {
                    taskList.Add(localSession.ExecuteAsync(new SimpleStatement(String.Format("INSERT INTO tbl_wait_pending (id) VALUES ({0})", Guid.NewGuid()))));
                }
                //Most task should be pending
                Assert.True(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "Most task should be pending");
                //Wait for finish
                Assert.True(localSession.WaitForAllPendingActions(20000), "All handles have received signal");
                Thread.Sleep(2000);
                Assert.False(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "All task should be completed (not pending)");
                //Either all completed or some of them can contain 
                Assert.True(taskList.All(t => 
                    t.Status == TaskStatus.RanToCompletion || 
                    (t.Exception != null && t.Exception.InnerException is WriteTimeoutException)), "All task should be completed");

                localSession.Dispose();
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void Session_Keyspace_Does_Not_Exist_On_Connect_Throws()
        {
            var localCluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint).Build();
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
        public void Session_Keyspace_Empty_On_Connect()
        {
            var localCluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint).Build();
            try
            {
                Assert.DoesNotThrow(() =>
                {
                    var localSession = localCluster.Connect("");
                    localSession.Execute("SELECT * FROM system.local");
                });
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void Session_Keyspace_Does_Not_Exist_On_Change_Throws()
        {
            var localCluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint).Build();
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
        public void Session_Keyspace_Connect_Case_Sensitive()
        {
            var localCluster = Cluster.Builder()
                .AddContactPoint(TestCluster.InitialContactPoint)
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
        public void Session_Use_Statement_Changes_Keyspace()
        {
            var localCluster = Cluster.Builder()
                .AddContactPoint(TestCluster.InitialContactPoint)
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
                        localSession.Execute("select * from local");
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
        public void Session_Use_Statement_Changes_Keyspace_Case_Insensitive()
        {
            var localCluster = Cluster.Builder()
                .AddContactPoint(TestCluster.InitialContactPoint)
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
                        localSession.Execute("select * from local");
                    }
                });
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void Session_Keyspace_Create_Case_Sensitive()
        {
            var localCluster = Cluster.Builder()
                .AddContactPoint(TestCluster.InitialContactPoint)
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
        public void Should_Create_The_Right_Amount_Of_Connections()
        {
            var localCluster1 = Cluster.Builder()
                .AddContactPoint(TestCluster.InitialContactPoint)
                .WithPoolingOptions(new PoolingOptions().SetCoreConnectionsPerHost(HostDistance.Local, 3))
                .Build();
            Cluster localCluster2 = null;
            try
            {
                var localSession1 = (Session)localCluster1.Connect();
                var hosts1 = localCluster1.AllHosts().ToList();
                Assert.AreEqual(2, hosts1.Count);
                //Execute multiple times a query on the newly created keyspace
                for (var i = 0; i < 12; i++)
                {
                    localSession1.Execute("SELECT * FROM system.local");
                }
                Thread.Sleep(2000);
                var pool11 = localSession1.GetOrCreateConnectionPool(hosts1[0], HostDistance.Local);
                var pool12 = localSession1.GetOrCreateConnectionPool(hosts1[1], HostDistance.Local);
                Assert.That(pool11.OpenConnections.Count(), Is.EqualTo(3));
                Assert.That(pool12.OpenConnections.Count(), Is.EqualTo(3));
                
                localCluster2 = Cluster.Builder()
                    .AddContactPoint(TestCluster.InitialContactPoint)
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
                Thread.Sleep(2000);
                var pool21 = localSession2.GetOrCreateConnectionPool(hosts2[0], HostDistance.Local);
                var pool22 = localSession2.GetOrCreateConnectionPool(hosts2[1], HostDistance.Local);
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
        public void Session_Disposed_On_Cluster()
        {
            var cluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint).Build();
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

#if !NETCORE
        [Test, Apartment(ApartmentState.STA)]
        public void Session_Connect_And_ShutDown_SupportsSTA()
        {
            Assert.DoesNotThrow(() =>
            {
                using (var localCluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint).Build())
                {
                    var localSession = localCluster.Connect();
                    var ps = localSession.Prepare("SELECT * FROM system.local");
                    TestHelper.Invoke(() => localSession.Execute(ps.Bind()), 10);
                }
            });
        }
#endif

        [Test]
        public void Session_Execute_Logging_With_Verbose_Level_Test()
        {
            var originalLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            Assert.DoesNotThrow(() =>
            {
                using (var localCluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint).Build())
                {
                    var localSession = localCluster.Connect("system");
                    var ps = localSession.Prepare("SELECT * FROM local");
                    TestHelper.ParallelInvoke(() => localSession.Execute(ps.Bind()), 100);
                }
            });
            Diagnostics.CassandraTraceSwitch.Level = originalLevel;
        }

        [Test]
        public void Session_Execute_Throws_TimeoutException_When_QueryAbortTimeout_Elapsed()
        {
            var dummyCluster = Cluster.Builder().AddContactPoint("0.0.0.0").Build();
            Assert.AreNotEqual(dummyCluster.Configuration.ClientOptions.QueryAbortTimeout, Timeout.Infinite);
            try
            {
                using (var localCluster = Cluster.Builder()
                    .AddContactPoint(TestCluster.InitialContactPoint)
                    //Disable socket read timeout
                    .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(0))
                    //Set abort timeout at a low value
                    .WithQueryTimeout(1500)
                    .Build())
                {
                    var localSession = localCluster.Connect("system");
                    localSession.Execute("SELECT * FROM local");
                    TestCluster.PauseNode(1);
                    TestCluster.PauseNode(2);
                    Assert.Throws<TimeoutException>(() => localSession.Execute("SELECT * FROM local"));
                }
            }
            finally
            {
                TestCluster.ResumeNode(1);
                TestCluster.ResumeNode(2);
            }
        }

        /// Tests that void results return empty RowSets
        ///
        /// Empty_RowSet_Test tests that empty RowSets are returned for void results. It creates a simple table and performs
        /// an INSERT query on the table, returning an empty RowSet. It then verifies the RowSet metadata is populated
        /// properly.
        ///
        /// @since 3.0.0
        /// @jira_ticket CSHARP-377
        /// @expected_result RowSet metadata is properly returned
        ///
        /// @test_category queries:basic
        [Test]
        public void Empty_RowSet_Test()
        {
            var localCluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint).Build();
            var localSession = localCluster.Connect(KeyspaceName);
            localSession.Execute("CREATE TABLE test (k int PRIMARY KEY, v int)");

            try
            {
                var rowSet = localSession.Execute("INSERT INTO test (k, v) VALUES (0, 0)");
                Assert.True(rowSet.IsExhausted());
                Assert.True(rowSet.IsFullyFetched);
                Assert.AreEqual(0, rowSet.Count());
                Assert.AreEqual(0, rowSet.GetAvailableWithoutFetching());
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }
    }
}
