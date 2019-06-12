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

using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.Policies.Util;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long"), Ignore("tests that are not marked with 'short' need to be refactored/deleted")]
    public class StressTests : TestGlobals
    {
        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            //For different threads
            Trace.AutoFlush = true;
        }

        /// <summary>
        /// Insert and select records in parallel them using Session.Execute sync methods.
        /// </summary>
        [Test]
        public void Parallel_Insert_And_Select_Sync()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(3, 1, true, false);
            using (var cluster = Cluster.Builder()
                                        .WithRetryPolicy(AlwaysRetryRetryPolicy.Instance)
                                        .AddContactPoint(testCluster.InitialContactPoint)
                                        .Build())
            {
                var session = cluster.Connect();
                var uniqueKsName = "keyspace_" + Randomm.RandomAlphaNum(10);
                session.Execute(@"CREATE KEYSPACE " + uniqueKsName +
                                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
                session.ChangeKeyspace(uniqueKsName);

                var tableName = "table_" + Guid.NewGuid().ToString("N").ToLower();
                session.Execute(String.Format(TestUtils.CREATE_TABLE_TIME_SERIES, tableName));

                var insertQuery = String.Format("INSERT INTO {0} (id, event_time, text_sample) VALUES (?, ?, ?)", tableName);
                var insertQueryPrepared = session.Prepare(insertQuery);
                var selectQuery = String.Format("SELECT * FROM {0} LIMIT 10000", tableName);

                const int rowsPerId = 1000;
                object insertQueryStatement = new SimpleStatement(insertQuery);
                if (CassandraVersion.Major < 2)
                {
                    //Use prepared statements all the way as it is not possible to bind on a simple statement with C* 1.2
                    insertQueryStatement = session.Prepare(insertQuery);
                }
                var actionInsert = GetInsertAction(session, insertQueryStatement, ConsistencyLevel.Quorum, rowsPerId);
                var actionInsertPrepared = GetInsertAction(session, insertQueryPrepared, ConsistencyLevel.Quorum, rowsPerId);
                var actionSelect = GetSelectAction(session, selectQuery, ConsistencyLevel.Quorum, 10);

                //Execute insert sync to have some records
                actionInsert();
                //Execute select sync to assert that everything is going OK
                actionSelect();


                var actions = new List<Action>();
                for (var i = 0; i < 10; i++)
                {
                    //Add 10 actions to execute
                    actions.AddRange(new[] {actionInsert, actionSelect, actionInsertPrepared});
                    actions.AddRange(new[] {actionSelect, actionInsert, actionInsertPrepared, actionInsert});
                    actions.AddRange(new[] {actionInsertPrepared, actionInsertPrepared, actionSelect});
                }
                //Execute in parallel the 100 actions
                TestHelper.ParallelInvoke(actions.ToArray());
            }
        }


        /// <summary>
        /// In parallel it inserts some records and selects them using Session.Execute sync methods.
        /// </summary>
        [Test]
        [TestCassandraVersion(2, 0)]
        public void Parallel_Insert_And_Select_Sync_With_Nodes_Failing()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(3, 1, true, false);
            using (var cluster = Cluster.Builder()
                .WithRetryPolicy(AlwaysRetryRetryPolicy.Instance)
                .AddContactPoint(testCluster.InitialContactPoint)
                .Build())
            {
                var session = cluster.Connect();
                var uniqueKsName = "keyspace_" + Randomm.RandomAlphaNum(10);
                session.Execute(@"CREATE KEYSPACE " + uniqueKsName +
                    " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
                session.ChangeKeyspace(uniqueKsName);

                var tableName = "table_" + Guid.NewGuid().ToString("N").ToLower();
                session.Execute(String.Format(TestUtils.CREATE_TABLE_TIME_SERIES, tableName));

                var insertQuery = String.Format("INSERT INTO {0} (id, event_time, text_sample) VALUES (?, ?, ?)", tableName);
                var insertQueryPrepared = session.Prepare(insertQuery);
                var selectQuery = String.Format("SELECT * FROM {0} LIMIT 10000", tableName);

                const int rowsPerId = 100;
                object insertQueryStatement = new SimpleStatement(insertQuery);
                if (CassandraVersion.Major < 2)
                {
                    //Use prepared statements all the way as it is not possible to bind on a simple statement with C* 1.2
                    insertQueryStatement = session.Prepare(insertQuery);
                }
                var actionInsert = GetInsertAction(session, insertQueryStatement, ConsistencyLevel.Quorum, rowsPerId);
                var actionInsertPrepared = GetInsertAction(session, insertQueryPrepared, ConsistencyLevel.Quorum, rowsPerId);
                var actionSelect = GetSelectAction(session, selectQuery, ConsistencyLevel.Quorum, 10);

                //Execute insert sync to have some records
                actionInsert();
                //Execute select sync to assert that everything is going OK
                actionSelect();

                var actions = new List<Action>();
                for (var i = 0; i < 10; i++)
                {
                    //Add 10 actions to execute
                    actions.AddRange(new[] { actionInsert, actionSelect, actionInsertPrepared });
                    actions.AddRange(new[] { actionSelect, actionInsert, actionInsertPrepared, actionInsert });
                    actions.AddRange(new[] { actionInsertPrepared, actionInsertPrepared, actionSelect });
                }

                actions.Insert(8, () =>
                {
                    Thread.Sleep(300);
                    testCluster.StopForce(2);
                });
                TestHelper.ParallelInvoke(actions.ToArray());
            }
        }

        /// <summary>
        /// Creates thousands of Session instances and checks memory consumption before and after Dispose and dereferencing
        /// </summary>
        [Test]
        public void Memory_Consumption_After_Cluster_Dispose()
        {
            var testCluster = TestClusterManager.GetTestCluster(2, DefaultMaxClusterCreateRetries, true, false);
            //Only warning as tracing through console slows it down.
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Warning;
            var start = GC.GetTotalMemory(false);
            long diff = 0;
            Trace.TraceInformation("--Initial memory: {0}", start / 1024);
            Action multipleConnect = () =>
            {
                var cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).Build();
                for (var i = 0; i < 200; i++)
                {
                    var session = cluster.Connect();
                    TestHelper.ParallelInvoke(() => session.Execute("select * from system.local"), 20);
                }
                Trace.TraceInformation("--Before disposing: {0}", GC.GetTotalMemory(false) / 1024);
                cluster.Dispose();
                Trace.TraceInformation("--After disposing: {0}", GC.GetTotalMemory(false) / 1024);
            };
            multipleConnect();
            var handle = new AutoResetEvent(false);
            //Collect all generations
            GC.Collect();
            //Wait 5 seconds for all the connections resources to be freed
            Timer timer = null;
            timer = new Timer(s =>
            {
                GC.Collect();
                handle.Set();
                diff = GC.GetTotalMemory(false) - start;
                Trace.TraceInformation("--End, diff memory: {0}", diff / 1024);
                var t = timer;
                if (t != null)
                {
                    t.Dispose();
                }
            }, null, 5000, Timeout.Infinite);
            handle.WaitOne();
            //the end memory usage can not be greater than double the start memory
            //If there is a memory leak, it should be an order of magnitude greater...
            Assert.Less(diff, start / 2);
        }

        public Action GetInsertAction(ISession session, object bindableStatement, ConsistencyLevel consistency, int rowsPerId)
        {
            Action action = () =>
            {
                Trace.TraceInformation("Starting inserting from thread {0}", Thread.CurrentThread.ManagedThreadId);
                var id = Guid.NewGuid();
                for (var i = 0; i < rowsPerId; i++)
                {
                    var paramsArray = new object[] { id, DateTime.Now, DateTime.Now.ToString(System.Globalization.CultureInfo.InvariantCulture) };
                    IStatement statement;
                    if (bindableStatement is SimpleStatement)
                    {
                        statement = new SimpleStatement(((SimpleStatement)bindableStatement).QueryString, paramsArray).SetConsistencyLevel(consistency);
                    }
                    else if (bindableStatement is PreparedStatement)
                    {
                        statement = ((PreparedStatement)bindableStatement).Bind(paramsArray).SetConsistencyLevel(consistency);
                    }
                    else
                    {
                        throw new Exception("Can not bind a statement of type " + bindableStatement.GetType().FullName);
                    }
                    session.Execute(statement);
                }
                Trace.TraceInformation("Finished inserting from thread {0}", Thread.CurrentThread.ManagedThreadId);
            };
            return action;
        }

        public Action GetSelectAction(ISession session, string query, ConsistencyLevel consistency, int executeLength)
        {
            Action action = () =>
            {
                for (var i = 0; i < executeLength; i++)
                {
                    session.Execute(new SimpleStatement(query).SetPageSize(500).SetConsistencyLevel(consistency));
                    //Count will iterate through the result set and it will likely to page results
                    //Assert.True(rs.Count() > 0);
                }
            };
            return action;
        }
    }
}
