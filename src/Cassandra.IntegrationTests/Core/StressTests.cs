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

﻿using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class StressTests
    {
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            //For different threads
            Trace.AutoFlush = true;
        }

        /// <summary>
        /// In parallel it inserts some records and selects them using Session.Execute sync methods.
        /// </summary>
        [Test]
        public void ParallelInsertAndSelectSync()
        {
            var originalTraceLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Warning;
            var builder = Cluster.Builder()
                .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            try
            {
                var session = clusterInfo.Session;
                session.Execute(@"
                    CREATE KEYSPACE tester
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
                ");
                TestUtils.WaitForSchemaAgreement(clusterInfo);
                session.ChangeKeyspace("tester");

                string tableName = "table_" + Guid.NewGuid().ToString("N").ToLower();
                session.Execute(String.Format(TestUtils.CREATE_TABLE_TIME_SERIES, tableName));
                TestUtils.WaitForSchemaAgreement(clusterInfo);

                var insertQuery = String.Format("INSERT INTO {0} (id, event_time, text_sample) VALUES (?, ?, ?)", tableName);
                var insertQueryPrepared = session.Prepare(insertQuery);
                var selectQuery = String.Format("SELECT * FROM {0} LIMIT 10000", tableName);

                const int rowsPerId = 100;
                object insertQueryStatement = new SimpleStatement(insertQuery);
                if (Options.Default.CassandraVersion.Major < 2)
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
                for (var i = 0; i < 10; i++ )
                {
                    //Add 10 actions to execute
                    actions.AddRange(new[] { actionInsert, actionSelect, actionInsertPrepared });
                    actions.AddRange(new[] { actionSelect, actionInsert, actionInsertPrepared, actionInsert });
                    actions.AddRange(new[] { actionInsertPrepared, actionInsertPrepared, actionSelect });
                }
                //Execute in parallel the 100 actions
                var parallelOptions = new ParallelOptions();
                parallelOptions.TaskScheduler = new ThreadPerTaskScheduler();
                parallelOptions.MaxDegreeOfParallelism = 1000;
                Parallel.Invoke(parallelOptions, actions.ToArray());
                Parallel.Invoke(actions.ToArray());
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
            Diagnostics.CassandraTraceSwitch.Level = originalTraceLevel;
        }
        /// <summary>
        /// In parallel it inserts some records and selects them using Session.Execute sync methods.
        /// </summary>
        [Test]
        [TestCassandraVersion(2, 0)]
        public void ParallelInsertAndSelectSyncWithNodesFailing()
        {
            var originalTraceLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Warning;
            var builder = Cluster.Builder()
                .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            try
            {
                var session = clusterInfo.Session;
                session.Execute(@"
                    CREATE KEYSPACE tester
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
                ");
                TestUtils.WaitForSchemaAgreement(clusterInfo);
                session.ChangeKeyspace("tester");

                string tableName = "table_" + Guid.NewGuid().ToString("N").ToLower();
                session.Execute(String.Format(TestUtils.CREATE_TABLE_TIME_SERIES, tableName));
                TestUtils.WaitForSchemaAgreement(clusterInfo);

                var insertQuery = String.Format("INSERT INTO {0} (id, event_time, text_sample) VALUES (?, ?, ?)", tableName);
                var insertQueryPrepared = session.Prepare(insertQuery);
                var selectQuery = String.Format("SELECT * FROM {0} LIMIT 10000", tableName);

                const int rowsPerId = 100;
                object insertQueryStatement = new SimpleStatement(insertQuery);
                if (Options.Default.CassandraVersion.Major < 2)
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
                    TestUtils.CcmStopForce(clusterInfo, 2);
                });

                //Execute in parallel more than 100 actions
                var parallelOptions = new ParallelOptions();
                parallelOptions.TaskScheduler = new ThreadPerTaskScheduler();
                parallelOptions.MaxDegreeOfParallelism = 1000;
                Parallel.Invoke(parallelOptions, actions.ToArray());
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
            Diagnostics.CassandraTraceSwitch.Level = originalTraceLevel;
        }

        [Test]
        public void TestInactivity()
        {
            var clusterInfo = TestUtils.CcmSetup(1);
            try
            {
                var rs = clusterInfo.Session.Execute("SELECT * FROM system.schema_keyspaces");
                Assert.Greater(rs.Count(), 0);
                Thread.Sleep(5 * 60 * 1000);

                rs = clusterInfo.Session.Execute("SELECT * FROM system.schema_keyspaces");
                Assert.Greater(rs.Count(), 0);
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        public Action GetInsertAction(ISession session, object bindableStatement, ConsistencyLevel consistency, int rowsPerId)
        {
            Action action = () =>
            {
                Trace.TraceInformation("Starting inserting from thread {0}", Thread.CurrentThread.ManagedThreadId);
                var id = Guid.NewGuid();
                for (var i = 0; i < rowsPerId; i++)
                {
                    var paramsArray = new object[] { id, DateTime.Now, DateTime.Now.ToString() };
                    IStatement statement = null;
                    if (bindableStatement is SimpleStatement)
                    {
                        statement = ((SimpleStatement)bindableStatement).Bind(paramsArray).SetConsistencyLevel(consistency);
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
                    var rs = session.Execute(new SimpleStatement(query).SetPageSize(500).SetConsistencyLevel(consistency).SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance));
                    //Count will iterate through the result set and it will likely to page results
                    //Assert.True(rs.Count() > 0);
                }
            };
            return action;
        }
    }
}
