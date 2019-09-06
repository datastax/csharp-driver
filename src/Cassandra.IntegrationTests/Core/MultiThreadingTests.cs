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
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long"), Ignore("tests that are not marked with 'short' need to be refactored/deleted")]
    public class MultiThreadingTests : TestGlobals
    {
        Builder _builder;
        private const int NodeCount = 2;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            var rp = new RetryLoadBalancingPolicy(new RoundRobinPolicy(), new ConstantReconnectionPolicy(100));
            rp.ReconnectionEvent += (s, ev) => Thread.Sleep((int)ev.DelayMs);
            _builder = Cluster.Builder()
                .WithReconnectionPolicy(new ConstantReconnectionPolicy(100))
                .WithQueryTimeout(60 * 1000)
                .WithLoadBalancingPolicy(rp);

            _builder = _builder.AddContactPoint(TestClusterManager.GetNonShareableTestCluster(NodeCount).InitialContactPoint);
        }

        /** name of test: ParallelInsertTest
         * 
         * @param nothingActually.
         * 
         */
        [Test]
        public void ParallelInsertTest()
        {
            Cluster localCluster = _builder.Build();
            ISession localSession = localCluster.Connect();
            string keyspaceName = "kp_pi1_" + Randomm.RandomAlphaNum(10);

            localSession.Execute(string.Format(@"CREATE KEYSPACE {0} WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }};", keyspaceName));

            TestUtils.WaitForSchemaAgreement(localCluster);
            localSession.ChangeKeyspace(keyspaceName);

            for (int KK = 0; KK < 1; KK++)
            {

                string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
                try
                {
                    localSession.Execute(string.Format(@"
                        CREATE TABLE {0}(
                        tweet_id uuid,
                        author text,
                        body text,
                        isok boolean,
                        PRIMARY KEY(tweet_id))", tableName));

                    TestUtils.WaitForSchemaAgreement(localCluster);
                }
                catch (AlreadyExistsException)
                {
                }

                int RowsNo = 1000;
                var ar = new IAsyncResult[RowsNo];
                var threads = new List<Thread>();
                var monit = new object();
                int readyCnt = 0;
                Trace.TraceInformation("Preparing...");

                for (int idx = 0; idx < RowsNo; idx++)
                {
                    int i = idx;
                    threads.Add(new Thread(() =>
                    {
                        try
                        {
                            lock (monit)
                            {
                                readyCnt++;
                                Monitor.Wait(monit);
                            }

                            ar[i] = localSession.BeginExecute(string.Format(@"
                                INSERT INTO {0} (tweet_id, author, isok, body) 
                                VALUES ({1},'test{2}',{3},'body{2}');", 
                                tableName, Guid.NewGuid(), i, i%2 == 0 ? "false" : "true"), ConsistencyLevel.One, null, null);
                            Interlocked.MemoryBarrier();
                        }
                        catch
                        {

                        }
                    }));
                }

                for (int idx = 0; idx < RowsNo; idx++)
                {
                    threads[idx].Start();
                }

                lock (monit)
                {
                    while (true)
                    {
                        if (readyCnt < RowsNo)
                        {
                            Monitor.Exit(monit);
                            Thread.Sleep(100);
                            Monitor.Enter(monit);
                        }
                        else
                        {
                            Monitor.PulseAll(monit);
                            break;
                        }
                    }
                }

                Trace.TraceInformation("Start!");

                var done = new HashSet<int>();
                while (done.Count < RowsNo)
                {
                    for (int i = 0; i < RowsNo; i++)
                    {
                        Interlocked.MemoryBarrier();
                        if (!done.Contains(i) && ar[i] != null)
                        {
                            if (ar[i].AsyncWaitHandle.WaitOne(10))
                            {
                                try
                                {
                                    localSession.EndExecute(ar[i]);
                                }
                                catch (Exception ex)
                                {
                                    Trace.TraceError("There was an exception while trying to end the async Execution: " + Environment.NewLine + ex);
                                }
                                done.Add(i);
                            }
                        }
                    }
                }

                Trace.TraceInformation("Inserted... now we are checking the count");

                var ret = localSession.Execute(string.Format(@"SELECT * from {0} LIMIT {1};", tableName, RowsNo + 100), ConsistencyLevel.Quorum);
                Assert.AreEqual(RowsNo, ret.GetRows().ToList().Count);


                for (int idx = 0; idx < RowsNo; idx++)
                {
                    threads[idx].Join();
                }
            }
        }

        [Test]
        public void InsertFireAndForget()
        {
            Cluster localCluster = null;
            try
            {
                string keyspaceName = "fireandforget_" + Randomm.RandomAlphaNum(8);
                localCluster = _builder.Build();
                ISession localSession = localCluster.Connect();
                localSession.CreateKeyspaceIfNotExists(keyspaceName);
                localSession.ChangeKeyspace(keyspaceName);
                localSession.Execute(String.Format(TestUtils.CreateTableAllTypes, "sampletable"));
                var insertStatement = localSession.Prepare("INSERT INTO sampletable (id, blob_sample) VALUES (?, ?)");
                var rowLength = 100;
                var rnd = new Random();
                var taskList = new List<Task<RowSet>>();
                for (var i = 0; i < rowLength; i++)
                {
                    taskList.Add(localSession.ExecuteAsync(insertStatement.Bind(Guid.NewGuid(), new byte[1024*rnd.Next(10)])));
                }

                var taskArray = taskList.ToArray();
                Task.WaitAny(taskArray);
                var rs = localSession.Execute("SELECT * FROM sampletable", ConsistencyLevel.One);
                Assert.IsTrue(rs.Count() > 0, "Table should contain 1 or more rows by now");

                Task.WaitAll(taskArray);
                rs = localSession.Execute("SELECT * FROM sampletable", ConsistencyLevel.Quorum);

                Assert.AreEqual(rowLength, rs.Count());
            }
            catch (Exception e)
            {
                Trace.TraceError("Unexpected Exception was thrown! Message: " + e.Message);
                throw e;
            }
            finally
            {
                if (localCluster != null)
                    localCluster.Shutdown();
            }
        }

        [Test]
        public void MassiveAsyncTest()
        {
            Cluster localCluster = _builder.Build();
            ISession localSession = localCluster.Connect();
            string keyspaceName = "kp_mat_" + Randomm.RandomAlphaNum(8);
            localSession.Execute(
                string.Format(@"CREATE KEYSPACE {0} 
                    WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }};"
                                , keyspaceName));
            localSession.ChangeKeyspace(keyspaceName);

            string tableName = "table" + Randomm.RandomAlphaNum(8);
            
            localSession.Execute(string.Format(@"CREATE TABLE {0}(
                    tweet_id uuid,
                    author text,
                    body text,
                    isok boolean,
                    PRIMARY KEY(tweet_id))", tableName));

            int RowsNo = 100;
            var ar = new bool[RowsNo];
            var thr = new Thread(() =>
            {
                for (int i = 0; i < RowsNo; i++)
                {
                    int tmpi = i;
                    localSession.BeginExecute(string.Format(@"INSERT INTO {0} (
                             tweet_id,
                             author,
                             isok,
                             body)
                            VALUES ({1},'test{2}',{3},'body{2}');", tableName, Guid.NewGuid(), i, i%2 == 0 ? "false" : "true")
                        , ConsistencyLevel.One, _ =>
                        {
                            ar[tmpi] = true;
                            Interlocked.MemoryBarrier();
                        }, null);
                }
            });

            thr.Start();

            var done = new HashSet<int>();
            while (done.Count < RowsNo)
            {
                for (int i = 0; i < RowsNo; i++)
                {
                    Interlocked.MemoryBarrier();
                    if (!done.Contains(i) && ar[i])
                    {
                        done.Add(i);
                    }
                }
            }

            thr.Join();

            Trace.TraceInformation("Inserted... now we are checking the count");

            var ret = localSession.Execute(string.Format(@"SELECT * from {0} LIMIT {1};", tableName, RowsNo + 100), ConsistencyLevel.Quorum);
            Assert.AreEqual(RowsNo, ret.GetRows().ToList().Count);
            localSession.Dispose();
        }

        [Test]
        public void ShutdownAsyncTest()
        {
            Cluster localCluster = _builder.Build();
            ISession localSession = localCluster.Connect();
            string keyspaceName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();
            localSession.Execute(
                    string.Format(@"CREATE KEYSPACE {0} WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }};", keyspaceName));
            localSession.ChangeKeyspace(keyspaceName);

            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                localSession.Execute(string.Format(@"
                    CREATE TABLE {0}(
                    tweet_id uuid,
                    author text,
                    body text,
                    isok boolean,
                    PRIMARY KEY(tweet_id))", tableName));
            }
            catch (AlreadyExistsException)
            {
            }

            int RowsNo = 1000;

            var ar = new bool[RowsNo];

            for (int i = 0; i < RowsNo; i++)
            {
                int tmpi = i;
                try
                {
                    var query = string.Format(@"INSERT INTO {0} (tweet_id, author, isok, body) VALUES ({1},'test{2}',{3},'body{2}');", 
                        tableName, Guid.NewGuid(), i, i%2 == 0 ? "false" : "true");
                    localSession.BeginExecute(query, ConsistencyLevel.Quorum, arx =>
                    {
                        try
                        {
                            localSession.EndExecute(arx);
                        }
                        catch (ObjectDisposedException)
                        {

                        }
                        finally
                        {
                            ar[tmpi] = true;
                            Interlocked.MemoryBarrier();
                        }
                    }, null);
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
            }
            localSession.Dispose();
        }
    }
}
