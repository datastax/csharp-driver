//
//      Copyright (C) 2012 DataStax Inc.
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
    [TestFixture, Category("long")]
    public class MultiThreadingTests : TwoNodesClusterTest
    {
        public override void TestFixtureSetUp()
        {
            this.Builder = Cluster.Builder()
                .WithReconnectionPolicy(new ConstantReconnectionPolicy(100))
                .WithQueryTimeout(60 * 1000);

            var rp = new RetryLoadBalancingPolicy(new RoundRobinPolicy(), new ConstantReconnectionPolicy(100));
            rp.ReconnectionEvent += (s, ev) =>
            {
                Trace.TraceInformation("o");
                Thread.Sleep((int)ev.DelayMs);
            };
            this.Builder.WithLoadBalancingPolicy(rp);

            base.TestFixtureSetUp();
        }

        [Test]
        public void ParallelInsertTest()
        {
            var localSession = Cluster.Connect();
            string keyspaceName = "kp_pi1";

            localSession.WaitForSchemaAgreement(
                localSession.Execute(
                    string.Format(@"CREATE KEYSPACE {0} 
         WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }};"
                                  , keyspaceName)));

            localSession.ChangeKeyspace(keyspaceName);

            for (int KK = 0; KK < 1; KK++)
            {
                Trace.TraceInformation("Try no:" + KK);

                string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
                try
                {
                    localSession.WaitForSchemaAgreement(
                        localSession.Execute(string.Format(@"
                            CREATE TABLE {0}(
                            tweet_id uuid,
                            author text,
                            body text,
                            isok boolean,
                            PRIMARY KEY(tweet_id))", tableName)));
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
                            Trace.TraceInformation("+");
                            lock (monit)
                            {
                                readyCnt++;
                                Monitor.Wait(monit);
                            }

                            ar[i] = localSession.BeginExecute(string.Format(@"
                                INSERT INTO {0} (tweet_id, author, isok, body) 
                                VALUES ({1},'test{2}',{3},'body{2}');", 
                                tableName, Guid.NewGuid(), i, i%2 == 0 ? "false" : "true"), ConsistencyLevel.One, null, null);
                            Thread.MemoryBarrier();
                        }
                        catch
                        {
                            Trace.TraceInformation("@");
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
                        Thread.MemoryBarrier();
                        if (!done.Contains(i) && ar[i] != null)
                        {
                            if (ar[i].AsyncWaitHandle.WaitOne(10))
                            {
                                try
                                {
                                    localSession.EndExecute(ar[i]);
                                }
                                catch
                                {
                                    Trace.TraceInformation("!");
                                }
                                done.Add(i);
                                Trace.TraceInformation("-");
                            }
                        }
                    }
                }

                Trace.TraceInformation("Inserted... now we are checking the count");

                var ret = localSession.Execute(string.Format(@"SELECT * from {0} LIMIT {1};", tableName, RowsNo + 100), ConsistencyLevel.Quorum);
                Assert.AreEqual(RowsNo, ret.GetRows().ToList().Count);


                localSession.Execute(string.Format(@"DROP TABLE {0};", tableName));

                for (int idx = 0; idx < RowsNo; idx++)
                {
                    threads[idx].Join();
                }
            }
            localSession.Execute(string.Format(@"DROP KEYSPACE {0};", keyspaceName));
        }

        [Test]
        public void ErrorInjectionInParallelInsertTest()
        {
            var localSession = (Session) Cluster.Connect();
            string keyspaceName = "kp_eipi";
            localSession.WaitForSchemaAgreement(
                localSession.Execute(
                    string.Format(@"CREATE KEYSPACE {0} WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }};", keyspaceName)));
            localSession.ChangeKeyspace(keyspaceName);

            for (int KK = 0; KK < 1; KK++)
            {
                Trace.TraceInformation("Try no:" + KK);

                string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
                try
                {
                    localSession.WaitForSchemaAgreement(
                        localSession.Execute(string.Format(@"
                            CREATE TABLE {0}(
                            tweet_id uuid,
                            author text,
                            body text,
                            isok boolean,
                            PRIMARY KEY(tweet_id))", tableName)));
                }
                catch (AlreadyExistsException)
                {
                }

                int RowsNo = 250;
                var ar = new bool[RowsNo];
                var threads = new List<Thread>();

                var errorInjector = new Thread(() =>
                {
                    Trace.TraceInformation("#");
                    localSession.SimulateSingleConnectionDown();

                    for (int i = 0; i < 50; i++)
                    {
                        Thread.Sleep(100);
                        Trace.TraceInformation("#");
                        localSession.SimulateSingleConnectionDown();
                    }
                });

                Trace.TraceInformation("Preparing...");

                for (int idx = 0; idx < RowsNo; idx++)
                {
                    int i = idx;
                    threads.Add(new Thread(() =>
                    {
                        try
                        {
                            Trace.TraceInformation("+");
                            var query = string.Format(@"INSERT INTO {0} (tweet_id, author, isok, body) VALUES ({1},'test{2}',{3},'body{2}');", tableName, Guid.NewGuid(), i, i%2 == 0 ? "false" : "true");
                            localSession.Execute(query, ConsistencyLevel.One);
                            ar[i] = true;
                            Thread.MemoryBarrier();
                        }
                        catch (Exception ex)
                        {
                            Trace.TraceInformation(ex.Message);
                            Trace.TraceInformation(ex.StackTrace);
                            ar[i] = true;
                            Thread.MemoryBarrier();
                        }
                    }));
                }

                errorInjector.Start();

                for (int idx = 0; idx < RowsNo; idx++)
                {
                    threads[idx].Start();
                }

                Trace.TraceInformation("Start!");

                var done = new HashSet<int>();
                while (done.Count < RowsNo)
                {
                    for (int i = 0; i < RowsNo; i++)
                    {
                        Thread.MemoryBarrier();
                        if (!done.Contains(i) && ar[i])
                        {
                            done.Add(i);
                            Trace.TraceInformation("-");
                        }
                    }
                }

                errorInjector.Join();

                for (int idx = 0; idx < RowsNo; idx++)
                {
                    threads[idx].Join(500);
                }

                Trace.TraceInformation("Inserted... now we are checking the count");

                var ret = localSession.Execute(string.Format(@"SELECT * from {0} LIMIT {1};", tableName, RowsNo + 100), ConsistencyLevel.Quorum);
                Assert.AreEqual(RowsNo, ret.GetRows().ToList().Count);

                localSession.Execute(string.Format(@"DROP TABLE {0};", tableName));
            }


            try
            {
                localSession.Execute(string.Format(@"DROP KEYSPACE {0};", keyspaceName));
            }
            catch
            {
            }
            localSession.Dispose();
        }

        [Test]
        public void InsertFireAndForget()
        {
            var keyspaceName = "kp_ifaf";
            var localSession = Cluster.Connect();
            localSession.CreateKeyspaceIfNotExists(keyspaceName);
            localSession.ChangeKeyspace(keyspaceName);

            localSession.WaitForSchemaAgreement(
                localSession.Execute(String.Format(TestUtils.CREATE_TABLE_ALL_TYPES, "sampletable")));

            var insertStatement = localSession.Prepare("INSERT INTO sampletable (id, blob_sample) VALUES (?, ?)");
            var rowLength = 10000;
            var rnd = new Random();
            var taskList = new List<Task<RowSet>>();
            for (var i = 0; i < rowLength; i++)
            {
                taskList.Add(localSession.ExecuteAsync(insertStatement.Bind(Guid.NewGuid(), new byte[1024 * rnd.Next(10)])));
            }

            var taskArray = taskList.ToArray();
            Task.WaitAny(taskArray);
            var rs = localSession.Execute("SELECT * FROM sampletable", ConsistencyLevel.One);
            Assert.IsTrue(rs.Count() > 0, "Table should contain 1 or more rows by now");
            
            Task.WaitAll(taskArray);
            rs = localSession.Execute("SELECT * FROM sampletable", ConsistencyLevel.Quorum);

            Assert.AreEqual(rowLength, rs.Count());
        }

        [Test]
        public void MassiveAsyncTest()
        {
            var localSession = Cluster.Connect();
            string keyspaceName = "kp_mat";
            localSession.WaitForSchemaAgreement(
                localSession.Execute(
                    string.Format(@"CREATE KEYSPACE {0} 
         WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }};"
                                  , keyspaceName)));
            localSession.ChangeKeyspace(keyspaceName);

            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                localSession.WaitForSchemaAgreement(
                    localSession.Execute(string.Format(@"CREATE TABLE {0}(
         tweet_id uuid,
         author text,
         body text,
         isok boolean,
         PRIMARY KEY(tweet_id))", tableName)));
            }
            catch (AlreadyExistsException)
            {
            }

            int RowsNo = 10000;

            var ar = new bool[RowsNo];

            var thr = new Thread(() =>
            {
                for (int i = 0; i < RowsNo; i++)
                {
                    Trace.TraceInformation("+");
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
                                             Thread.MemoryBarrier();
                                         }, null);
                }
            });

            thr.Start();

            var done = new HashSet<int>();
            while (done.Count < RowsNo)
            {
                for (int i = 0; i < RowsNo; i++)
                {
                    Thread.MemoryBarrier();
                    if (!done.Contains(i) && ar[i])
                    {
                        done.Add(i);
                        Trace.TraceInformation("-");
                    }
                }
            }

            thr.Join();

            Trace.TraceInformation("Inserted... now we are checking the count");

            var ret = localSession.Execute(string.Format(@"SELECT * from {0} LIMIT {1};", tableName, RowsNo + 100), ConsistencyLevel.Quorum);
            Assert.AreEqual(RowsNo, ret.GetRows().ToList().Count);

            try
            {
                localSession.Execute(string.Format(@"DROP TABLE {0};", tableName));
            }
            catch
            {
            }
            localSession.Dispose();
        }

        [Test]
        public void ShutdownAsyncTest()
        {
            var localSession = Cluster.Connect();
            string keyspaceName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();
            localSession.WaitForSchemaAgreement(
                localSession.Execute(
                    string.Format(@"CREATE KEYSPACE {0} WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }};", keyspaceName)));
            localSession.ChangeKeyspace(keyspaceName);

            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                localSession.WaitForSchemaAgreement(
                    localSession.Execute(string.Format(@"
                        CREATE TABLE {0}(
                        tweet_id uuid,
                        author text,
                        body text,
                        isok boolean,
                        PRIMARY KEY(tweet_id))", tableName)));
            }
            catch (AlreadyExistsException)
            {
            }

            int RowsNo = 10000;

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
                            Trace.TraceInformation("*");
                        }
                        finally
                        {
                            ar[tmpi] = true;
                            Thread.MemoryBarrier();
                        }
                    }, null);
                }
                catch (ObjectDisposedException)
                {
                    Trace.TraceInformation("!");
                    break;
                }
            }
            localSession.Dispose();
        }
    }
}