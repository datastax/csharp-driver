using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif
using System.Text;
using System.Globalization;

namespace Cassandra.MSTest
{
    [TestClass]
    public partial class AdvancedTests
    {
        Cluster Cluster;
        Session Session;
        CCMBridge.CCMCluster CCMCluster;

        public AdvancedTests()
        {
        }
                
        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
   	     var clusterb = Cluster.Builder();
            clusterb.WithReconnectionPolicy(new ConstantReconnectionPolicy(100));

            var rp = new RoundRobinPolicyWithReconnectionRetries(new ConstantReconnectionPolicy(100));
            rp.ReconnectionEvent += new EventHandler<RoundRobinPolicyWithReconnectionRetriesEventArgs>((s, ev) =>
            {
                Console.Write("o");
                System.Threading.Thread.Sleep((int)ev.DelayMs);
            });
            clusterb.WithLoadBalancingPolicy(rp);
            clusterb.WithQueryTimeout(60 * 1000);

            CCMCluster = CCMBridge.CCMCluster.Create(2, clusterb);
            Session = CCMCluster.Session;
            Cluster = CCMCluster.Cluster;
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMCluster.Discard();
        }


        public void parallelInsertTest()
        {            
            string keyspaceName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();

            Session.Execute(
            string.Format(@"CREATE KEYSPACE {0} 
         WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};"
                , keyspaceName));

            Session.Cluster.WaitForSchemaAgreement();

            Session.ChangeKeyspace(keyspaceName);

            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                Session.Execute(string.Format(@"CREATE TABLE {0}(
         tweet_id uuid,
         author text,
         body text,
         isok boolean,
         PRIMARY KEY(tweet_id))", tableName));

                Session.Cluster.WaitForSchemaAgreement();
            }
            catch (AlreadyExistsException)
            {
            }
            Randomm rndm = new Randomm();
            int RowsNo = 300;
            IAsyncResult[] ar = new IAsyncResult[RowsNo];
            List<Thread> threads = new List<Thread>();
            object monit = new object();
            int readyCnt = 0;
            Console.WriteLine();
            Console.WriteLine("Preparing...");

            for (int idx = 0; idx < RowsNo; idx++)
            {

                var i = idx;
                threads.Add(new Thread(() =>
                {
                    try
                    {
                        Console.Write("+");
                        lock (monit)
                        {
                            readyCnt++;
                            Monitor.Wait(monit);
                        }

                        ar[i] = Session.BeginExecute(string.Format(@"INSERT INTO {0} (
         tweet_id,
         author,
         isok,
         body)
VALUES ({1},'test{2}',{3},'body{2}');", tableName, Guid.NewGuid().ToString(), i, i % 2 == 0 ? "false" : "true")
                       , ConsistencyLevel.Default, null, null);
                        Thread.MemoryBarrier();
                    }
                    catch
                    {
                        Console.Write("@");
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

            Console.WriteLine();
            Console.WriteLine("Start!");

            HashSet<int> done = new HashSet<int>();
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
                                Session.EndExecute(ar[i]);
                            }
                            catch
                            {
                                Console.Write("!");
                            }
                            done.Add(i);
                            Console.Write("-");
                        }
                    }
                }
            }

            Console.WriteLine();
            Console.WriteLine("Inserted... now we are checking the count");

            using (var ret = Session.Execute(string.Format(@"SELECT * from {0} LIMIT {1};", tableName, RowsNo + 100)))
            {
                Assert.Equal(RowsNo, ret.RowsCount);
            }

            Session.Execute(string.Format(@"DROP TABLE {0};", tableName));

            Session.Execute(string.Format(@"DROP KEYSPACE {0};", keyspaceName));

            for (int idx = 0; idx < RowsNo; idx++)
            {
                threads[idx].Join();
            }
        }

        
        public void ErrorInjectionInParallelInsertTest()
        {
            string keyspaceName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();
                Session.Execute(
                string.Format(@"CREATE KEYSPACE {0} 
         WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};"
                    , keyspaceName));
                Session.Cluster.WaitForSchemaAgreement();
            Session.ChangeKeyspace(keyspaceName);

            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            try
            {
                Session.Execute(string.Format(@"CREATE TABLE {0}(
         tweet_id uuid,
         author text,
         body text,
         isok boolean,
         PRIMARY KEY(tweet_id))", tableName));
                Session.Cluster.WaitForSchemaAgreement();
            }
            catch (AlreadyExistsException)
            {
            }
            Randomm rndm = new Randomm();
            int RowsNo = 300;
            bool[] ar = new bool[RowsNo];
            List<Thread> threads = new List<Thread>();
            object monit = new object();
            int readyCnt = 0;

            Thread errorInjector = new Thread(() =>
            {
                lock (monit)
                {
                    readyCnt++;
                    Monitor.Wait(monit);
                }
                Thread.Sleep(5);
                Console.Write("#");
                Session.SimulateSingleConnectionDown();

                for (int i = 0; i < 100; i++)
                {
                    Thread.Sleep(1);
                    Console.Write("#");
                    Session.SimulateSingleConnectionDown();
                }
            });

            Console.WriteLine();
            Console.WriteLine("Preparing...");

            for (int idx = 0; idx < RowsNo; idx++)
            {

                var i = idx;
                threads.Add(new Thread(() =>
                {
                    try
                    {
                        Console.Write("+");
                        lock (monit)
                        {
                            readyCnt++;
                            Monitor.Wait(monit);
                        }

                        Session.Execute(string.Format(@"INSERT INTO {0} (
         tweet_id,
         author,
         isok,
         body)
VALUES ({1},'test{2}',{3},'body{2}');", tableName, Guid.NewGuid().ToString(), i, i % 2 == 0 ? "false" : "true")
                       );
                        ar[i] = true;
                        Thread.MemoryBarrier();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.StackTrace);
                        ar[i] = true;
                        Thread.MemoryBarrier();
                    }

                }));
            }

            for (int idx = 0; idx < RowsNo; idx++)
            {
                threads[idx].Start();
            }
            errorInjector.Start();

            lock (monit)
            {
                while (true)
                {
                    if (readyCnt < RowsNo + (1))
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

            Console.WriteLine();
            Console.WriteLine("Start!");

            HashSet<int> done = new HashSet<int>();
            while (done.Count < RowsNo)
            {
                for (int i = 0; i < RowsNo; i++)
                {
                    Thread.MemoryBarrier();
                    if (!done.Contains(i) && ar[i])
                    {
                        done.Add(i);
                        Console.Write("-");
                    }
                }
            }

            for (int idx = 0; idx < RowsNo; idx++)
            {
                threads[idx].Join();
            }

            errorInjector.Join();

            Console.WriteLine();
            Console.WriteLine("Inserted... now we are checking the count");

            using (var ret = Session.Execute(string.Format(@"SELECT * from {0} LIMIT {1};", tableName, RowsNo + 100)))
            {
                Assert.Equal(RowsNo, ret.RowsCount);
            }

            try
            {
                Session.Execute(string.Format(@"DROP TABLE {0};", tableName));
            }
            catch { }

            try
            {
                Session.Execute(string.Format(@"DROP KEYSPACE {0};", keyspaceName));
            }
            catch { }
        }


    }
}
