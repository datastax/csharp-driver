using NUnit.Framework;
using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;

namespace Cassandra.IntegrationTests.Core
{
    [TestClass]
    public class Stress2Tests
    {
        private static long totalElapsedTime;
        private ISession Session;
        //        private HistogramMetric _readHistogram = metrics.Metrics.Histogram(typeof(DatastaxDriverTest), "Reads");
        //        private HistogramMetric _writeHistogram = metrics.Metrics.Histogram(typeof(DatastaxDriverTest), "Writes");


        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
        }

        [TestCleanup]
        public void Dispose()
        {
        }

        public Builder initialize(string datacenter, long constDelayMS, int queryTimeout,
                                  int coreConnectionPerHost, int maxConnectionPerHost, int cpCon)
        {
            var lbp = new RetryLoadBalancingPolicy(new DCAwareRoundRobinPolicy(datacenter),
                                                   new ExponentialReconnectionPolicy(constDelayMS, constDelayMS*100));
            //lbp.ReconnectionEvent += new EventHandler<RetryLoadBalancingPolicyEventArgs>((s, ea) => { Console.Write("~(" + ea.DelayMs + ")"); Thread.Sleep((int)ea.DelayMs); });
            Builder cassandraBuilder = Cluster.Builder()
                                              .WithLoadBalancingPolicy(lbp)
                                              .WithReconnectionPolicy(new ConstantReconnectionPolicy(constDelayMS))
                                              .WithRetryPolicy(new DefaultRetryPolicy())
//                   .WithQueryTimeout(queryTimeout)
                ;
            cassandraBuilder.PoolingOptions.SetCoreConnectionsPerHost(HostDistance.Local, coreConnectionPerHost);
            cassandraBuilder.PoolingOptions.SetMaxConnectionsPerHost(HostDistance.Local, maxConnectionPerHost);
            cassandraBuilder.PoolingOptions.SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, cpCon);
            cassandraBuilder.PoolingOptions.SetMinSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 0);
            return cassandraBuilder;
            //            ConsoleReporter consoleReport = new ConsoleReporter();
            //            consoleReport.Start(10, metrics.TimeUnit.Seconds);
        }

        private void parallelInsertTestGeneric(int nThreads, int cpCon)
        {
            CCMBridge.ReusableCCMCluster.Setup(1);

            string datacenter = "datacenter1";
            long constDelayMS = 100;
            int queryTimeout = 50000;
            int coreConnectionPerHost = 64;
            int maxConnectionPerHost = 64;

            CCMBridge.ReusableCCMCluster.Build(initialize(datacenter, constDelayMS, queryTimeout, coreConnectionPerHost, maxConnectionPerHost, cpCon));
            Session = CCMBridge.ReusableCCMCluster.Connect("tester");

            Console.WriteLine("Start parallel insert test (" + nThreads + " , " + cpCon + ")");
            string keyspaceName = "testkeyspace2" + nThreads + "x" + cpCon;
//            Console.WriteLine("Creating keyspace");
            Session.WaitForSchemaAgreement(
                Session.Execute(
                    string.Format(@"CREATE KEYSPACE {0} 
                        WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }};"
                                  , keyspaceName)));
            Session.ChangeKeyspace(keyspaceName);
            string tableName = "testtable";
            try
            {
                Session.WaitForSchemaAgreement(
                    Session.Execute(string.Format(@"CREATE TABLE {0}(
                         tweet_id int,
                         author text,
                         body text,
                         isok boolean,
                         PRIMARY KEY(tweet_id))", tableName)));
            }
            catch (AlreadyExistsException)
            {
            }
//            Console.WriteLine("Prepare statement");
            PreparedStatement insertPrep = Session.Prepare("INSERT INTO " + tableName + @" (
                tweet_id,
                author,
                isok,
                body)
                VALUES (?,?,?,?);");
//            Console.WriteLine("Insert Values");

            int RowsNo = 100000;
            int stepSize = RowsNo/nThreads;

            var tasks = new Thread[nThreads];
            var monit = new object();
            int readyCnt = 0;


            for (int i = 0; i < nThreads; i++)
            {
                int startIndex = i*stepSize;
                int endIndex = (i + 1)*stepSize;
                tasks[i] = new Thread(() =>
                {
                    lock (monit)
                    {
                        readyCnt++;
                        Monitor.Wait(monit);
                    }
                    insertRange(insertPrep, startIndex, endIndex);
                });
                tasks[i].Start();
            }

            Stopwatch t = Stopwatch.StartNew();

            lock (monit)
            {
                while (true)
                {
                    if (readyCnt < nThreads)
                    {
                        Monitor.Exit(monit);
                        Thread.Sleep(100);
                        Monitor.Enter(monit);
                    }
                    else
                    {
                        t.Restart();
                        Monitor.PulseAll(monit);
                        break;
                    }
                }
            }

            foreach (Thread task in tasks)
                task.Join();

            Console.WriteLine("Avg query response time " + totalElapsedTime/(double) RowsNo + "ms");
            Console.WriteLine("Avg single insert time " + t.ElapsedMilliseconds/(double) RowsNo + "ms");

            //using (var res = Session.Execute(string.Format(@"SELECT COUNT(*) FROM {0} LIMIT {1}", tableName, RowsNo + 100), ConsistencyLevel.Quorum))
            //{
            //    var cnt = res.GetRows().FirstOrDefault().GetValue<long>(0);
            //    Assert.Equal(RowsNo, cnt);
            //} 

            Session.Execute(string.Format(@"DROP TABLE {0};", tableName));
            Session.Execute(string.Format(@"DROP KEYSPACE {0};", keyspaceName));

            CCMBridge.ReusableCCMCluster.Drop();
        }

        public void insertRange(PreparedStatement prepStatement, int startIndex, int endIndex)
        {
            Thread.Sleep(500);
            Console.WriteLine("Inserting values from " + startIndex + " to " + endIndex);
            Stopwatch t = Stopwatch.StartNew();
            for (int idx = startIndex; idx < endIndex; idx++)
            {
                try
                {
                    Session.Execute(
                        prepStatement
                            .Bind(new object[]
                            {
                                idx,
                                "author" + idx,
                                idx%2 == 0 ? false : true,
                                "body" + idx
                            }).SetConsistencyLevel(ConsistencyLevel.Quorum));
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error while inserting " + ex.StackTrace);
                }
            }
            long elapsedMs = t.ElapsedMilliseconds;
            Interlocked.Add(ref totalElapsedTime, elapsedMs);
            long avg = elapsedMs/(endIndex - startIndex);
            Console.WriteLine("... Inserted values from " + startIndex + " to " + endIndex + " avg:" + avg + "ms");
        }

        [Test]
        public void test1()
        {
            parallelInsertTestGeneric(10, 1);
            parallelInsertTestGeneric(50, 1);
            parallelInsertTestGeneric(100, 1);
            parallelInsertTestGeneric(10, 50);
            parallelInsertTestGeneric(50, 50);
            parallelInsertTestGeneric(100, 50);
            parallelInsertTestGeneric(150, 50);
            parallelInsertTestGeneric(300, 50);
            parallelInsertTestGeneric(500, 50);
            parallelInsertTestGeneric(10, 100);
            parallelInsertTestGeneric(50, 100);
            parallelInsertTestGeneric(100, 100);
            parallelInsertTestGeneric(150, 100);
            parallelInsertTestGeneric(300, 100);
            parallelInsertTestGeneric(500, 100);
        }
    }
}