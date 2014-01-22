using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MyTest;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Threading;
using System.Globalization;

namespace Cassandra.MSTest
{
    [TestClass]
    public class Stress1Tests
    {
        Session Session;
//        private HistogramMetric _readHistogram = metrics.Metrics.Histogram(typeof(DatastaxDriverTest), "Reads");
//        private HistogramMetric _writeHistogram = metrics.Metrics.Histogram(typeof(DatastaxDriverTest), "Writes");


        [TestInitialize]
        public void SetFixture()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            Diagnostics.CassandraPerformanceCountersEnabled = true;
            Diagnostics.CassandraStackTraceIncluded = true;
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");


        }

        [TestCleanup]
        public void Dispose()
        {
        }

        public class MyRetryRetryPolicy : IRetryPolicy
        {
            public static readonly MyRetryRetryPolicy Instance = new MyRetryRetryPolicy();

            private MyRetryRetryPolicy() { }

            public RetryDecision OnReadTimeout(Query query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
            {
                Console.WriteLine("Read Timeout");
                return null;
            }

            public RetryDecision OnWriteTimeout(Query query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
            {
                Console.WriteLine("Write Timeout");
                return null;
            }

            public RetryDecision OnUnavailable(Query query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
            {
                Console.WriteLine("Unavailable");
                return null;
            }
        }

        public Builder initialize(string datacenter, long constDelayMS, int queryTimeout, 
            int coreConnectionPerHost, int maxConnectionPerHost,int cpCon)
        {

            var lbp = new RetryLoadBalancingPolicy(new DCAwareRoundRobinPolicy(datacenter), new ExponentialReconnectionPolicy(constDelayMS, constDelayMS * 100));
            lbp.ReconnectionEvent += new EventHandler<RetryLoadBalancingPolicyEventArgs>((s, ea) => { Console.Write("~(" + ea.DelayMs + ")"); Thread.Sleep((int)ea.DelayMs); });
            Builder cassandraBuilder = Cluster.Builder()
                .WithLoadBalancingPolicy(lbp)// new DCAwareRoundRobinPolicy(datacenter))
                    .WithReconnectionPolicy(new ConstantReconnectionPolicy(constDelayMS))
                    .WithRetryPolicy(MyRetryRetryPolicy.Instance)
                    .WithQueryTimeout(queryTimeout)
                    .WithCompression(CompressionType.NoCompression);

            cassandraBuilder.PoolingOptions.SetCoreConnectionsPerHost(HostDistance.Local, coreConnectionPerHost);
            cassandraBuilder.PoolingOptions.SetMaxConnectionsPerHost(HostDistance.Local, maxConnectionPerHost);
            cassandraBuilder.PoolingOptions.SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, cpCon);
            cassandraBuilder.PoolingOptions.SetMinSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 10);

            return cassandraBuilder;
//            ConsoleReporter consoleReport = new ConsoleReporter();
//            consoleReport.Start(10, metrics.TimeUnit.Seconds);
        }

        public void parallelInsertTestGeneric(int nThreads, int cpCon)
        {
            CCMBridge.ReusableCCMCluster.Setup(3, 0, true);
            string datacenter = "datacenter1";
            long constDelayMS = 500;
            int queryTimeout = Timeout.Infinite;
            int coreConnectionPerHost = 2;
            int maxConnectionPerHost = 8;

            CCMBridge.CCMCluster  ccmCluster = CCMBridge.CCMCluster.Create(3, initialize(datacenter, constDelayMS, queryTimeout, coreConnectionPerHost, maxConnectionPerHost, cpCon));

            CCMBridge.ReusableCCMCluster.Build(initialize(datacenter, constDelayMS, queryTimeout, coreConnectionPerHost, maxConnectionPerHost,cpCon));
            Session = ccmCluster.Session;

            Console.WriteLine("Start parallel insert test (" + nThreads + " , " + cpCon + ")");
            string keyspaceName = "testkeyspace1" + nThreads + "x" + cpCon;
//            Console.WriteLine("Creating keyspace");
            try
            {
                Session.WaitForSchemaAgreement(
                     Session.Execute(
                        string.Format(@"CREATE KEYSPACE {0} 
                        WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }};"
                            , keyspaceName)));
            }
            catch (AlreadyExistsException) { }
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
            { }
//            Console.WriteLine("Prepare statement");
            PreparedStatement insertPrep = Session.Prepare("INSERT INTO " + tableName + @" (
                tweet_id,
                author,
                isok,
                body)
                VALUES (?,?,?,?);");
        //    Console.WriteLine("Insert Values");

            int RowsNo = 100000;
            int stepSize = RowsNo / nThreads;

            Thread[] tasks = new Thread[nThreads];
            object monit = new object();
            int readyCnt = 0;


            for (int i = 0; i < nThreads; i++)
            {
                var startIndex = i * stepSize;
                var endIndex = (i + 1) * stepSize;
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

            foreach (var task in tasks)
                task.Join();

            Console.WriteLine("Avg query response time " + ((double)totalElapsedTime) / (double)RowsNo + "ms");
            Console.WriteLine("Avg single insert time " + ((double)t.ElapsedMilliseconds) / (double)RowsNo + "ms");

            RETRY:
            try
            {
                using (var res = Session.Execute(string.Format(@"SELECT COUNT(*) FROM {0} LIMIT {1}", tableName, RowsNo + 100), ConsistencyLevel.Quorum))
                {
                    var cnt = res.GetRows().FirstOrDefault().GetValue<long>(0);
                    Assert.Equal(RowsNo, cnt);
                }
            }
            catch (Exception)
            {
                goto RETRY;
            }

            Session.Execute(string.Format(@"DROP TABLE {0};", tableName));
            Session.Execute(string.Format(@"DROP KEYSPACE {0};", keyspaceName));
            CCMBridge.ReusableCCMCluster.Drop();            
            ccmCluster.Discard();
        }

        static long totalElapsedTime = 0;

        public void insertRange(PreparedStatement prepStatement, int startIndex, int endIndex)
        {
            Console.WriteLine("Inserting values from " + startIndex + " to " + endIndex);
            Stopwatch t = Stopwatch.StartNew();
            int pendingJobs = 0;
            for (int idx = startIndex; idx < endIndex; idx++)
            {
                Interlocked.Increment(ref pendingJobs);

                Session.BeginExecute(
                        prepStatement
                        .Bind(new object[] { 
                            idx,
                            "author"+idx,
                            idx % 2 == 0 ? false : true,
                            "body"+idx
                        }).SetConsistencyLevel(ConsistencyLevel.One), (ar) =>
                        {
                            try
                            {
                                Session.EndExecute(ar);
                            }
                            catch (NoHostAvailableException ex)
                            {
                                foreach( var node in ex.Errors)
                                {
                                    Console.WriteLine("Error on " + node.Key.ToString());
                                    var expts = node.Value;
                                    foreach(var excpt in expts)
                                        Console.WriteLine("      Error while inserting " + excpt.StackTrace + "\n!!!MESSAGE!!!\n" + excpt.Message);
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Error while inserting " + ex.StackTrace + "\n!!!MESSAGE!!!\n" + ex.Message );
                            }
                            finally
                            {
                                Interlocked.Decrement(ref pendingJobs);
                            }

                        }, null);
            }

            while (true)
            {
                Thread.MemoryBarrier();
                if (pendingJobs == 0)
                    break;
                Thread.Sleep(10);
            }
            var elapsedMs = t.ElapsedMilliseconds;
            //                _writeHistogram.Update(elapsedMs);
            Interlocked.Add(ref totalElapsedTime, elapsedMs);
            var avg = elapsedMs / (endIndex - startIndex);
           Console.WriteLine("... Inserted values from " + startIndex + " to " + endIndex + " avg:" + avg +"ms");
        }

        [TestMethod]
        [Stress]
        [WorksForMe]
        public void test1()
        {
            parallelInsertTestGeneric(10, 100);
            parallelInsertTestGeneric(50, 100);
            parallelInsertTestGeneric(100, 100);
            parallelInsertTestGeneric(150, 100);
            parallelInsertTestGeneric(300, 100);
            parallelInsertTestGeneric(500, 100);

            parallelInsertTestGeneric(10, 50);
            parallelInsertTestGeneric(50, 50);
            parallelInsertTestGeneric(100, 50);
            parallelInsertTestGeneric(150, 50);
            parallelInsertTestGeneric(300, 50);
            parallelInsertTestGeneric(500, 50);
        }
    }
}
