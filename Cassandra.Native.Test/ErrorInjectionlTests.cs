using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using System.Net;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using MyUTExt;

namespace Cassandra.Test
{
    public class ErrrorInjectionCompressionTests : ErrrorInjectionTestsBase
    {
        public ErrrorInjectionCompressionTests()
            : base(true)
        {
        }
    }

    public class ErrrorInjectionNoCompressionTests : ErrrorInjectionTestsBase
    {
        public ErrrorInjectionNoCompressionTests()
            : base(false)
        {
        }
    }

    public class ErrrorInjectionTestsBase : IUseFixture<Dev.SettingsFixture>, IDisposable
    {
        bool _compression = true;
        CompressionType Compression
        {
            get
            {
                return _compression ? CompressionType.Snappy : CompressionType.NoCompression;
            }
        }

        public ErrrorInjectionTestsBase(bool compression)
        {
            _compression = compression;
        }

        Session Session;
        string Keyspace = "";

        Action<string> Message;

        public void SetFixture(Dev.SettingsFixture setFix)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");

            var clusterb = Cluster.Builder().WithConnectionString(setFix.Settings["CassandraConnectionString"]);
            clusterb.WithReconnectionPolicy(new ConstantReconnectionPolicy(100));
            if (_compression)
                clusterb.WithCompression(CompressionType.Snappy);

            var rp = new RoundRobinPolicyWithReconnectionRetries(new ConstantReconnectionPolicy(100));
            rp.ReconnectionEvent += new EventHandler<RoundRobinPolicyWithReconnectionRetriesEventArgs>((s, ev) => {
                Console.Write("o");
                Thread.Sleep((int)ev.DelayMs);
            });
            clusterb.WithLoadBalancingPolicy(rp);
            clusterb.WithConnectionTimeout(60*1000);
            var cluster = clusterb.Build();
            Session = cluster.Connect(this.Keyspace);

            Message = setFix.InfoMessage;
        }

        public void Dispose()
        {
            Session.Dispose();
        }

        [Fact]
        public void ParallelInsertTest()
        {
            Console.WriteLine("Compression is:" + (Compression == CompressionType.Snappy ? "SNAPPY" : "OFF"));

            string keyspaceName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();

            try
            {
                Session.Execute(
                string.Format(@"CREATE KEYSPACE {0} 
         WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};"
                    , keyspaceName));
            }
            catch(Exception ex) 
            {
            }

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
            }
            catch (AlreadyExistsException)
            {
            }
            Randomm rndm = new Randomm();
            int RowsNo = 1000;
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
VALUES ({1},'test{2}','{3}','body{2}');", tableName, Guid.NewGuid().ToString(), i, i % 2 == 0 ? "false" : "true")
                       );
                        ar[i] = true;
                        Thread.MemoryBarrier();
                    }
                    catch (Exception ex)
                    {
                        Message(ex.Message);
                        Message(ex.StackTrace);
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


//        [Fact]
//        public void executingPreparedStatementWithFakePrepareID()
//        {
//            Session.ChangeKeyspace("test");
//            string tableName = "table" + Guid.NewGuid().ToString("N");
//            Session.Execute(string.Format(@"CREATE TABLE {0}(
//         tweet_id uuid PRIMARY KEY,
//         value {1}
//         );", tableName, "int"));

//            byte[] preparedID;
//            Metadata md;
//            object[] toInsert = new object[1] { 1 };
//            CommonBasicTests cbt = new CommonBasicTests();
//            cbt.PrepareQuery(this.Session, string.Format("INSERT INTO {0}(tweet_id, value) VALUES ('{1}', ?);", tableName, toInsert[0].ToString()), out preparedID, out md);

//            Session.ExecuteQuery(new byte[16], md, toInsert);

//            var rows = Session.Execute(string.Format("SELECT * FROM {0};", tableName));
//            rows.Dispose();
//            Session.Execute(string.Format("DROP TABLE {0};", tableName));
//        }
    }
}
