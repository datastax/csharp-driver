using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using System.Net;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using MyUTExt;

namespace Cassandra.Native.Test
{
    public class PoolTests : IUseFixture<Dev.SettingsFixture>, IDisposable
    {
        bool _compression = false;
        CassandraCompressionType Compression
        {
            get
            {
                return _compression ? CassandraCompressionType.Snappy : CassandraCompressionType.NoCompression;
            }
        }
        CassandraSession Session;
        string Keyspace = "";

        public void SetFixture(Dev.SettingsFixture setFix)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");

            var serverSp = setFix.Settings["CassandraServer"].Split(':');

            string ip = serverSp[0];
            int port = int.Parse(serverSp[1]);

            var serverAddress = new IPEndPoint(IPAddress.Parse(ip), port);

            Session = new CassandraSession(new List<IPEndPoint>() { serverAddress }, this.Keyspace, this.Compression, 10 * 60 * 1000);
        }

        public void Dispose()
        {
            Session.Dispose();
        }

        [Fact]
        public void ParallelInsertTest()
        {
            string keyspaceName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();

            Session.NonQuery(
            string.Format(@"CREATE KEYSPACE {0} 
         WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};"
                , keyspaceName));

            Session.ChangeKeyspace(keyspaceName);

            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            Session.NonQuery(string.Format(@"CREATE TABLE {0}(
         tweet_id uuid,
         author text,
         body text,
         isok boolean,
         PRIMARY KEY(tweet_id))", tableName));
            Randomm rndm = new Randomm();
            int RowsNo = 1000;
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

                        ar[i] = Session.BeginNonQuery(string.Format(@"INSERT INTO {0} (
         tweet_id,
         author,
         isok,
         body)
VALUES ({1},'test{2}','{3}','body{2}');", tableName, Guid.NewGuid().ToString(), i, i % 2 == 0 ? "false" : "true")
                       , null, null);
                        Thread.MemoryBarrier();
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

            Console.WriteLine();
            Console.WriteLine("Start!");

            HashSet<int> done = new HashSet<int>();
            while (done.Count < RowsNo)
            {
                for (int i = 0; i < RowsNo; i++)
                {
                    Thread.MemoryBarrier();
                    if (!done.Contains(i) &&  ar[i] != null)
                    {
                        if (ar[i].AsyncWaitHandle.WaitOne(10))
                        {
                            Session.EndNonQuery(ar[i]);
                            done.Add(i);
                            Console.Write("-");
                        }
                    }
                }
            }

            Console.WriteLine();
            Console.WriteLine("Inserted... now we are checking the count");

            using (var ret = Session.Query(string.Format(@"SELECT * from {0} LIMIT {1};", tableName, RowsNo+100)))
            {
                Assert.Equal(RowsNo, ret.RowsCount);
            }
           
            Session.NonQuery(string.Format(@"DROP TABLE {0};", tableName));

            Session.NonQuery(string.Format(@"DROP KEYSPACE {0};", keyspaceName));

            for (int idx = 0; idx < RowsNo; idx++)
            {
                threads[idx].Join();
            }
         }
    }
}
