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
    [Dev.Ignore]
    public class ErrrorInjectionCompressionTests : ErrrorInjectionTestsBase
    {
        public ErrrorInjectionCompressionTests()
            : base(true)
        {
        }
    }

    [Dev.Ignore]
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
        CassandraCompressionType Compression
        {
            get
            {
                return _compression ? CassandraCompressionType.Snappy : CassandraCompressionType.NoCompression;
            }
        }

        public ErrrorInjectionTestsBase(bool compression)
        {
            _compression = compression;
        }

        CassandraSession Session;
        string Keyspace = "";
        IPEndPoint serverAddress;

        public void SetFixture(Dev.SettingsFixture setFix)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");

            var serverSp = setFix.Settings["CassandraServer"].Split(':');

            string ip = serverSp[0];
            int port = int.Parse(serverSp[1]);

            serverAddress = new IPEndPoint(IPAddress.Parse(ip), port);

            Session = new CassandraSession(new List<IPEndPoint>() { serverAddress, serverAddress, serverAddress, serverAddress, serverAddress }, this.Keyspace, this.Compression, 10 * 1000);
        }

        public void Dispose()
        {
            Session.Dispose();
        }

        [Fact]
        public void ParallelInsertTest()
        {
            Console.WriteLine("Compression is:"+(Compression== CassandraCompressionType.Snappy?"SNAPPY":"OFF"));

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
                Session.SimulateSingleConnectionDown(serverAddress);

                for (int i = 0; i < 100; i++)
                {
                    Thread.Sleep(1);
                    Console.Write("#");
                    Session.SimulateSingleConnectionDown(serverAddress);
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

                        Session.NonQuery(string.Format(@"INSERT INTO {0} (
         tweet_id,
         author,
         isok,
         body)
VALUES ({1},'test{2}','{3}','body{2}');", tableName, Guid.NewGuid().ToString(), i, i % 2 == 0 ? "false" : "true")
                       );
                        ar[i] = true;
                        Thread.MemoryBarrier();
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine(ex.Message);
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
                    if (readyCnt < RowsNo+(1))
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

            using (var ret = Session.Query(string.Format(@"SELECT * from {0} LIMIT {1};", tableName, RowsNo+100)))
            {
                Assert.Equal(RowsNo, ret.RowsCount);
            }

            Session.NonQuery(string.Format(@"DROP TABLE {0};", tableName));

            Session.NonQuery(string.Format(@"DROP KEYSPACE {0};", keyspaceName));
         }


        [Fact]
        public void executingPreparedStatementWithFakePrepareID()
        {
            Session.ChangeKeyspace("test");
            string tableName = "table" + Guid.NewGuid().ToString("N");
            Session.NonQuery(string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         value {1}
         );", tableName, "int"));

            byte[] preparedID;
            Metadata md;
            object[] toInsert = new object[1] { 1 };
            CommonBasicTests cbt = new CommonBasicTests();
            cbt.PrepareQuery(this.Session, string.Format("INSERT INTO {0}(tweet_id, value) VALUES ('{1}', ?);", tableName, toInsert[0].ToString()), out preparedID, out md);
            
            Session.ExecuteQuery(new byte[16], md, toInsert);           
        
            var rows = Session.Query(string.Format("SELECT * FROM {0};", tableName));
            rows.Dispose();
            Session.NonQuery(string.Format("DROP TABLE {0};", tableName));                     
        }
    }
}
