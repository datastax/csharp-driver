using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using System.Threading;
using System.Net;
using Cassandra.Native;
 

namespace MyUTExt
{
    public class CommonBasicTests : IUseFixture<Dev.SettingsFixture>, IDisposable
    {
        bool _compression;
        CassandraCompressionType Compression
        {
            get
            {
                return _compression ? CassandraCompressionType.Snappy : CassandraCompressionType.NoCompression;
            }
        }
        bool _buffering;
        BufferingMode Buffering
        {
            get
            {
                return _buffering ? BufferingMode.FrameBuffering : BufferingMode.NoBuffering;
            }
        }        

        public CommonBasicTests(bool compression, bool buffering)
        {
            this._compression = compression;
            this._buffering = buffering;;
        }        
        
        CassandraManager manager;

        public void SetFixture(Dev.SettingsFixture setFix)
        {
            var serverSp = setFix.Settings["CassandraServer"].Split(':');

            string ip = serverSp[0];
            int port = int.Parse(serverSp[1]);

            var serverAddress = new IPEndPoint(IPAddress.Parse(ip), port);

            manager = new CassandraManager(new List<IPEndPoint>() { serverAddress },this.Compression, this.Buffering);
        }

        public void Dispose()
        {
            manager.Dispose();
        }



        public void ProcessOutput(IOutput ret, object expectedValue = null)
        {
            using (ret)
            {
                if (ret is OutputError)
                {
                    if (expectedValue is OutputError)
                        Dev.Assert.True(true, "CQL Error [" + (ret as OutputError).CassandraErrorType.ToString() + "] " + (ret as OutputError).Message);
                    else
                        Dev.Assert.True(false, "CQL Error [" + (ret as OutputError).CassandraErrorType.ToString() + "] " + (ret as OutputError).Message);
                }
                else if (ret is OutputPrepared)
                {
                    Console.WriteLine("CQL> Prepared:\t" + (ret as OutputPrepared).QueryID);
                }
                else if (ret is OutputSetKeyspace)
                {
                    if (expectedValue != null)
                        Dev.Assert.Equal((string)expectedValue, (ret as OutputSetKeyspace).Value);
                    Console.WriteLine("CQL> SetKeyspace:\t" + (ret as OutputSetKeyspace).Value);
                }
                else if (ret is OutputVoid)
                {
                    if (expectedValue != null)
                        Dev.Assert.True(false, string.Format("\n Received output:  {0} \n Expected output:  {1}", ret.ToString(), expectedValue.ToString()));
                    else
                        Console.WriteLine("CQL> (OK)");
                }
                else if (ret is OutputRows)
                {
                    if (expectedValue != null)
                        Dev.Assert.Equal((int)expectedValue, (ret as OutputRows).Rows);
                    CqlRowsPopulator rowPopulator = new CqlRowsPopulator(ret as OutputRows);
                    rowPopulator.PrintTo(Console.Out);
                    Console.WriteLine("CQL> Done.");
                }
                else
                {
                    Dev.Assert.True(false, "Unexpected IOutput: " + ret.GetType().FullName);
                }
            }
        }

        public void ExecuteSync(CassandraManagedConnection conn, string query, string messageInstead = null, object expectedValue = null)
        {
            if (messageInstead != null)
                Console.WriteLine("CQL<\t" + messageInstead);
            else
                Console.WriteLine("CQL< Query:\t" + query);
            var ret = conn.ExecuteQuery(query);
            ProcessOutput(ret, expectedValue);
        }

        public CassandraManagedConnection ConnectToTestServer()
        {
            return manager.Connect();
        }

        public void Test()
        {
            var conn = ConnectToTestServer();
            using (conn)
            {
                string keyspaceName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();

                ExecuteSync(conn, string.Format(@"CREATE KEYSPACE {0} 
         WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }};"
                    , keyspaceName));

                ExecuteSync(conn, string.Format(@"USE {0}", keyspaceName));
                string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
                ExecuteSync(conn, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid,
         author text,
         body text,
         isok boolean,
		 fval float,
		 dval double,
         PRIMARY KEY(tweet_id))", tableName));

                Randomm rndm = new Randomm();
                StringBuilder longQ = new StringBuilder();
                longQ.AppendLine("BEGIN BATCH ");

                int RowsNo = 5000;
                for (int i = 0; i < RowsNo; i++)
                {
                    longQ.AppendFormat(@"INSERT INTO {0} (
         tweet_id,
         author,
         isok,
         body,
		 fval,
		 dval)
VALUES ({1},'test{2}','{3}','body{2}','{4}','{5}');", tableName, Guid.NewGuid().ToString(), i, i % 2 == 0 ? "false" : "true", rndm.NextSingle(), rndm.NextDouble());
                }
                longQ.AppendLine("APPLY BATCH;");
                ExecuteSync(conn, longQ.ToString(), "Inserting...");
                ExecuteSync(conn, string.Format(@"SELECT * from {0} LIMIT 5000;", tableName), null, RowsNo);
                ExecuteSync(conn, string.Format(@"DROP TABLE {0};", tableName));

                ExecuteSync(conn, string.Format(@"DROP KEYSPACE {0};", keyspaceName));
            }
        }
       

        public void ExceedingCassandraType(Type toExceed, Type toExceedWith, bool shouldPass = true)
        {
            string cassandraDataTypeName = convertTypeNameToCassandraEquivalent(toExceed);
            CassandraManagedConnection conn = ConnectToTestServer();
            using (conn)
            {
                ExecuteSync(conn, "USE test");
                string tableName = "table" + Guid.NewGuid().ToString("N");
                ExecuteSync(conn, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         label text,
         number {1}
         );", tableName, cassandraDataTypeName));

                ExecuteSync(conn, string.Format("INSERT INTO {0}(tweet_id, label, number) VALUES ({1}, '{2}', '{3}');", tableName, Guid.NewGuid().ToString(), "Minimum", toExceedWith.GetField("MinValue").GetValue(this)), null, shouldPass ? null : new OutputInvalid());
                ExecuteSync(conn, string.Format("INSERT INTO {0}(tweet_id, label, number) VALUES ({1}, '{2}', '{3}');", tableName, Guid.NewGuid().ToString(), "Maximum", toExceedWith.GetField("MaxValue").GetValue(this)), null, shouldPass ? null : new OutputInvalid()); 
                ExecuteSync(conn, string.Format("SELECT * FROM {0};", tableName));
                ExecuteSync(conn, string.Format("DROP TABLE {0};", tableName));
            }
        }
        private string convertTypeNameToCassandraEquivalent(Type t)
        {
            switch (t.Name)
            {
                case "Int32":
                    return "int";

                case "Int64":
                    return "bigint";

                case "Single":
                    return "float";

                case "Double":
                    return "double";

                default:
                    throw new InvalidOperationException();                    
            }
        }
        
        public void inputingSingleValue(Type tp)
        {            
            Randomm rndm = new Randomm();
            string cassandraDataTypeName = convertTypeNameToCassandraEquivalent(tp);                        
            CassandraManagedConnection conn = ConnectToTestServer();
            using (conn)
            {
                ExecuteSync(conn, "USE test");
                string tableName = "table" + Guid.NewGuid().ToString("N");
                ExecuteSync(conn, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         number {1}
         );", tableName, cassandraDataTypeName));
               
                ExecuteSync(conn, string.Format("INSERT INTO {0}(tweet_id,number) VALUES ({1}, '{2}');", tableName, Guid.NewGuid().ToString(), rndm.GetType().GetMethod("Next" + tp.Name).Invoke(rndm, new object[] { })), null);
                ExecuteSync(conn, string.Format("SELECT * FROM {0};", tableName), null, 1);
                ExecuteSync(conn, string.Format("DROP TABLE {0};", tableName));
            }
        }
        
        public void ConnectionsTest()
        {
            int cnt = 10;

            for (int j = 0; j < cnt; j++)
            {
                var conns = new CassandraManagedConnection[cnt];
                try
                {
                    for (int i = 0; i < cnt; i++)
                        conns[i] = manager.Connect();

                    for (int i = 0; i < cnt; i++)
                        conns[i].ExecuteQuery("USE unknknk");
                }
                finally
                {
                    for (int i = 0; i < cnt; i++)
                        if (conns[i] != null)
                            conns[i].Dispose();
                }
            }
        }

        public void TimestampTest()
        {                        
            CassandraManagedConnection conn = ConnectToTestServer();
            using (conn)
            {
                ExecuteSync(conn, "USE test");
                string tableName = "table" + Guid.NewGuid().ToString("N");
                ExecuteSync(conn, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         ts timestamp
         );", tableName));                
                
                ExecuteSync(conn, string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid().ToString(), "2011-02-03 04:05+0000"), null);
                ExecuteSync(conn, string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid().ToString(), 220898707200000), null);
                ExecuteSync(conn, string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid().ToString(), 0), null);

                ExecuteSync(conn, string.Format("SELECT * FROM {0};", tableName), null);
                ExecuteSync(conn, string.Format("DROP TABLE {0};", tableName));
            }
        }
    }
}
