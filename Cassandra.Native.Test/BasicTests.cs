using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using System.Threading;
using System.Net;

namespace Cassandra.Native.Test
{
    public class BasicTests : IUseFixture<Dev.SettingsFixture>, IDisposable   
    {
        public BasicTests()
        {
        }

        CassandraManager manager;

        public void SetFixture(Dev.SettingsFixture setFix)
        {
            var serverSp = setFix.Settings["CassandraServer"].Split(':');

            string ip = serverSp[0];
            int port = int.Parse(serverSp[1]);

            var serverAddress = new IPEndPoint(IPAddress.Parse(ip), port);

            manager = new CassandraManager(new List<IPEndPoint>() { serverAddress });
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
                if(expectedValue is OutputError)
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

        public void Test()
        {
            using (var conn = manager.Connect())
            {
                string keyspaceName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();

                ExecuteSync(conn, string.Format(@"CREATE KEYSPACE {0} 
         WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }};"
                    ,keyspaceName));

                ExecuteSync( conn, string.Format(@"USE {0}", keyspaceName));
                string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
                ExecuteSync( conn, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid,
         author text,
         body text,
         isok boolean,
         PRIMARY KEY(tweet_id))", tableName));

                StringBuilder longQ = new StringBuilder();
                longQ.AppendLine("BEGIN BATCH ");

                int RowsNo = 2000;
                for (int i = 0; i < RowsNo; i++)
                {
                    longQ.AppendFormat(@"INSERT INTO {0} (
         tweet_id,
         author,
         isok,
         body)
         VALUES ({1},'test{2}','{3}','body{2}');", tableName, Guid.NewGuid().ToString(), i, i % 2 == 0 ? "false" : "true");
                }
                longQ.AppendLine("APPLY BATCH;");
                ExecuteSync(conn, longQ.ToString(), "Inserting...");
                ExecuteSync(conn, string.Format(@"SELECT * from {0} LIMIT 10000;", tableName), null, RowsNo);
                ExecuteSync( conn, string.Format(@"DROP TABLE {0};", tableName));

                ExecuteSync(conn, string.Format(@"DROP KEYSPACE {0};", keyspaceName));
            }
        }

        [Fact]
        public void SimpleTest()
        {
            Test();
        }

        [Fact]
        public void ExceedingCassINT()
        {
            using (var conn = manager.Connect())
            {
                ExecuteSync(conn, "USE test");
                string tableName = "table" + Guid.NewGuid().ToString("N");
                ExecuteSync(conn, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         number int
         );", tableName));
                ExecuteSync(conn, string.Format("INSERT INTO {0}(tweet_id,number) VALUES ({1}, '{2}');", tableName, Guid.NewGuid().ToString(), long.MaxValue), null, new OutputInvalid());
                ExecuteSync(conn, string.Format("DROP TABLE {0};", tableName));
            }
        }

    }
}
