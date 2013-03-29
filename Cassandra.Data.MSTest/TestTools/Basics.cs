using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;

namespace Cassandra.Data.MSTest
{
    [TestClass]
    public partial class BasicTests
    {
        public BasicTests()
        {
        }

        CqlConnection connection = null;

        [TestInitialize]
        public void SetFixture()
        {
            connection = new CqlConnection("Contact Points=cassi.cloudapp.net;Port=9042");
        }

        [TestCleanup]
        public void Dispose()
        {
            connection.Dispose();
        }

        public void complexTest()
        {
            connection.Open();
            var cmd = connection.CreateCommand();

            string keyspaceName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();

            cmd.CommandText = string.Format(@"CREATE KEYSPACE {0} 
                     WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }};"
                 , keyspaceName);
            cmd.ExecuteNonQuery();

            connection.ChangeDatabase(keyspaceName);

            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            cmd.CommandText = string.Format(@"CREATE TABLE {0}(
         tweet_id uuid,
         author text,
         body text,
         isok boolean,
         PRIMARY KEY(tweet_id))", tableName);
            cmd.ExecuteNonQuery();


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
         VALUES ({1},'test{2}',{3},'body{2}');", tableName, Guid.NewGuid().ToString(), i, i % 2 == 0 ? "false" : "true");
            }
            longQ.AppendLine("APPLY BATCH;");
            cmd.CommandText = longQ.ToString();
            cmd.ExecuteNonQuery();

            cmd.CommandText = string.Format(@"SELECT * from {0} LIMIT 10000;", tableName);
            var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                    Console.Write(reader.GetValue(i).ToString() + "|");
                Console.WriteLine();
            }

            cmd.CommandText = string.Format(@"DROP TABLE {0};", tableName);
            cmd.ExecuteNonQuery();

            cmd.CommandText = string.Format(@"DROP KEYSPACE {0};", keyspaceName);
            cmd.ExecuteNonQuery();

        }

    }
}