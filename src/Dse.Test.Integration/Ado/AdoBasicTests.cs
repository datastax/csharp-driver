//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Text;
using Dse.Data;
using Dse.Test.Integration.TestClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration.Data
{
    [TestFixture, Category("short"), Category("realcluster")]
    public class AdoBasicTests : SharedClusterTest
    {
        private CqlConnection _connection;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            var cb = new CassandraConnectionStringBuilder
            {
                ContactPoints = new[] { TestCluster.InitialContactPoint }, 
                Port = 9042
            };
            _connection = new CqlConnection(cb.ToString());
        }

        [Test]
        public void ExecuteNonQueryInsertAndSelectTest()
        {
            _connection.Open();
            var cmd = _connection.CreateCommand();

            string keyspaceName = "keyspace_ado_1";
            cmd.CommandText = string.Format(TestUtils.CreateKeyspaceSimpleFormat, keyspaceName, 3);
            cmd.ExecuteNonQuery();

            _connection.ChangeDatabase(keyspaceName);

            string tableName = "table_ado_1";
            cmd.CommandText = string.Format(@"
                CREATE TABLE {0}(
                tweet_id uuid,
                author text,
                body text,
                isok boolean,
                PRIMARY KEY(tweet_id))", tableName);
            cmd.ExecuteNonQuery();


            var longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            int RowsNo = 300;
            for (int i = 0; i < RowsNo; i++)
            {
                longQ.AppendFormat(@"
                INSERT INTO {0} (tweet_id, author, isok, body)
                VALUES ({1},'test{2}',{3},'body{2}');", tableName, Guid.NewGuid(), i, i%2 == 0 ? "false" : "true");
            }
            longQ.AppendLine("APPLY BATCH;");
            cmd.CommandText = longQ.ToString();
            cmd.ExecuteNonQuery();

            cmd.CommandText = string.Format(@"SELECT * from {0} LIMIT 10000;", tableName);
            var reader = cmd.ExecuteReader();
            var counter = 0;
            while (reader.Read())
            {
                Assert.AreEqual(4, reader.FieldCount);
                counter++;
            }

            Assert.AreEqual(RowsNo, counter);
        }

        [Test]
        public void ExecuteScalarReturnsFirstColumn()
        {
            _connection.Open();
            var cmd1 = _connection.CreateCommand();
            var cmd3 = _connection.CreateCommand();

            cmd1.CommandText = "SELECT key FROM system.local";
            cmd3.CommandText = "SELECT * FROM system.local WHERE key = 'does not exist'";
            Assert.IsInstanceOf<string>(cmd1.ExecuteScalar());
            Assert.IsNull(cmd3.ExecuteScalar());
        }
    }
}
