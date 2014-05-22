//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
using System;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading;
using Cassandra.Data;
using NUnit.Framework;
using System.Configuration;

namespace Cassandra.IntegrationTests.Data
{
    [TestFixture, Category("short")]
    public class AdoBasicTests : SingleNodeClusterTest
    {
        private CqlConnection _connection;

        public override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();

            var host = "127.0.0.1";
            if (TestUtils.UseRemoteCcm)
            {
                host = Options.Default.IP_PREFIX + "1";
            }
            var cb = new CassandraConnectionStringBuilder();
            cb.ContactPoints = new[] { host};
            cb.Port = 9042;
            _connection = new CqlConnection(cb.ToString());
        }

        [Test]
        public void ExecuteNonQueryInsertAndSelectTest()
        {
            _connection.Open();
            var cmd = _connection.CreateCommand();

            string keyspaceName = "keyspace_ado_1";
            cmd.CommandText = string.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspaceName, 3);
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

            cmd.CommandText = string.Format(@"DROP TABLE {0};", tableName);
            cmd.ExecuteNonQuery();

            cmd.CommandText = string.Format(@"DROP KEYSPACE {0};", keyspaceName);
            cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Tests that ExecuteScalar method returns the first column value of the first row, or null if no rows.
        /// </summary>
        [Test]
        public void ExecuteScalarReturnsFirstColumn()
        {
            _connection.Open();
            var cmd1 = _connection.CreateCommand();
            var cmd2 = _connection.CreateCommand();
            var cmd3 = _connection.CreateCommand();

            cmd1.CommandText = "SELECT keyspace_name, durable_writes FROM system.schema_keyspaces";
            cmd2.CommandText = "SELECT durable_writes, keyspace_name FROM system.schema_keyspaces";
            cmd3.CommandText = "SELECT * FROM system.schema_keyspaces WHERE keyspace_name = 'NOT_EXISTENT_" + Guid.NewGuid().ToString() + "'";
            Assert.IsInstanceOf<string>(cmd1.ExecuteScalar());
            Assert.IsInstanceOf<bool>(cmd2.ExecuteScalar());
            Assert.IsNull(cmd3.ExecuteScalar());
        }
    }
}