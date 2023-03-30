//
//      Copyright (C) DataStax Inc.
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
using System.Linq;
using System.Text;
using Cassandra.Data;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Data
{
    public class AdoBasicTests : SimulacronTest
    {
        private CqlConnection _connection;

        public override void SetUp()
        {
            base.SetUp();
            var cb = new CassandraConnectionStringBuilder
            {
                ContactPoints = new[] { TestCluster.InitialContactPoint.Address.ToString() }, 
                Port = 9042
            };
            _connection = new CqlConnection(cb.ToString());
        }

        public override void TearDown()
        {
            _connection?.Dispose();
            base.TearDown();
        }

        [Test]
        public void ExecuteNonQueryInsertAndSelectTest()
        {
            _connection.Open();
            var cmd = _connection.CreateCommand();

            string keyspaceName = "keyspace_ado_1";
            cmd.CommandText = string.Format(TestUtils.CreateKeyspaceSimpleFormat, keyspaceName, 3);
            cmd.ExecuteNonQuery();

            VerifyStatement(
                QueryType.Query,
                $"CREATE KEYSPACE \"{keyspaceName}\" WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : {3} }}",
                1);

            _connection.ChangeDatabase(keyspaceName);

            VerifyStatement(QueryType.Query, $"USE \"{keyspaceName}\"", 1);

            string tableName = "table_ado_1";
            cmd.CommandText = string.Format("CREATE TABLE {0} (tweet_id uuid,author text,body text,isok boolean,PRIMARY KEY(tweet_id))", tableName);
            cmd.ExecuteNonQuery();

            VerifyStatement(
                QueryType.Query,
                $"CREATE TABLE {tableName} (tweet_id uuid,author text,body text,isok boolean,PRIMARY KEY(tweet_id))",
                1);

            var longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            var guids = Enumerable.Range(0, 300).Select(i => Guid.NewGuid()).ToArray();
            int RowsNo = 300;
            for (int i = 0; i < RowsNo; i++)
            {
                longQ.AppendFormat("INSERT INTO {0} (tweet_id, author, isok, body) VALUES ({1},'test{2}',{3},'body{2}');", tableName, guids[i], i, i%2 == 0 ? "false" : "true");
            }
            longQ.AppendLine("APPLY BATCH;");
            cmd.CommandText = longQ.ToString();
            cmd.ExecuteNonQuery();

            VerifyStatement(
                QueryType.Query,
                longQ.ToString(),
                1);

            TestCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * from {tableName} LIMIT 10000;")
                      .ThenRowsSuccess(
                          new[] {"tweet_id", "author", "body", "isok"},
                          r => r.WithRows(guids.Select((guid, idx) => new object[] { guid, $"test{idx}", $"body{idx}", idx % 2 != 0 }).ToArray())));

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

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT key FROM system.local")
                      .ThenRowsSuccess(new [] { "key" }, r => r.WithRow("local")));

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM system.local WHERE key = 'does not exist'")
                      .ThenVoidSuccess());

            cmd1.CommandText = "SELECT key FROM system.local";
            cmd3.CommandText = "SELECT * FROM system.local WHERE key = 'does not exist'";
            Assert.IsInstanceOf<string>(cmd1.ExecuteScalar());
            Assert.IsNull(cmd3.ExecuteScalar());
        }
    }
}
