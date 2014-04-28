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

using Assert = NUnit.Framework.Assert;
using System;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading;
using Cassandra.Data;
using NUnit.Framework;
using NAssert = NUnit.Framework.Assert;

namespace Cassandra.IntegrationTests.Data
{
    [TestFixture]
    public class AdoBasicTests
    {
        private CqlConnection connection;
        private ISession session;

        [SetUp]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMBridge.ReusableCCMCluster.Setup(2);
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder());
            session = CCMBridge.ReusableCCMCluster.Connect("tester");

            var cb = new CassandraConnectionStringBuilder();
            cb.ContactPoints = new[] {Options.Default.IP_PREFIX + "1"};
            cb.Port = 9042;
            connection = new CqlConnection(cb.ToString());
        }

        [TearDown]
        public void Dispose()
        {
            connection.Dispose();
            CCMBridge.ReusableCCMCluster.Drop();
        }

        public void createObjectsInsertAndSelect()
        {
            connection.Open();
            var cmd = connection.CreateCommand();

            string keyspaceName = "keyspace" + Guid.NewGuid().ToString("N").ToLower();

            cmd.CommandText = string.Format(@"CREATE KEYSPACE {0} 
                     WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};"
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


            var longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            int RowsNo = 300;
            for (int i = 0; i < RowsNo; i++)
            {
                longQ.AppendFormat(@"INSERT INTO {0} (
         tweet_id,
         author,
         isok,
         body)
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
                NAssert.AreEqual(4, reader.FieldCount);
                counter++;
            }

            NAssert.AreEqual(RowsNo, counter);

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
            connection.Open();
            var cmd1 = connection.CreateCommand();
            var cmd2 = connection.CreateCommand();
            var cmd3 = connection.CreateCommand();

            cmd1.CommandText = "SELECT keyspace_name, durable_writes FROM system.schema_keyspaces";
            cmd2.CommandText = "SELECT durable_writes, keyspace_name FROM system.schema_keyspaces";
            cmd3.CommandText = "SELECT * FROM system.schema_keyspaces WHERE keyspace_name = 'NOT_EXISTENT_" + Guid.NewGuid().ToString() + "'";
            NAssert.IsInstanceOf<string>(cmd1.ExecuteScalar());
            NAssert.IsInstanceOf<bool>(cmd2.ExecuteScalar());
            NAssert.IsNull(cmd3.ExecuteScalar());
        }

        [Test]
        public void ExecuteNonQueryTest()
        {
            createObjectsInsertAndSelect();
        }
    }
}