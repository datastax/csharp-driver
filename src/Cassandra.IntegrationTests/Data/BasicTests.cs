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

namespace Cassandra.IntegrationTests.Data
{
    [TestClass]
    public class BasicTests
    {
        private CqlConnection connection;
        private Session session;

        [TestInitialize]
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

        [TestCleanup]
        public void Dispose()
        {
            connection.Dispose();
            CCMBridge.ReusableCCMCluster.Drop();
        }

        public void complexTest()
        {
            connection.Open();
            DbCommand cmd = connection.CreateCommand();

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

            int RowsNo = 2000;
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
            DbDataReader reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                    Console.Write(reader.GetValue(i) + "|");
                Console.WriteLine();
            }

            cmd.CommandText = string.Format(@"DROP TABLE {0};", tableName);
            cmd.ExecuteNonQuery();

            cmd.CommandText = string.Format(@"DROP KEYSPACE {0};", keyspaceName);
            cmd.ExecuteNonQuery();
        }

        [TestMethod]
        [WorksForMe]
        public void ComplexTest()
        {
            complexTest();
        }
    }
}