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
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    public class LinqToCqlInsertUnitTests : MappingTestBase
    {
        private class InsertNullTable
        {
            public int Key { get; set; }

            public string Value { get; set; }
        }

        [Test]
        public void Insert_With_Nulls_Test()
        {
            var table = new Table<InsertNullTable>(GetSession((_,__) => {}), new MappingConfiguration());
            var row = new InsertNullTable { Key = 101, Value = null };

            var cqlInsert = table.Insert(row);
            object[] values;
            var cql = cqlInsert.GetCqlAndValues(out values);

            TestHelper.VerifyInsertCqlColumns("InsertNullTable", cql, new[] {"Key", "Value"}, 
                new object[] {row.Key, row.Value}, values);
        }

        [Test]
        public void Insert_Without_Nulls_Test()
        {
            var table = new Table<InsertNullTable>(GetSession((_,__) => {}), new MappingConfiguration());
            var row = new InsertNullTable { Key = 102, Value = null };

            var cqlInsert = table.Insert(row, false);
            object[] values;
            var cql = cqlInsert.GetCqlAndValues(out values);

            Assert.AreEqual("INSERT INTO InsertNullTable (Key) VALUES (?)", cql);
            TestHelper.VerifyInsertCqlColumns("InsertNullTable", cql, new[] {"Key"}, 
                new object[] {row.Key}, values);
        }

        [Test]
        public void Insert_Without_Nulls_With_Table_And_Keyspace_Name_Test()
        {
            var table = new Table<InsertNullTable>(GetSession((_,__) => {}), new MappingConfiguration(), "tbl1", "ks100");
            var row = new InsertNullTable { Key = 102, Value = null };

            var cqlInsert = table.Insert(row, false);
            object[] values;
            var cql = cqlInsert.GetCqlAndValues(out values);

            TestHelper.VerifyInsertCqlColumns("ks100.tbl1", cql, new[] {"Key"},
                new object[] {102}, values);
        }

        [Test]
        public void Insert_Without_Nulls_With_Table_Test()
        {
            var table = new Table<InsertNullTable>(GetSession((_,__) => {}), new MappingConfiguration(), "tbl1");
            var row = new InsertNullTable { Key = 110, Value = null };

            var cqlInsert = table.Insert(row, false);
            object[] values;
            var cql = cqlInsert.GetCqlAndValues(out values);

            TestHelper.VerifyInsertCqlColumns("tbl1", cql, new[] {"Key"},
                new object[]{ 110 }, values);
        }

        [Test]
        public void Insert_IfNotExists_Test()
        {
            var table = SessionExtensions.GetTable<AllTypesDecorated>(GetSession((_,__) => {}));
            var uuid = Guid.NewGuid();
            var row = new AllTypesDecorated { Int64Value = 202, UuidValue = uuid};

            var cqlInsert = table.Insert(row).IfNotExists();
            object[] values;
            var cql = cqlInsert.GetCql(out values);

            StringAssert.EndsWith("IF NOT EXISTS", cql);
        }

        [Test]
        public void Insert_IfNotExists_With_Ttl_And_Timestamp_Test()
        {
            var table = new Table<InsertNullTable>(GetSession((_,__) => {}), new MappingConfiguration());
            var row = new InsertNullTable { Key = 103, Value = null };

            var timestamp = DateTimeOffset.UtcNow;
            var cqlInsert = table.Insert(row);
            cqlInsert.IfNotExists();
            cqlInsert.SetTTL(86401);
            cqlInsert.SetTimestamp(timestamp);
            object[] values;
            var cql = cqlInsert.GetCqlAndValues(out values);
            var expectedTimestamp = (timestamp - new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero)).Ticks / 10;
            TestHelper.VerifyInsertCqlColumns("InsertNullTable", cql, new[] {"Key", "Value"}, 
                new object[] {103, null, 86401, expectedTimestamp}, values, "IF NOT EXISTS USING TTL ? AND TIMESTAMP ?");
        }

        [Test]
        public void Insert_IfNotExists_Without_Nulls_With_Timestamp_Test()
        {
            var table = new Table<InsertNullTable>(GetSession((_,__) => {}), new MappingConfiguration());
            var row = new InsertNullTable { Key = 104, Value = null };

            var timestamp = DateTimeOffset.UtcNow;
            var cqlInsert = table.Insert(row, false);
            cqlInsert.IfNotExists();
            cqlInsert.SetTimestamp(timestamp);
            object[] values;
            var cql = cqlInsert.GetCqlAndValues(out values);
            var expectedTimestamp = (timestamp - new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero)).Ticks / 10;
            TestHelper.VerifyInsertCqlColumns("InsertNullTable", cql, new[] {"Key"}, 
                new object[]{104, expectedTimestamp}, values, "IF NOT EXISTS USING TIMESTAMP ?");
        }

        [Test]
        public void Insert_IfNotExists_With_Ttl()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var table = GetTable<Song>(session);
            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "Neil Young",
                Title = "Tonight's The Night",
                ReleaseDate = DateTimeOffset.Now
            };
            const int ttl = 300;
            var insert = table
                .Insert(song);
            insert.IfNotExists();
            insert.SetTTL(ttl);
            insert.Execute();
            object[] values;
            var cql = insert.GetCqlAndValues(out values);
            TestHelper.VerifyInsertCqlColumns("Song", query, new[] {"Title", "Id", "Artist", "ReleaseDate"}, 
                new object[] { song.Title, song.Id, song.Artist, song.ReleaseDate, ttl }, values, "IF NOT EXISTS USING TTL ?");
        }
    }
}