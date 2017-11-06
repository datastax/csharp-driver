//
//      Copyright (C) 2017 DataStax Inc.
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
using Dse.Data.Linq;
using Dse.Mapping;
using Dse.Test.Unit.Mapping.Pocos;
using NUnit.Framework;

namespace Dse.Test.Unit.Mapping.Linq
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
            var table = new Table<InsertNullTable>(null, new MappingConfiguration());
            var row = new InsertNullTable { Key = 101, Value = null };

            var cqlInsert = table.Insert(row);
            object[] values;
            var cql = cqlInsert.GetCqlAndValues(out values);

            Assert.AreEqual("INSERT INTO InsertNullTable (Key, Value) VALUES (?, ?)", cql);
            Assert.AreEqual(2, values.Length);
            Assert.AreEqual(row.Key, values[0]);
            Assert.AreEqual(row.Value, values[1]);
        }

        [Test]
        public void Insert_Without_Nulls_Test()
        {
            var table = new Table<InsertNullTable>(null, new MappingConfiguration());
            var row = new InsertNullTable { Key = 102, Value = null };

            var cqlInsert = table.Insert(row, false);
            object[] values;
            var cql = cqlInsert.GetCqlAndValues(out values);

            Assert.AreEqual("INSERT INTO InsertNullTable (Key) VALUES (?)", cql);
            Assert.AreEqual(1, values.Length);
            Assert.AreEqual(row.Key, values[0]);
        }

        [Test]
        public void Insert_Without_Nulls_With_Table_And_Keyspace_Name_Test()
        {
            var table = new Table<InsertNullTable>(null, new MappingConfiguration(), "tbl1", "ks100");
            var row = new InsertNullTable { Key = 102, Value = null };

            var cqlInsert = table.Insert(row, false);
            object[] values;
            var cql = cqlInsert.GetCqlAndValues(out values);

            Assert.AreEqual("INSERT INTO ks100.tbl1 (Key) VALUES (?)", cql);
            Assert.AreEqual(1, values.Length);
            Assert.AreEqual(102, values[0]);
        }

        [Test]
        public void Insert_Without_Nulls_With_Table_Test()
        {
            var table = new Table<InsertNullTable>(null, new MappingConfiguration(), "tbl1");
            var row = new InsertNullTable { Key = 110, Value = null };

            var cqlInsert = table.Insert(row, false);
            object[] values;
            var cql = cqlInsert.GetCqlAndValues(out values);

            Assert.AreEqual("INSERT INTO tbl1 (Key) VALUES (?)", cql);
            Assert.AreEqual(1, values.Length);
            Assert.AreEqual(110, values[0]);
        }

        [Test]
        public void Insert_IfNotExists_Test()
        {
            var table = SessionExtensions.GetTable<AllTypesDecorated>(null);
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
            var table = new Table<InsertNullTable>(null, new MappingConfiguration());
            var row = new InsertNullTable { Key = 103, Value = null };

            var timestamp = DateTimeOffset.UtcNow;
            var cqlInsert = table.Insert(row);
            cqlInsert.IfNotExists();
            cqlInsert.SetTTL(86401);
            cqlInsert.SetTimestamp(timestamp);
            object[] values;
            var cql = cqlInsert.GetCqlAndValues(out values);

            Assert.AreEqual("INSERT INTO InsertNullTable (Key, Value) VALUES (?, ?) IF NOT EXISTS USING TTL ? AND TIMESTAMP ?", cql);
            Assert.AreEqual(4, values.Length);
            Assert.AreEqual(103, values[0]);
            Assert.AreEqual(null, values[1]);
            Assert.AreEqual(86401, values[2]);
            Assert.AreEqual((timestamp - new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero)).Ticks / 10, values[3]);
        }

        [Test]
        public void Insert_IfNotExists_Without_Nulls_With_Timestamp_Test()
        {
            var table = new Table<InsertNullTable>(null, new MappingConfiguration());
            var row = new InsertNullTable { Key = 104, Value = null };

            var timestamp = DateTimeOffset.UtcNow;
            var cqlInsert = table.Insert(row, false);
            cqlInsert.IfNotExists();
            cqlInsert.SetTimestamp(timestamp);
            object[] values;
            var cql = cqlInsert.GetCqlAndValues(out values);

            Assert.AreEqual("INSERT INTO InsertNullTable (Key) VALUES (?) IF NOT EXISTS USING TIMESTAMP ?", cql);
            Assert.AreEqual(2, values.Length);
            Assert.AreEqual(104, values[0]);
            Assert.AreEqual((timestamp - new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero)).Ticks / 10, values[1]);
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
            table
                .Insert(song)
                .IfNotExists()
                .SetTTL(ttl)
                .Execute();
            Assert.AreEqual(
                "INSERT INTO Song (Id, Title, Artist, ReleaseDate) VALUES (?, ?, ?, ?) IF NOT EXISTS USING TTL ?",
                query);
            Assert.AreEqual(new object[] { song.Id, song.Title, song.Artist, song.ReleaseDate, ttl }, parameters);
        }
    }
}