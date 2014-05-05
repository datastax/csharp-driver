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

using System.Linq;
using Cassandra.Data.Linq;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq
{
    [TestClass]
    public class LinqUTTests
    {

        [AllowFiltering]
        [Table("x_t")]
        public class TestTable
        {
            [PartitionKey]
            [Column("x_pk")]
            public string pk { get; set; }

            [ClusteringKey(1)]
            [Column("x_ck1")]
            public int? ck1 { get; set; }

            [ClusteringKey(2)]
            [Column("x_ck2")]
            public int ck2 { get; set; }

            [Column("x_f1")]
            public int f1 { get; set; }
        }

        [TestMethod]
        [WorksForMe]
        public void TestCqlFromLinq()
        {
            Table<TestTable> table = SessionExtensions.GetTable<TestTable>(null);

            Assert.AreEqual(
                (from ent in table select ent).ToString(),
                @"SELECT * FROM ""x_t"" ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table select ent.f1).ToString(),
                @"SELECT ""x_f1"" FROM ""x_t"" ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table where ent.pk == "koko" select ent.f1).ToString(),
                @"SELECT ""x_f1"" FROM ""x_t"" WHERE ""x_pk"" = 'koko' ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table where ent.pk == "koko" select new { ent.f1, ent.ck2 }).ToString(),
                @"SELECT ""x_f1"", ""x_ck2"" FROM ""x_t"" WHERE ""x_pk"" = 'koko' ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2 }).ToString(),
                @"SELECT ""x_f1"", ""x_ck2"" FROM ""x_t"" WHERE ""x_pk"" = 'koko' AND ""x_ck2"" = 10 ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2 }).Take(10).ToString(),
                @"SELECT ""x_f1"", ""x_ck2"" FROM ""x_t"" WHERE ""x_pk"" = 'koko' AND ""x_ck2"" = 10 LIMIT 10 ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2 }).OrderBy(c => c.ck2).ToString(),
                @"SELECT ""x_f1"", ""x_ck2"" FROM ""x_t"" WHERE ""x_pk"" = 'koko' AND ""x_ck2"" = 10 ORDER BY ""x_ck2"" ASC ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2, ent.ck1 }).OrderBy(c => c.ck2).OrderByDescending(c => c.ck1).ToString(),
                @"SELECT ""x_f1"", ""x_ck2"", ""x_ck1"" FROM ""x_t"" WHERE ""x_pk"" = 'koko' AND ""x_ck2"" = 10 ORDER BY ""x_ck2"" ASC, ""x_ck1"" DESC ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2, ent.ck1 }).OrderBy(c => c.ck2).OrderByDescending(c => c.ck1).ToString(),
                @"SELECT ""x_f1"", ""x_ck2"", ""x_ck1"" FROM ""x_t"" WHERE ""x_pk"" = 'koko' AND ""x_ck2"" = 10 ORDER BY ""x_ck2"" ASC, ""x_ck1"" DESC ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table where CqlToken.Create(ent.pk, ent.ck2, ent.ck2) > CqlToken.Create("x", 2) select new { ent.f1, ent.ck2, ent.ck1 }).OrderBy(c => c.ck2).OrderByDescending(c => c.ck1).ToString(),
                @"SELECT ""x_f1"", ""x_ck2"", ""x_ck1"" FROM ""x_t"" WHERE token(""x_pk"", ""x_ck2"", ""x_ck2"") > token('x', 2) ORDER BY ""x_ck2"" ASC, ""x_ck1"" DESC ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table where ent.ck2 > ent.ck1 select ent).ToString(),
                @"SELECT * FROM ""x_t"" WHERE ""x_ck2"" > ""x_ck1"" ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table select ent).Count().ToString(),
                @"SELECT count(*) FROM ""x_t""");

            Assert.AreEqual(
                (from ent in table select ent).FirstOrDefault().ToString(),
                @"SELECT * FROM ""x_t"" LIMIT 1 ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table select ent).First().ToString(),
                @"SELECT * FROM ""x_t"" LIMIT 1 ALLOW FILTERING");

            Assert.AreEqual(
                (from ent in table select ent).Where(e => e.pk.CompareTo("a") > 0).First().ToString(),
                @"SELECT * FROM ""x_t"" WHERE ""x_pk"" > 'a' LIMIT 1 ALLOW FILTERING");

            try
            {
                Assert.AreEqual(
                    (from ent in table where ent.pk == "x" || ent.ck2 == 1 select ent).ToString(),
                    @"?");
            }
            catch (CqlLinqNotSupportedException)
            {
            }

            Assert.AreEqual(
                (from ent in table where new[] {10, 30, 40}.Contains(ent.ck2) select ent).ToString(),
                @"SELECT * FROM ""x_t"" WHERE ""x_ck2"" IN (10, 30, 40) ALLOW FILTERING");

            try
            {
                Assert.AreEqual(
                    (from ent in table where new int[] {}.Contains(ent.ck2) select ent).ToString(),
                    @"?");
            }
            catch (CqlArgumentException)
            {
            }

            Assert.AreEqual(
               (from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select ent).Delete().ToString(),
               @"DELETE FROM ""x_t"" WHERE ""x_ck2"" IN (10, 30, 40)");

            Assert.AreEqual(
               (table.Insert(new TestTable() { ck1 = 1, ck2 = 2, f1 = 3, pk = "x" })).ToString(),
               @"INSERT INTO ""x_t""(""x_pk"", ""x_ck1"", ""x_ck2"", ""x_f1"") VALUES ('x', 1, 2, 3)");

            try
            {
                Assert.AreEqual(
                    (from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select new { x = ent.pk, e = ent }).ToString(),
                    @"?");
            }
            catch (CqlLinqNotSupportedException) { }

            {
                var batch = SessionExtensions.CreateBatch(null);
                batch.Append(table.Insert(new TestTable() { ck1 = 1, ck2 = 2, f1 = 3, pk = "x" }));
                batch.Append((from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select new { f1 = 1223 }).Update());
                batch.Append((from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select ent).Delete());
                Assert.AreEqual(batch.ToString().Replace("\r", ""),
                    @"BEGIN BATCH
INSERT INTO ""x_t""(""x_pk"", ""x_ck1"", ""x_ck2"", ""x_f1"") VALUES ('x', 1, 2, 3);
UPDATE ""x_t"" SET ""x_f1"" = 1223 WHERE ""x_ck2"" IN (10, 30, 40);
DELETE FROM ""x_t"" WHERE ""x_ck2"" IN (10, 30, 40);
APPLY BATCH".Replace("\r", ""));
            }

            {
                var batch = SessionExtensions.CreateBatch(null);
                batch.Append(table.Insert(new TestTable() { ck1 = 1, ck2 = 2, f1 = 3, pk = "x" }));
                batch.Append(table.Where(ent => new int[] { 10, 30, 40 }.Contains(ent.ck2)).Select(ent => new { f1 = 1223 }).Update());
                batch.Append(table.Where(ent => new int[] { 10, 30, 40 }.Contains(ent.ck2)).Delete());
                Assert.AreEqual(batch.ToString().Replace("\r", ""),
                    @"BEGIN BATCH
INSERT INTO ""x_t""(""x_pk"", ""x_ck1"", ""x_ck2"", ""x_f1"") VALUES ('x', 1, 2, 3);
UPDATE ""x_t"" SET ""x_f1"" = 1223 WHERE ""x_ck2"" IN (10, 30, 40);
DELETE FROM ""x_t"" WHERE ""x_ck2"" IN (10, 30, 40);
APPLY BATCH".Replace("\r", ""));
            }
        }

        [TestMethod]
        [WorksForMe]
        public void TestCqlFromLinqPaxosSupport()
        {
            var table = SessionExtensions.GetTable<TestTable>(null);

            Assert.AreEqual(
               (table.Insert(new TestTable() { ck1 = 1, ck2 = 2, f1 = 3, pk = "x" })).IfNotExists().ToString(),
               @"INSERT INTO ""x_t""(""x_pk"", ""x_ck1"", ""x_ck2"", ""x_f1"") VALUES ('x', 1, 2, 3) IF NOT EXISTS");

            Assert.AreEqual((from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select new { f1 = 1223 }).UpdateIf((a) => a.f1 == 123).ToString(),
                    @"UPDATE ""x_t"" SET ""x_f1"" = 1223 WHERE ""x_ck2"" IN (10, 30, 40) IF ""x_f1"" = 123");

            Assert.AreEqual((from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select ent).DeleteIf((a) => a.f1 == 123).ToString(),
                @"DELETE FROM ""x_t"" WHERE ""x_ck2"" IN (10, 30, 40) IF ""x_f1"" = 123");

            Assert.AreEqual((from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select ent).Delete().IfExists().ToString(),
                @"DELETE FROM ""x_t"" WHERE ""x_ck2"" IN (10, 30, 40) IF EXISTS ");
        }

        [TestMethod]
        [WorksForMe]
        public void TestCqlNullValuesLinqSupport()
        {
            var table = SessionExtensions.GetTable<TestTable>(null);

            Assert.AreEqual(
               (table.Insert(new TestTable() { ck1 = null, ck2 = 2, f1 = 3, pk = "x" })).ToString(),
               @"INSERT INTO ""x_t""(""x_pk"", ""x_ck2"", ""x_f1"") VALUES ('x', 2, 3)");

            Assert.AreEqual((from ent in table where new int?[] { 10, 30, 40 }.Contains(ent.ck1) select new { f1 = 1223 }).Update().ToString(),
                    @"UPDATE ""x_t"" SET ""x_f1"" = 1223 WHERE ""x_ck1"" IN (10, 30, 40)");

            Assert.AreEqual((from ent in table where new int?[] { 10, 30, 40 }.Contains(ent.ck1) select new TestTable() { f1 = 1223, ck1 = null }).Update().ToString(),
                    @"UPDATE ""x_t"" SET ""x_f1"" = 1223, ""x_ck1"" = NULL WHERE ""x_ck1"" IN (10, 30, 40)");

            Assert.AreEqual((from ent in table where ent.ck1 == 1 select new TestTable() { f1 = 1223, ck1=null }).Update().ToString(),
                    @"UPDATE ""x_t"" SET ""x_f1"" = 1223, ""x_ck1"" = NULL WHERE ""x_ck1"" = 1");

            Assert.AreEqual((from ent in table where new int?[] { 10, 30, 40 }.Contains(ent.ck1) select new { f1 = 1223, ck1 = (int?)null }).UpdateIf((a) => a.f1 == 123).ToString(),
                    @"UPDATE ""x_t"" SET ""x_f1"" = 1223, ""x_ck1"" = NULL WHERE ""x_ck1"" IN (10, 30, 40) IF ""x_f1"" = 123");
        }
    }
}