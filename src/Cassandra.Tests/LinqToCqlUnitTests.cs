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
using System;
using Moq;
using System.Collections.Generic;

namespace Cassandra.Tests
{
    [TestFixture]
    public class LinqToCqlUnitTests
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

        [Test]
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

        [Test]
        public void LinqGeneratedUpdateStatementTest()
        {
            var table = SessionExtensions.GetTable<AllTypesEntity>(null);
            string query;
            string expectedQuery;

            query = table
                .Where(r => r.StringValue == "key")
                .Select(r => new AllTypesEntity() { IntValue = 1 })
                .Update()
                .ToString();
            expectedQuery = "UPDATE \"AllTypesEntity\" SET \"IntValue\" = 1 WHERE \"StringValue\" = 'key'";
            Assert.AreEqual(expectedQuery, query);

            Assert.Throws<CqlArgumentException>(() =>
            {
                //Update without a set statement
                //Must include SELECT to project to a new form
                query = table
                    .Where(r => r.StringValue == "key")
                    .Update()
                    .ToString();
            });
        }

        [Test]
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

        [Test]
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

        /// <summary>
        /// Test utility: Represents an application entity with most of common types as properties
        /// </summary>
        public class AllTypesEntity
        {
            public bool BooleanValue { get; set; }
            public DateTime DateTimeValue { get; set; }
            public decimal DecimalValue { get; set; }
            public double DoubleValue { get; set; }
            public Int64 Int64Value { get; set; }
            public int IntValue { get; set; }
            public string StringValue { get; set; }
            public Guid UuidValue { get; set; }
        }

        /// <summary>
        /// Tests the Linq to CQL generated where clause 
        /// </summary>
        [Test]
        public void LinqSelectWhereTest()
        {
            var sessionMock = new Mock<ISession>();
            var session = sessionMock.Object;

            var table = session.GetTable<AllTypesEntity>();
            var date = new DateTime(1975, 1, 1);
            var linqQueries = new List<CqlQuery<AllTypesEntity>>()
            {
                (from ent in table where ent.BooleanValue == true select ent),
                (from ent in table where ent.BooleanValue == false select ent),
                (from ent in table where ent.DateTimeValue < date select ent),
                (from ent in table where ent.DateTimeValue >= date select ent),
                (from ent in table where ent.IntValue == 0 select ent),
                (from ent in table where ent.StringValue == "Hello world" select ent)
                
            };
            var expectedCqlQueries = new List<string>()
            {
                "SELECT * FROM \"AllTypesEntity\" WHERE \"BooleanValue\" = true",
                "SELECT * FROM \"AllTypesEntity\" WHERE \"BooleanValue\" = false",
                "SELECT * FROM \"AllTypesEntity\" WHERE \"DateTimeValue\" < 157766400000",
                "SELECT * FROM \"AllTypesEntity\" WHERE \"DateTimeValue\" >= 157766400000",
                "SELECT * FROM \"AllTypesEntity\" WHERE \"IntValue\" = 0",
                "SELECT * FROM \"AllTypesEntity\" WHERE \"StringValue\" = 'Hello world'"
            };
            var actualCqlQueries = new List<IStatement>();
            sessionMock
                .Setup(s => s.BeginExecute(It.IsAny<IStatement>(), It.IsAny<object>(), It.IsAny<AsyncCallback>(), It.IsAny<object>()))
                .Callback<IStatement, object, AsyncCallback, object>((stmt2, b, c, d) => actualCqlQueries.Add(stmt2));


            //Execute all linq queries
            foreach (var q in linqQueries)
            {
                q.Execute();
            }
            sessionMock.Verify();

            Assert.AreEqual(expectedCqlQueries.Count, actualCqlQueries.Count);
            //Check that all expected queries and actual queries are equal
            for (var i = 0; i < expectedCqlQueries.Count; i++)
            {
                Assert.IsInstanceOf<SimpleStatement>(actualCqlQueries[i]);
                Assert.AreEqual(
                    expectedCqlQueries[i],
                    ((SimpleStatement)actualCqlQueries[i]).QueryString,
                    "Expected Cql query and generated CQL query by Linq do not match.");
            }
        }
    }
}