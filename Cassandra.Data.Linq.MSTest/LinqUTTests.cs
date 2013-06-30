using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Globalization;
using System.Diagnostics;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cassandra.MSTest;
#endif

namespace Cassandra.Data.Linq.MSTest
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
            public int ck1 { get; set; }

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
            var table = SessionExtensions.GetTable<TestTable>(null);

            Assert.Equal(
                (from ent in table select ent).ToString(),
                @"SELECT * FROM x_t ALLOW FILTERING");

            Assert.Equal(
                (from ent in table select ent.f1).ToString(),
                @"SELECT x_f1 FROM x_t ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" select ent.f1).ToString(),
                @"SELECT x_f1 FROM x_t WHERE x_pk = 'koko' ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" select new { ent.f1, ent.ck2 }).ToString(),
                @"SELECT x_f1, x_ck2 FROM x_t WHERE x_pk = 'koko' ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2 }).ToString(),
                @"SELECT x_f1, x_ck2 FROM x_t WHERE x_pk = 'koko' AND x_ck2 = 10 ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2 }).Take(10).ToString(),
                @"SELECT x_f1, x_ck2 FROM x_t WHERE x_pk = 'koko' AND x_ck2 = 10 LIMIT 10 ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2 }).OrderBy(c => c.ck2).ToString(),
                @"SELECT x_f1, x_ck2 FROM x_t WHERE x_pk = 'koko' AND x_ck2 = 10 ORDER BY x_ck2 ASC ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2, ent.ck1 }).OrderBy(c => c.ck2).OrderByDescending(c => c.ck1).ToString(),
                @"SELECT x_f1, x_ck2, x_ck1 FROM x_t WHERE x_pk = 'koko' AND x_ck2 = 10 ORDER BY x_ck2 ASC, x_ck1 DESC ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2, ent.ck1 }).OrderBy(c => c.ck2).OrderByDescending(c => c.ck1).ToString(),
                @"SELECT x_f1, x_ck2, x_ck1 FROM x_t WHERE x_pk = 'koko' AND x_ck2 = 10 ORDER BY x_ck2 ASC, x_ck1 DESC ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where CqlToken.Create(ent.pk, ent.ck2, ent.ck2) > CqlToken.Create("x", 2) select new { ent.f1, ent.ck2, ent.ck1 }).OrderBy(c => c.ck2).OrderByDescending(c => c.ck1).ToString(),
                @"SELECT x_f1, x_ck2, x_ck1 FROM x_t WHERE token(x_pk, x_ck2, x_ck2) > token('x', 2) ORDER BY x_ck2 ASC, x_ck1 DESC ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.ck2 > ent.ck1 select ent).ToString(),
                @"SELECT * FROM x_t WHERE x_ck2 > x_ck1 ALLOW FILTERING");

            Assert.Equal(
                (from ent in table select ent).Count().ToString(),
                @"SELECT count(*) FROM x_t");

            Assert.Equal(
                (from ent in table select ent).FirstOrDefault().ToString(),
                @"SELECT * FROM x_t LIMIT 1 ALLOW FILTERING");

            Assert.Equal(
                (from ent in table select ent).First().ToString(),
                @"SELECT * FROM x_t LIMIT 1 ALLOW FILTERING");

            Assert.Equal(
                (from ent in table select ent).Where(e => e.pk.CompareTo("a") > 0).First().ToString(),
                @"SELECT * FROM x_t WHERE x_pk > 'a' LIMIT 1 ALLOW FILTERING");

            try
            {
                Assert.Equal(
                    (from ent in table where ent.pk == "x" || ent.ck2 == 1 select ent).ToString(),
                    @"?");
            }
            catch (CqlLinqNotSupportedException) { }

            Assert.Equal(
                (from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select ent).ToString(),
                @"SELECT * FROM x_t WHERE x_ck2 IN (10, 30, 40) ALLOW FILTERING");

            try
            {
                Assert.Equal(
                    (from ent in table where new int[] { }.Contains(ent.ck2) select ent).ToString(),
                    @"?");
            }
            catch (CqlArgumentException) { }

            Assert.Equal(
               (from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select ent).Delete().ToString(),
               @"DELETE FROM x_t WHERE x_ck2 IN (10, 30, 40)");

            Assert.Equal(
               (table.Insert(new TestTable() { ck1 = 1, ck2 = 2, f1 = 3, pk = "x" })).ToString(),
               @"INSERT INTO x_t(x_pk, x_ck1, x_ck2, x_f1) VALUES ('x', 1, 2, 3)");

            try
            {
                Assert.Equal(
                    (from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select new { x = ent.pk, e = ent }).ToString(),
                    @"?");
            }
            catch (CqlLinqNotSupportedException) { }

            {
                var batch = SessionExtensions.CreateBatch(null);
                batch.Append(table.Insert(new TestTable() { ck1 = 1, ck2 = 2, f1 = 3, pk = "x" }));
                batch.Append((from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select new { f1 = 1223 }).Update());
                batch.Append((from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select ent).Delete());
                Assert.Equal(batch.ToString().Replace("\r",""),
                    @"BEGIN BATCH
INSERT INTO x_t(x_pk, x_ck1, x_ck2, x_f1) VALUES ('x', 1, 2, 3);
UPDATE x_t SET x_f1 = 1223 WHERE x_ck2 IN (10, 30, 40);
DELETE FROM x_t WHERE x_ck2 IN (10, 30, 40);
APPLY BATCH".Replace("\r", ""));
            }

            {
                var batch = SessionExtensions.CreateBatch(null);
                batch.Append(table.Insert(new TestTable() { ck1 = 1, ck2 = 2, f1 = 3, pk = "x" }));
                batch.Append(table.Where(ent => new int[] { 10, 30, 40 }.Contains(ent.ck2)).Select(ent => new { f1 = 1223 }).Update());
                batch.Append(table.Where(ent => new int[] { 10, 30, 40 }.Contains(ent.ck2)).Delete());
                Assert.Equal(batch.ToString().Replace("\r", ""),
                    @"BEGIN BATCH
INSERT INTO x_t(x_pk, x_ck1, x_ck2, x_f1) VALUES ('x', 1, 2, 3);
UPDATE x_t SET x_f1 = 1223 WHERE x_ck2 IN (10, 30, 40);
DELETE FROM x_t WHERE x_ck2 IN (10, 30, 40);
APPLY BATCH".Replace("\r", ""));
            }
        }
    }
}
