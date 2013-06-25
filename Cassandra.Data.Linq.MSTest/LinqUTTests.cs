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
        [Table("t")]
        public class TestTable
        {
            [PartitionKey]
            [Column("pk")]
            public string pk { get; set; }

            [ClusteringKey(1)]
            [Column("ck1")]
            public int ck1 { get; set; }

            [ClusteringKey(2)]
            [Column("ck2")]
            public int ck2 { get; set; }

            [Column("f1")]
            public int f1 { get; set; }
        }

        [TestMethod]
        [WorksForMe]
        public void TestCqlFromLinq()
        {
            var table = SessionExtensions.GetTable<TestTable>(null);

            Assert.Equal(
                (from ent in table select ent).ToString(),
                @"SELECT * FROM t ALLOW FILTERING");

            Assert.Equal(
                (from ent in table select ent.f1).ToString(),
                @"SELECT f1 FROM t ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" select ent.f1).ToString(),
                @"SELECT f1 FROM t WHERE pk = 'koko' ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" select new { ent.f1, ent.ck2 }).ToString(),
                @"SELECT f1, ck2 FROM t WHERE pk = 'koko' ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2 }).ToString(),
                @"SELECT f1, ck2 FROM t WHERE pk = 'koko' AND ck2 = 10 ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2 }).Take(10).ToString(),
                @"SELECT f1, ck2 FROM t WHERE pk = 'koko' AND ck2 = 10 LIMIT 10 ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2 }).OrderBy(c => c.ck2).ToString(),
                @"SELECT f1, ck2 FROM t WHERE pk = 'koko' AND ck2 = 10 ORDER BY ck2 ASC ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2, ent.ck1 }).OrderBy(c => c.ck2).OrderByDescending(c => c.ck1).ToString(),
                @"SELECT f1, ck2, ck1 FROM t WHERE pk = 'koko' AND ck2 = 10 ORDER BY ck2 ASC, ck1 DESC ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.pk == "koko" && ent.ck2 == 10 select new { ent.f1, ent.ck2, ent.ck1 }).OrderBy(c => c.ck2).OrderByDescending(c => c.ck1).ToString(),
                @"SELECT f1, ck2, ck1 FROM t WHERE pk = 'koko' AND ck2 = 10 ORDER BY ck2 ASC, ck1 DESC ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where CqlToken.Create(ent.pk, ent.ck2, ent.ck2) > CqlToken.Create("x", 2) select new { ent.f1, ent.ck2, ent.ck1 }).OrderBy(c => c.ck2).OrderByDescending(c => c.ck1).ToString(),
                @"SELECT f1, ck2, ck1 FROM t WHERE token (pk , ck2 , ck2) > token ('x' , 2) ORDER BY ck2 ASC, ck1 DESC ALLOW FILTERING");

            Assert.Equal(
                (from ent in table where ent.ck2 > ent.ck1 select ent).ToString(),
                @"SELECT * FROM t WHERE ck2 > ck1 ALLOW FILTERING");

            Assert.Equal(
                (from ent in table select ent).Count().ToString(),
                @"SELECT count(*) FROM t");

            Assert.Equal(
                (from ent in table select ent).FirstOrDefault().ToString(),
                @"SELECT * FROM t LIMIT 1 ALLOW FILTERING");

            Assert.Equal(
                (from ent in table select ent).First().ToString(),
                @"SELECT * FROM t LIMIT 1 ALLOW FILTERING");

            Assert.Equal(
                (from ent in table select ent).Where(e => e.pk.CompareTo("a") > 0).First().ToString(),
                @"SELECT * FROM t WHERE pk > 'a' LIMIT 1 ALLOW FILTERING");

            try
            {
                Assert.Equal(
                    (from ent in table where ent.pk == "x" || ent.ck2 == 1 select ent).ToString(),
                    @"?");
            }
            catch (CqlLinqNotSupportedException) { }

            Assert.Equal(
                (from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select ent).ToString(),
                @"SELECT * FROM t WHERE ck2 IN (10, 30, 40) ALLOW FILTERING");

            try
            {
                Assert.Equal(
                    (from ent in table where new int[] { }.Contains(ent.ck2) select ent).ToString(),
                    @"?");
            }
            catch (CqlArgumentException) { }

            Assert.Equal(
               (from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select ent).Delete().ToString(),
               @"DELETE FROM t WHERE ck2 IN (10, 30, 40)");

            Assert.Equal(
               (table.Insert(new TestTable() { ck1 = 1, ck2 = 2, f1 = 3, pk = "x" })).ToString(),
               @"INSERT INTO t(pk,ck1,ck2,f1) VALUES ('x',1,2,3);");

            try
            {
                Assert.Equal(
                    (from ent in table where new int[] { 10, 30, 40 }.Contains(ent.ck2) select new { x = ent.pk, e = ent }).ToString(),
                    @"?");
            }
            catch (CqlLinqNotSupportedException) { }
        }
    }
}
