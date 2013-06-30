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
    public class FoundBugTests
    {
        private string KeyspaceName = "test";

        Session Session;

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMBridge.ReusableCCMCluster.Setup(2);
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder());
            Session = CCMBridge.ReusableCCMCluster.Connect("tester");
            Session.CreateKeyspaceIfNotExists(KeyspaceName);
            Session.ChangeKeyspace(KeyspaceName);
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMBridge.ReusableCCMCluster.Drop();
        }


        [AllowFiltering]
        [Table("test1")]
        public class TestTable
        {
            [PartitionKey(1)]
            [Column("user")]
            public int UserId
            { get; set; }

            [PartitionKey(2)]
            [Column("date")]
            public int Date { get; set; }
            [ClusteringKey(1)]

            [Column("time")]
            public long Token
            { get; set; }
        }


        [TestMethod]
        [WorksForMe]
        //https://datastax-oss.atlassian.net/browse/CSHARP-43
        //LINQ query with multiple "where" generate wrong cql and it is failed to execute
        public void Bug_CSHARP_43()
        {

            var userId = 1;
            var date = 2;
            var time = 3;

            var table = Session.GetTable<TestTable>();
            table.CreateIfNotExists();

            table.Insert(new TestTable() { UserId = 1, Date = 2, Token = 1 }).Execute();
            table.Insert(new TestTable() { UserId = 1, Date = 2, Token = 2 }).Execute();
            table.Insert(new TestTable() { UserId = 1, Date = 2, Token = 3 }).Execute();
            table.Insert(new TestTable() { UserId = 1, Date = 2, Token = 4 }).Execute();
            table.Insert(new TestTable() { UserId = 1, Date = 2, Token = 5 }).Execute();

            var query = table.Where(i => i.UserId == userId && i.Date == date);

            var query2 = query.Where(i => i.Token >= time); query2 = query2.OrderBy(i => i.Token);

            var query3 = query.Where(i => i.Token <= time); query3 = query3.OrderByDescending(i => i.Token);

            Assert.Equal("SELECT * FROM test1 WHERE user = 1 AND date = 2 ALLOW FILTERING", query.ToString());
            Assert.Equal("SELECT * FROM test1 WHERE user = 1 AND date = 2 AND time >= 3 ORDER BY time ASC ALLOW FILTERING", query2.ToString());
            Assert.Equal("SELECT * FROM test1 WHERE user = 1 AND date = 2 AND time <= 3 ORDER BY time DESC ALLOW FILTERING", query3.ToString());

            var result2 = query2.Execute().ToList();
            var result3 = query3.Execute().ToList();

            Assert.Equal(3, result2.First().Token);
            Assert.Equal(5, result2.Last().Token);
            Assert.Equal(3, result3.First().Token);
            Assert.Equal(1, result3.Last().Token);

        }
    }
}
