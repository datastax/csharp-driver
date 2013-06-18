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
#endif

namespace Cassandra.Data.Linq.MSTest
{
    [TestClass]
    public class FoundBugTests
    {
        private string KeyspaceName = "test";

        Session session;
        CCMBridge.CCMCluster CCMCluster;
        Cluster Cluster;

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMCluster = CCMBridge.CCMCluster.Create(2, Cluster.Builder());
            session = CCMCluster.Session;
            Cluster = CCMCluster.Cluster;
            session.CreateKeyspaceIfNotExists(KeyspaceName);
            session.ChangeKeyspace(KeyspaceName);
        }

        [TestCleanup]
        public void Dispose()
        {
            if (CCMCluster != null)
                CCMCluster.Discard();
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

            var table = session.GetTable<TestTable>();
            table.CreateIfNotExists();

            var query = table.Where(i => i.UserId == userId && i.Date == date);

            var query2 = query.Where(i => i.Token >= time); query2 = query2.OrderBy(i => i.Token); 

            var query3 = query.Where(i => i.Token <= time); query3 = query3.OrderByDescending(i => i.Token);

            Assert.Equal("SELECT * FROM test1 WHERE user = 1 AND date = 2 ALLOW FILTERING", query.CqlString());
            Assert.Equal("SELECT * FROM test1 WHERE user = 1 AND date = 2 AND time >= 3 ORDER BY time ASC ALLOW FILTERING", query2.CqlString());
            Assert.Equal("SELECT * FROM test1 WHERE user = 1 AND date = 2 AND time <= 3 ORDER BY time DESC ALLOW FILTERING", query3.CqlString());

            var result = query.Execute();

        }
    }
}
