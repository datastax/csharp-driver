using System;
using System.Collections.Generic;
using Dev;
using System.Linq;
using System.Threading;
using System.Globalization;
using System.Diagnostics;

namespace Cassandra.Data.Linq.Test
{
    public class TweetsContext : Context
    {
        public TweetsContext(Session session, ConsistencyLevel createConsistencyLevel = ConsistencyLevel.Default)
            : base(session)
        {
            AddTables();
            CreateTablesIfNotExist(createConsistencyLevel);
        }

        private void AddTables()
        {
            AddTable<Tweets>();
        }
    }

    [AllowFiltering]
    public class Tweets
    {        
        [PartitionKey]
        public Guid tweet_id;        
        public string author;
        [SecondaryIndex]
        public string body;
        [ClusteringKey(1)]
        public bool isok;
        
        public int idx;

        public HashSet<string> exampleSet = new HashSet<string>();
    }

    public class BasicTests : IDisposable
    {
        public BasicTests()
        {
        }
        Session session; 
        TweetsContext ents;
        string keyspaceName = "Tweets" + Guid.NewGuid().ToString("N");

        public void SetFixture(Dev.SettingsFixture setFix)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;

            var clusterb = Cluster.Builder().WithConnectionString(setFix.Settings["CassandraConnectionString"]);
            clusterb.WithDefaultKeyspace(keyspaceName);
            var cluster = clusterb.Build();
            session = cluster.ConnectAndCreateDefaultKeyspaceIfNotExists();
            ents = new TweetsContext(session);
        }

        public void Dispose()
        {
            session.DeleteKeyspace(keyspaceName);
            session.Dispose();
        }

        class X
        {
            public string x;
            public int y;
        }

        [Fact]
        public void Test1()
        {
            var table = ents.GetTable<Tweets>();

            int RowsNo = 2000;
            List<Tweets> entL = new List<Tweets>();
            for (int i = 0; i < RowsNo; i++)
            {
                var ent = new Tweets() { tweet_id = Guid.NewGuid(), author = "test" + i.ToString(), body = "body" + i.ToString(), isok = (i % 2 == 0) };
                ent.exampleSet.Add(i.ToString());
                ent.exampleSet.Add((i + 1).ToString());
                ent.exampleSet.Add((i - 1).ToString());
                table.AddNew(ent, EntityTrackingMode.KeepAttachedAfterSave);
                entL.Add(ent);                
            }
            ents.SaveChanges(SaveChangesMode.Batch);

            var cnt = table.Count().Execute();
            
            Dev.Assert.Equal(RowsNo,cnt);
            
            foreach(var ent in entL)
                table.Delete(ent);

            ents.SaveChanges(SaveChangesMode.Batch);

            var cnt2 = table.Count().Execute();
            Dev.Assert.Equal(0, cnt2);
        }


        [Fact]
        public void Test2()
        {
            var table = ents.GetTable<Tweets>();
            int RowsNb = 3000;

            for (int i = 0; i < RowsNb; i++)
                table.AddNew(new Tweets() { tweet_id = Guid.NewGuid(), idx = i, isok = i % 2 == 0, author = "author" + i.ToString(), body = "bla bla bla" });

            ents.SaveChanges(SaveChangesMode.Batch);

            //test filtering
            var evens = (from ent in table where ent.isok == true select ent).Execute();
            Assert.True(evens.All(ev => ev.idx % 2 == 0));


            //test pagination
            int PerPage = 1234;

            var firstPage = (from ent in table select ent).Take(PerPage).Execute();
            var continuation = firstPage.Last().tweet_id.CqlToken();

            int pages = 1;
            int lastcnt = 0;
            while(true)
            {
                var nextPage = (from ent in table where ent.tweet_id.CqlToken() > continuation select ent).Take(PerPage).Execute().ToList();
                if (nextPage.Count < PerPage)
                {
                    lastcnt = nextPage.Count;
                    break;
                }
                continuation = nextPage.Last().tweet_id.CqlToken();
                pages++;
            }

            Assert.Equal(pages, RowsNb / PerPage);
            Assert.Equal(lastcnt, RowsNb % PerPage);
        }

        

        //[Fact]
        public void TestBuffering()
        {           
            var table = ents.GetTable<Tweets>();           

            var q2 = (from e in table where table.Token(e.idx) <= 0 select e).Take(10).OrderBy((e) => e.idx).ThenByDescending((e) => e.isok);

            var qxx = q2.ToString();
            int RowsNb = 10;
            for (int i = 0; i < RowsNb; i++)
            {
                var ent = new Tweets() { tweet_id = Guid.NewGuid(), author = "author" + i.ToString(), isok =i%2 == 0, body = "blablabla", idx = i};
                table.AddNew(ent, EntityTrackingMode.KeepAttachedAfterSave);
            }
            var ent2 = new Tweets() { tweet_id = Guid.NewGuid(), author = "author" + RowsNb+1, isok = false, body = "blablabla", idx = RowsNb + 1 };
            table.AddNew(ent2, EntityTrackingMode.KeepAttachedAfterSave);
            ents.SaveChanges(SaveChangesMode.OneByOne);

            table.Attach(ent2, EntityUpdateMode.ModifiedOnly);
            ent2.author = "Koko";
            ents.SaveChanges(SaveChangesMode.OneByOne);


            var iq = table.Count();
            var c = iq.Execute();


            foreach (var r in (from e in table where e.idx == 0 select e).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where new int[] { 0, 1, 2 }.Contains(e.idx) select new { x = e.idx, y = e.tweet_id }).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.idx == 0 select new { Key = e.idx }).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.idx == 0 select new { Key = e.idx, e.isok }).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.idx == 0 select new X() { x = e.author, y = e.idx }).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.idx == 0 select e.author).Execute())
            {
                var x = r;
            }
        }
    }
}
