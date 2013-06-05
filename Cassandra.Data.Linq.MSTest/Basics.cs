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
    public partial class BasicLinqTests
    {
        public class TweetsContext : Context
        {
            public TweetsContext(Session session, ConsistencyLevel createConsistencyLevel = ConsistencyLevel.Default)
                : base(session)
            {
                AddTables();
                CreateTablesIfNotExist(createConsistencyLevel);
                session.Cluster.WaitForSchemaAgreement();
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

            [ClusteringKey(2)]
            public int idx;

            public HashSet<string> exampleSet = new HashSet<string>();
        }


        Session session;
        CCMBridge.CCMCluster CCMCluster;
        Cluster Cluster;

        TweetsContext ents;
        string keyspaceName = "Tweets" + Guid.NewGuid().ToString("N");

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMCluster = CCMBridge.CCMCluster.Create(2, Cluster.Builder());
            session = CCMCluster.Session;
            Cluster = CCMCluster.Cluster;
            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.Cluster.WaitForSchemaAgreement();
            session.ChangeKeyspace(keyspaceName);
            ents = new TweetsContext(session);
        }

        [TestCleanup]
        public void Dispose()
        {
            if(CCMCluster!=null)
                CCMCluster.Discard();
        }

        class X
        {
            public string x;
            public int y;
        }



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

            Assert.Equal(RowsNo, cnt);

            foreach (var ent in entL)
                table.Delete(ent);

            ents.SaveChanges(SaveChangesMode.Batch);

            var cnt2 = table.Count().Execute();
            Assert.Equal(0, cnt2);
        }


        public void testPagination()
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
            var continuation = CqlToken.Create(firstPage.Last().tweet_id);

            int pages = 1;
            int lastcnt = 0;
            while (true)
            {
                var nextPage = (from ent in table where CqlToken.Create(ent.tweet_id) > continuation select ent).Take(PerPage).Execute().ToList();
                if (nextPage.Count < PerPage)
                {
                    lastcnt = nextPage.Count;
                    break;
                }
                continuation = CqlToken.Create(nextPage.Last().tweet_id);
                pages++;
            }

            Assert.Equal(pages, RowsNb / PerPage);
            Assert.Equal(lastcnt, RowsNb % PerPage);
        }

        public void testBuffering()
        {
            var table = ents.GetTable<Tweets>();

            var q2 = (from e in table where CqlToken.Create(e.idx) <= 0 select e).Take(10).OrderBy((e) => e.idx).ThenByDescending((e) => e.isok);

            var qxx = q2.ToString();
            int RowsNb = 10;
            for (int i = 0; i < RowsNb; i++)
            {
                var ent = new Tweets() { tweet_id = Guid.NewGuid(), author = "author" + i.ToString(), isok = i % 2 == 0, body = "blablabla", idx = i };
                table.AddNew(ent, EntityTrackingMode.KeepAttachedAfterSave);
            }
            var ent2 = new Tweets() { tweet_id = Guid.NewGuid(), author = "author" + RowsNb + 1, isok = false, body = "blablabla", idx = RowsNb + 1 };
            table.AddNew(ent2, EntityTrackingMode.KeepAttachedAfterSave);
            ents.SaveChanges(SaveChangesMode.OneByOne);

            table.Attach(ent2, EntityUpdateMode.ModifiedOnly);
            ent2.author = "Koko";
            ents.SaveChanges(SaveChangesMode.OneByOne);


            var iq = table.Count();
            var c = iq.Execute();


            foreach (var r in (from e in table where e.isok == true && e.idx == 0 select e).Execute())
            {
                var x = r;
            }

            //https://issues.apache.org/jira/browse/CASSANDRA-5303?page=com.atlassian.streams.streams-jira-plugin:activity-stream-issue-tab
            //foreach (var r in (from e in table where e.isok == true && new int[] { 0, 1, 2 }.Contains(e.idx) select new { x = e.idx, y = e.tweet_id }).Execute())
            //{
            //    var x = r;
            //}

            foreach (var r in (from e in table where e.isok == false && e.idx == 0 select new { Key = e.idx }).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.isok == true && e.idx == 0 select new { Key = e.idx, e.isok }).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.isok == true && e.idx == 0 select new X() { x = e.author, y = e.idx }).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.isok == false && e.idx == 0 select e.author).Execute())
            {
                var x = r;
            }
        }

        public void Bug16_JIRA()
        {
            var table = ents.GetTable<Tweets>();

            Guid tweet_ID = Guid.NewGuid();            
            var isok = false;

            table.AddNew(new Tweets() { tweet_id = tweet_ID, author = "author", isok = isok, body = "blablabla", idx = 1 }, EntityTrackingMode.KeepAttachedAfterSave);

            table.Where((a) => a.tweet_id == tweet_ID && a.isok == isok && a.idx == 1).Select((t) => new Tweets { body = "Lorem Ipsum", author = "Anonymous" }).Update().Execute();

            var smth = table.Where(x => x.isok == isok).Execute().ToList();
        }


    }
}

