﻿//
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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Cassandra.Data.Linq;

namespace Cassandra.IntegrationTests.Linq
{
    [TestClass]
    public class BasicLinqTests
    {
        private Session Session;
        private TweetsContext ents;

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMBridge.ReusableCCMCluster.Setup(2);
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder());
            Session = CCMBridge.ReusableCCMCluster.Connect("tester");
            ents = new TweetsContext(Session);
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMBridge.ReusableCCMCluster.Drop();
        }

        public void Test1()
        {
            ContextTable<Tweets> table = ents.GetTable<Tweets>();

            var buf = new byte[256];
            for (int i = 0; i < 256; i++)
                buf[i] = (byte) i;

            int RowsNo = 2000;
            var entL = new List<Tweets>();
            for (int i = 0; i < RowsNo; i++)
            {
                var ent = new Tweets {tweet_id = Guid.NewGuid(), author = "test" + i, body = "body" + i, isok = (i%2 == 0)};
                ent.exampleSet.Add(i.ToString());
                ent.exampleSet.Add((i + 1).ToString());
                ent.exampleSet.Add((i - 1).ToString());
                ent.data = buf;
                table.AddNew(ent, EntityTrackingMode.KeepAttachedAfterSave);
                entL.Add(ent);
            }
            ents.SaveChanges(SaveChangesMode.Batch);

            long cnt = table.Count().Execute();

            Assert.Equal(RowsNo, cnt);

            byte[] q = (from e in table select e.data).FirstOrDefault().Execute();
            for (int i = 0; i < 256; i++)
                Assert.Equal(q[i], (byte) i);


            foreach (Tweets ent in entL)
                table.Delete(ent);

            ents.SaveChanges(SaveChangesMode.Batch);

            long cnt2 = table.Count().Execute();
            Assert.Equal(0, cnt2);
        }


        public void testPagination()
        {
            ContextTable<Tweets> table = ents.GetTable<Tweets>();
            int RowsNb = 3000;

            for (int i = 0; i < RowsNb; i++)
                table.AddNew(new Tweets {tweet_id = Guid.NewGuid(), idx = i, isok = i%2 == 0, author = "author" + i, body = "bla bla bla"});

            ents.SaveChanges(SaveChangesMode.Batch);

            //test filtering
            IEnumerable<Tweets> evens = (from ent in table where ent.isok select ent).Execute();
            Assert.True(evens.All(ev => ev.idx%2 == 0));

            //test pagination
            int PerPage = 1234;

            IEnumerable<Tweets> firstPage = (from ent in table select ent).Take(PerPage).Execute();
            CqlToken continuation = CqlToken.Create(firstPage.Last().tweet_id);

            int pages = 1;
            int lastcnt = 0;
            while (true)
            {
                List<Tweets> nextPage =
                    (from ent in table where CqlToken.Create(ent.tweet_id) > continuation select ent).Take(PerPage).Execute().ToList();
                if (nextPage.Count < PerPage)
                {
                    lastcnt = nextPage.Count;
                    break;
                }
                continuation = CqlToken.Create(nextPage.Last().tweet_id);
                pages++;
            }

            Assert.Equal(pages, RowsNb/PerPage);
            Assert.Equal(lastcnt, RowsNb%PerPage);
        }

        public void testBuffering()
        {
            ContextTable<Tweets> table = ents.GetTable<Tweets>();

            CqlQuery<Tweets> q2 =
                (from e in table where CqlToken.Create(e.idx) <= 0 select e).Take(10).OrderBy(e => e.idx).ThenByDescending(e => e.isok);

            string qxx = q2.ToString();
            int RowsNb = 10;
            for (int i = 0; i < RowsNb; i++)
            {
                var ent = new Tweets {tweet_id = Guid.NewGuid(), author = "author" + i, isok = i%2 == 0, body = "blablabla", idx = i};
                table.AddNew(ent, EntityTrackingMode.KeepAttachedAfterSave);
            }
            var ent2 = new Tweets {tweet_id = Guid.NewGuid(), author = "author" + RowsNb + 1, isok = false, body = "blablabla", idx = RowsNb + 1};
            table.AddNew(ent2, EntityTrackingMode.KeepAttachedAfterSave);
            ents.SaveChanges(SaveChangesMode.OneByOne);

            table.Attach(ent2, EntityUpdateMode.ModifiedOnly);
            ent2.author = "Koko";
            ents.SaveChanges(SaveChangesMode.OneByOne);


            CqlScalar<long> iq = table.Count();
            long c = iq.Execute();


            foreach (Tweets r in (from e in table where e.isok && e.idx == 0 select e).Execute())
            {
                Tweets x = r;
            }

            //https://issues.apache.org/jira/browse/CASSANDRA-5303?page=com.atlassian.streams.streams-jira-plugin:activity-stream-issue-tab
            //foreach (var r in (from e in table where e.isok == true && new int[] { 0, 1, 2 }.Contains(e.idx) select new { x = e.idx, y = e.tweet_id }).Execute())
            //{
            //    var x = r;
            //}

            foreach (var r in (from e in table where e.isok == false && e.idx == 0 select new {Key = e.idx}).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.isok && e.idx == 0 select new {Key = e.idx, e.isok}).Execute())
            {
                var x = r;
            }

            foreach (X r in (from e in table where e.isok && e.idx == 0 select new X {x = e.author, y = e.idx}).Execute())
            {
                X x = r;
            }

            foreach (string r in (from e in table where e.isok == false && e.idx == 0 select e.author).Execute())
            {
                string x = r;
            }
        }

        public void Bug16_JIRA()
        {
            ContextTable<Tweets> table = ents.GetTable<Tweets>();

            Guid tweet_ID = Guid.NewGuid();
            bool isok = false;

            table.AddNew(new Tweets {tweet_id = tweet_ID, author = "author", isok = isok, body = "blablabla", idx = 1},
                         EntityTrackingMode.KeepAttachedAfterSave);

            table.Where(a => a.tweet_id == tweet_ID && a.isok == isok && a.idx == 1)
                 .Select(t => new Tweets {body = "Lorem Ipsum", author = "Anonymous"})
                 .Update()
                 .Execute();

            List<Tweets> smth = table.Where(x => x.isok == isok).Execute().ToList();
        }

        [TestMethod]
        [WorksForMe]
        public void Test()
        {
            Test1();
        }

        [TestMethod]
        [WorksForMe]
        public void TestPagination()
        {
            testPagination();
        }

        [TestMethod]
        [WorksForMe]
        public void TestBuffering()
        {
            testBuffering();
        }

        [TestMethod]
        [WorksForMe]
        public void TestBug16_JIRA()
        {
            Bug16_JIRA();
        }

        [AllowFiltering]
        public class Tweets
        {
            public string author;
            [SecondaryIndex] public string body;

            public byte[] data;
            public HashSet<string> exampleSet = new HashSet<string>();
            [ClusteringKey(2)] public int idx;
            [ClusteringKey(1)] public bool isok;
            [PartitionKey] public Guid tweet_id;
        }

        public class TweetsContext : Context
        {
            public TweetsContext(Session session)
                : base(session)
            {
                AddTables();
                CreateTablesIfNotExist();
            }

            private void AddTables()
            {
                AddTable<Tweets>();
            }
        }

        private class X
        {
            public string x;
            public int y;
        }
    }
}