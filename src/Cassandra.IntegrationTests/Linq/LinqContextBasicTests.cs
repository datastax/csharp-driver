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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Cassandra.Data.Linq;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq
{
    [Category("short")]
    public class LinqContextBasicTests : TwoNodesClusterTest
    {
        private TweetsContext ents;

        public override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            ents = new TweetsContext(Session);
        }

        [Test]
        public void LinqContextInsertDeleteSelectTest()
        {
            ContextTable<Tweets> table = ents.GetTable<Tweets>("tweets01");

            var buf = new byte[256];
            for (int i = 0; i < 256; i++)
                buf[i] = (byte) i;

            int RowsNo = 2000;
            var entL = new List<Tweets>();
            for (int i = 0; i < RowsNo; i++)
            {
                var ent = new Tweets {tweet_id = Guid.NewGuid(), author = "test" + i, body = "body" + i, isok = (i%2 == 0)};
                ent.exampleSet.Add(i.ToString(CultureInfo.InvariantCulture));
                ent.exampleSet.Add((i + 1).ToString(CultureInfo.InvariantCulture));
                ent.exampleSet.Add((i - 1).ToString(CultureInfo.InvariantCulture));
                ent.data = buf;
                table.AddNew(ent, EntityTrackingMode.KeepAttachedAfterSave);
                entL.Add(ent);
            }
            ents.SaveChanges(SaveChangesMode.Batch);

            long cnt = table.Count().Execute();

            Assert.AreEqual(RowsNo, cnt);

            byte[] q = (from e in table select e.data).FirstOrDefault().Execute();
            for (int i = 0; i < 256; i++)
                Assert.AreEqual(q[i], (byte) i);


            foreach (Tweets ent in entL)
                table.Delete(ent);

            ents.SaveChanges(SaveChangesMode.Batch);

            long cnt2 = table.Count().Execute();
            Assert.AreEqual(0, cnt2);
        }

        [Test]
        public void LinqContextPaginationTest()
        {
            ContextTable<Tweets> table = ents.GetTable<Tweets>("tweets02");
            int RowsNb = 3000;

            for (int i = 0; i < RowsNb; i++)
                table.AddNew(new Tweets {tweet_id = Guid.NewGuid(), idx = i, isok = i%2 == 0, author = "author" + i, body = "bla bla bla"});

            ents.SaveChanges(SaveChangesMode.Batch);

            //test filtering
            IEnumerable<Tweets> evens = (from ent in table where ent.isok == true select ent).Execute();
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

            Assert.AreEqual(pages, RowsNb/PerPage);
            Assert.AreEqual(lastcnt, RowsNb%PerPage);
        }

        [Test]
        public void LinqContextBufferingTest()
        {
            ContextTable<Tweets> table = ents.GetTable<Tweets>("tweets03");

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


            foreach (Tweets r in (from e in table where e.isok == true && e.idx == 0 select e).Execute())
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

            foreach (var r in (from e in table where e.isok == true && e.idx == 0 select new {Key = e.idx, e.isok}).Execute())
            {
                var x = r;
            }

            foreach (var r in (from e in table where e.isok == true && e.idx == 0 select new {x = e.author, y = e.idx}).Execute())
            {
                var x = r;
            }

            foreach (string r in (from e in table where e.isok == false && e.idx == 0 select e.author).Execute())
            {
                string x = r;
            }
        }

        [Test]
        public void Bug16JiraTest()
        {
            ContextTable<Tweets> table = ents.GetTable<Tweets>("tweets04");

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
            public TweetsContext(ISession session)
                : base(session)
            {
                AddTables();
                CreateTablesIfNotExist();
            }

            private void AddTables()
            {
                AddTable<Tweets>("tweets01");
                AddTable<Tweets>("tweets02");
                AddTable<Tweets>("tweets03");
                AddTable<Tweets>("tweets04");
            }
        }
    }
}