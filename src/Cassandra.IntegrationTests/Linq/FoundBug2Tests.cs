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
using System.Threading;
using Cassandra.Data.Linq;

namespace Cassandra.IntegrationTests.Linq
{
    [TestClass]
    public class FoundBug2Tests
    {
        private ISession Session;
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

        [TestMethod]
        [WorksForMe]
        //https://datastax-oss.atlassian.net/browse/CSHARP-44
        //Linq: Attach entity, update entity values, save changes. Comparison of null property values throws exception
        public void Bug_CSHARP_44()
        {
            ContextTable<Tweets> table = ents.GetTable<Tweets>();

            var buf = new byte[256];
            for (int i = 0; i < 256; i++)
                buf[i] = (byte) i;

            int RowsNo = 100;
            var entL = new List<Tweets>();
            for (int i = 0; i < RowsNo; i++)
            {
                var ent = new Tweets {tweet_id = Guid.NewGuid(), author = "test" + i, body = "body" + i, isok = (i%2 == 0)};
                ent.exampleSet.Add(i.ToString(CultureInfo.InvariantCulture));
                ent.exampleSet.Add((i + 1).ToString(CultureInfo.InvariantCulture));
                ent.exampleSet.Add((i - 1).ToString(CultureInfo.InvariantCulture));
                ent.data = null;
                table.AddNew(ent, EntityTrackingMode.KeepAttachedAfterSave);
                entL.Add(ent);
            }
            ents.SaveChanges(SaveChangesMode.Batch);

            long cnt = table.Count().Execute();

            Assert.Equal(RowsNo, cnt);


            foreach (Tweets enti in entL)
                enti.data = buf;

            ents.SaveChanges(SaveChangesMode.Batch);

            byte[] dat = (from e in table select e.data).FirstOrDefault().Execute();

            Assert.ArrEqual(dat, buf);
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
                AddTable<Tweets>();
            }
        }
    }
}