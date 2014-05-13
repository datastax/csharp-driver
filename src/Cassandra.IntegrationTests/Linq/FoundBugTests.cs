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

using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Cassandra.Data.Linq;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq
{
    [Category("short")]
    public class FoundBugTests : SingleNodeClusterTest
    {
        [Test]
        //https://datastax-oss.atlassian.net/browse/CSHARP-43
        //LINQ query with multiple "where" generate wrong cql and it is failed to execute
        public void Bug_CSHARP_43()
        {
            int userId = 1;
            int date = 2;
            int time = 3;

            Table<TestTable> table = Session.GetTable<TestTable>();
            table.CreateIfNotExists();

            table.Insert(new TestTable {UserId = 1, Date = 2, Token = 1}).Execute();
            table.Insert(new TestTable {UserId = 1, Date = 2, Token = 2}).Execute();
            table.Insert(new TestTable {UserId = 1, Date = 2, Token = 3}).Execute();
            table.Insert(new TestTable {UserId = 1, Date = 2, Token = 4}).Execute();
            table.Insert(new TestTable {UserId = 1, Date = 2, Token = 5}).Execute();

            CqlQuery<TestTable> query = table.Where(i => i.UserId == userId && i.Date == date);

            CqlQuery<TestTable> query2 = query.Where(i => i.Token >= time);
            query2 = query2.OrderBy(i => i.Token);

            CqlQuery<TestTable> query3 = query.Where(i => i.Token <= time);
            query3 = query3.OrderByDescending(i => i.Token);

            Assert.AreEqual("SELECT * FROM \"test1\" WHERE \"user\" = 1 AND \"date\" = 2 ALLOW FILTERING", query.ToString());
            Assert.AreEqual("SELECT * FROM \"test1\" WHERE \"user\" = 1 AND \"date\" = 2 AND \"time\" >= 3 ORDER BY \"time\" ASC ALLOW FILTERING",
                         query2.ToString());
            Assert.AreEqual("SELECT * FROM \"test1\" WHERE \"user\" = 1 AND \"date\" = 2 AND \"time\" <= 3 ORDER BY \"time\" DESC ALLOW FILTERING",
                         query3.ToString());

            List<TestTable> result2 = query2.Execute().ToList();
            List<TestTable> result3 = query3.Execute().ToList();

            Assert.AreEqual(3, result2.First().Token);
            Assert.AreEqual(5, result2.Last().Token);
            Assert.AreEqual(3, result3.First().Token);
            Assert.AreEqual(1, result3.Last().Token);
        }

        [AllowFiltering]
        [Table("test1")]
        public class TestTable
        {
            [PartitionKey(1)]
            [Column("user")]
            public int UserId { get; set; }

            [PartitionKey(2)]
            [Column("date")]
            public int Date { get; set; }

            [ClusteringKey(1)]
            [Column("time")]
            public long Token { get; set; }
        }
    }
}