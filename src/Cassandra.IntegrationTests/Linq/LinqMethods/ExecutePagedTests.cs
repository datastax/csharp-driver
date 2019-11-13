//
//      Copyright (C) DataStax Inc.
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
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using NUnit.Framework;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using Cassandra.Mapping;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short"), Category("realcluster")]
    public class ExecutePagedTests : SharedClusterTest
    {
        private ISession _session;
        private readonly string _keyspace = TestUtils.GetUniqueKeyspaceName().ToLower();
        private readonly string _tableName = TestUtils.GetUniqueTableName().ToLower();
        private readonly MappingConfiguration _mappingConfig = new MappingConfiguration().Define(new Map<Song>().PartitionKey(s => s.Id));
        private const int TotalRows = 100;

        private Table<Song> GetTable()
        {
            return new Table<Song>(_session, _mappingConfig, _tableName, _keyspace);
        }

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _session.CreateKeyspace(_keyspace);
            var table = GetTable();
            table.Create();
            var tasks = new List<Task>();
            for (var i = 0; i < TotalRows; i++)
            {
                tasks.Add(table.Insert(new Song
                {
                    Id = Guid.NewGuid(),
                    Artist = "Artist " + i,
                    Title = "Title " + i,
                    ReleaseDate = DateTimeOffset.Now
                }).ExecuteAsync());
            }
            Assert.True(Task.WaitAll(tasks.ToArray(), 10000));
        }

        [Test]
        public void ExecutePaged_Fetches_Only_PageSize()
        {
            const int pageSize = 10;
            var table = GetTable();
            var page = table.SetPageSize(pageSize).ExecutePaged();
            Assert.AreEqual(pageSize, page.Count);
            Assert.AreEqual(pageSize, page.Count());
        }

        /// <summary>
        /// Checks that while retrieving all the following pages it will get the full original list (unique ids).
        /// </summary>
        [Test]
        public async Task ExecutePaged_Fetches_Following_Pages()
        {
            const int pageSize = 5;
            var table = GetTable();
            var fullList = new HashSet<Guid>();
            var page = await table.SetPageSize(pageSize).ExecutePagedAsync().ConfigureAwait(false);
            Assert.AreEqual(pageSize, page.Count);
            foreach (var s in page)
            {
                fullList.Add(s.Id);
            }
            var safeCounter = 0;
            while (page.PagingState != null && safeCounter++ < TotalRows)
            {
                page = table.SetPagingState(page.PagingState).ExecutePaged();
                Assert.LessOrEqual(page.Count, pageSize);
                foreach (var s in page)
                {
                    fullList.Add(s.Id);
                }
            }
            Assert.AreEqual(TotalRows, fullList.Count);
        }

        [Test]
        public void ExecutePaged_Where_Fetches_Only_PageSize()
        {
            const int pageSize = 10;
            var table = GetTable();
            var page = table.Where(s => CqlFunction.Token(s.Id) > long.MinValue).SetPageSize(pageSize).ExecutePaged();
            Assert.AreEqual(pageSize, page.Count);
            Assert.AreEqual(pageSize, page.Count());
        }

        [Test]
        public void ExecutePaged_Where_Fetches_Following_Pages()
        {
            const int pageSize = 5;
            var table = GetTable();
            var fullList = new HashSet<Guid>();
            var page = table.Where(s => CqlFunction.Token(s.Id) > long.MinValue).SetPageSize(pageSize).ExecutePaged();
            Assert.AreEqual(pageSize, page.Count);
            foreach (var s in page)
            {
                fullList.Add(s.Id);
            }
            var safeCounter = 0;
            while (page.PagingState != null && safeCounter++ < TotalRows)
            {
                page = table.Where(s => CqlFunction.Token(s.Id) > long.MinValue).SetPageSize(pageSize).SetPagingState(page.PagingState).ExecutePaged();
                Assert.LessOrEqual(page.Count, pageSize);
                foreach (var s in page)
                {
                    fullList.Add(s.Id);
                }
            }
            Assert.AreEqual(TotalRows, fullList.Count);
        }
    }
}
