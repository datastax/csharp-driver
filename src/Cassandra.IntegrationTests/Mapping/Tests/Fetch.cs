//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.FluentMappings;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class Fetch : SharedClusterTest
    {
        ISession _session;
        private string _uniqueKsName;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
        }

        [SetUp]
        public void TestSetup()
        {
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        /// <summary>
        /// Successfully Fetch mapped records by passing in a static query string
        /// </summary>
        [Test]
        public void Fetch_UsingSelectCqlString()
        {
            Table<Author> table = new Table<Author>(_session, new MappingConfiguration());
            table.Create();

            var mapper = new Mapper(_session, new MappingConfiguration().Define(new FluentUserMapping()));
            List<string> followersForAuthor = new List<string>() { "follower1", "follower2", "" };
            Author expectedAuthor = new Author
            {
                AuthorId = Guid.NewGuid().ToString(),
                Followers = followersForAuthor,
            };

            mapper.Insert(expectedAuthor);
            List<Author> authors = mapper.Fetch<Author>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, authors.Count);
            expectedAuthor.AssertEquals(authors[0]);
        }

        /// <summary>
        /// Successfully Fetch mapped records by passing in a static query string
        /// </summary>
        [Test]
        public void FetchAsync_Using_Select_Cql_And_PageSize()
        {
            var table = new Table<Author>(_session, new MappingConfiguration());
            table.Create();

            var mapper = new Mapper(_session, new MappingConfiguration().Define(new FluentUserMapping()));
            var ids = new[] {Guid.NewGuid().ToString(), Guid.NewGuid().ToString()};

            mapper.Insert(new Author { AuthorId = ids[0] });
            mapper.Insert(new Author { AuthorId = ids[1] });

            List<Author> authors = null;
            mapper.FetchAsync<Author>(Cql.New("SELECT * from " + table.Name).WithOptions(o => o.SetPageSize(int.MaxValue))).ContinueWith(t =>
            {
                authors = t.Result.ToList();
            }).Wait();
            Assert.AreEqual(2, authors.Count);;
            CollectionAssert.AreEquivalent(ids, authors.Select(a => a.AuthorId));
        }

        /// <summary>
        /// Successfully Fetch mapped records by passing in a Cql Object
        /// </summary>
        [Test]
        public void Fetch_UsingCqlObject()
        {
            Table<Author> table = new Table<Author>(_session, new MappingConfiguration());
            table.Create();
            int totalInserts = 10;

            var mapper = new Mapper(_session, new MappingConfiguration().Define(new FluentUserMapping()));
            List<Author> expectedAuthors = Author.GetRandomList(totalInserts);
            foreach (Author expectedAuthor in expectedAuthors)
                mapper.Insert(expectedAuthor);

            Cql cql = new Cql("SELECT * from " + table.Name);
            List<Author> actualAuthors = mapper.Fetch<Author>(cql).ToList();
            Assert.AreEqual(totalInserts, actualAuthors.Count);
            Author.AssertListsContainTheSame(expectedAuthors, actualAuthors);
        }

        /// <summary>
        /// Successfully Fetch mapped records by passing in a Cql Object, 
        /// using all available acceptable consistency levels
        /// </summary>
        [Test]
        public void Fetch_WithConsistencyLevel_Valids()
        {
            Table<Author> table = new Table<Author>(_session, new MappingConfiguration());
            table.Create();
            int totalInserts = 10;

            var mapper = new Mapper(_session, new MappingConfiguration().Define(new FluentUserMapping()));
            List<Author> expectedAuthors = Author.GetRandomList(totalInserts);
            foreach (Author expectedAuthor in expectedAuthors)
                mapper.Insert(expectedAuthor);

            var consistencyLevels = new[]
            {
                ConsistencyLevel.All,
                ConsistencyLevel.LocalOne,
                ConsistencyLevel.LocalQuorum,
                ConsistencyLevel.Quorum,
                ConsistencyLevel.One,
            };
            foreach (var consistencyLevel in consistencyLevels)
            {
                Cql cql = new Cql("SELECT * from " + table.Name).WithOptions(c => c.SetConsistencyLevel(consistencyLevel));
                List<Author> actualAuthors = mapper.Fetch<Author>(cql).ToList();
                Assert.AreEqual(totalInserts, actualAuthors.Count);
                Author.AssertListsContainTheSame(expectedAuthors, actualAuthors);
            }
        }

        /// <summary>
        /// Attempte to Fetch mapped records by passing in a Cql Object, 
        /// using consistency levels that are only valid for writes
        /// </summary>
        [Test]
        public void Fetch_WithConsistencyLevel_Invalids_OnlySupportedForWrites()
        {
            Table<Author> table = new Table<Author>(_session, new MappingConfiguration());
            table.Create();
            int totalInserts = 10;

            var mapper = new Mapper(_session, new MappingConfiguration().Define(new FluentUserMapping()));
            List<Author> expectedAuthors = Author.GetRandomList(totalInserts);
            foreach (Author expectedAuthor in expectedAuthors)
                mapper.Insert(expectedAuthor);

            Cql cql = new Cql("SELECT * from " + table.Name).WithOptions(c => c.SetConsistencyLevel(ConsistencyLevel.Any));
            var err = Assert.Throws<InvalidQueryException>(() => mapper.Fetch<Author>(cql).ToList());
            Assert.AreEqual("ANY ConsistencyLevel is only supported for writes", err.Message);

            if (CassandraVersion < Version.Parse("3.0.0"))
            {
                cql = new Cql("SELECT * from " + table.Name).WithOptions(c => c.SetConsistencyLevel(ConsistencyLevel.EachQuorum));
                err = Assert.Throws<InvalidQueryException>(() => mapper.Fetch<Author>(cql).ToList());
                Assert.AreEqual("EACH_QUORUM ConsistencyLevel is only supported for writes", err.Message);
            }
        }

        /// <summary>
        /// Successfully Fetch mapped records by passing in a Cql Object, 
        /// with consistency level set larger than available node count.
        /// Assert expected failure message.
        /// </summary>
        [Test]
        public void Fetch_WithConsistencyLevel_Invalids_NotEnoughReplicas()
        {
            Table<Author> table = new Table<Author>(_session, new MappingConfiguration());
            table.Create();
            int totalInserts = 10;

            var mapper = new Mapper(_session, new MappingConfiguration().Define(new FluentUserMapping()));
            List<Author> expectedAuthors = Author.GetRandomList(totalInserts);
            foreach (Author expectedAuthor in expectedAuthors)
                mapper.Insert(expectedAuthor);

            Cql cql = new Cql("SELECT * from " + table.Name).WithOptions(c => c.SetConsistencyLevel(ConsistencyLevel.Two));
            var err = Assert.Throws<UnavailableException>(() => mapper.Fetch<Author>(cql));
            Assert.AreEqual("Not enough replicas available for query at consistency Two (2 required but only 1 alive)", err.Message);

            cql = new Cql("SELECT * from " + table.Name).WithOptions(c => c.SetConsistencyLevel(ConsistencyLevel.Three));
            err = Assert.Throws<UnavailableException>(() => mapper.Fetch<Author>(cql));
            Assert.AreEqual("Not enough replicas available for query at consistency Three (3 required but only 1 alive)", err.Message);
        }

        /// <summary>
        /// When a List of strings is uploaded as null, it is downloaded as an empty list
        /// </summary>
        [Test]
        public void Fetch_ListUploadedAsNull()
        {
            Table<Author> table = new Table<Author>(_session, new MappingConfiguration());
            table.Create();
            var mapper = new Mapper(_session, new MappingConfiguration().Define(new FluentUserMapping()));
            Author expectedAuthor = Author.GetRandom();
            expectedAuthor.Followers = null;
            mapper.Insert(expectedAuthor);

            // Assert values from cql query
            Author expectedAuthorFromQuery = new Author
            {
                Followers = new List<string>(), // not null
                AuthorId = expectedAuthor.AuthorId,
            };

            Cql cql = new Cql("SELECT * from " + table.Name);
            List<Author> actualAuthors = mapper.Fetch<Author>(cql).ToList();
            Assert.AreEqual(1, actualAuthors.Count);
            expectedAuthorFromQuery.AssertEquals(actualAuthors[0]);
        }

        /// <summary>
        /// Page through results from a mapped Fetch request
        /// 
        /// @Jira CSHARP-215 https://datastax-oss.atlassian.net/browse/CSHARP-215
        /// </summary>
        [Test]
        public void Fetch_Lazy()
        {
            Table<Author> table = new Table<Author>(_session, new MappingConfiguration());
            table.Create();

            var mapper = new Mapper(_session, new MappingConfiguration().Define(new FluentUserMapping()));
            List<Author> expectedAuthors = Author.GetRandomList(100);
            foreach (Author expectedAuthor in expectedAuthors)
                mapper.Insert(expectedAuthor);

            // wait until all records are available to be fetched: 
            List<Author> authors = mapper.Fetch<Author>("SELECT * from " + table.Name).ToList();
            DateTime futureDateTime = DateTime.Now.AddSeconds(10);
            while (authors.Count < expectedAuthors.Count && DateTime.Now < futureDateTime)
                authors = mapper.Fetch<Author>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(expectedAuthors.Count, authors.Count, "Setup FAIL: Less than expected number of authors uploaded");

            Cql cql = new Cql("SELECT * from " + table.Name).WithOptions(o => o.SetConsistencyLevel(ConsistencyLevel.Quorum));
            List<Author> authorsFetchedAndSaved = new List<Author>();
            var authorsFetched = mapper.Fetch<Author>(cql).GetEnumerator();
            while (authorsFetched.MoveNext())
                authorsFetchedAndSaved.Add(authorsFetched.Current);

            Assert.AreEqual(expectedAuthors.Count, authorsFetchedAndSaved.Count);
            foreach (var authorFetched in authorsFetchedAndSaved)
            {
                Author.AssertListContains(expectedAuthors, authorFetched);
            }
        }

        /// <summary>
        /// Page through results from a mapped FetchPage request
        /// 
        /// @Jira CSHARP-262 https://datastax-oss.atlassian.net/browse/CSHARP-262
        /// </summary>
        [Test]
        public void FetchPage_Manual_Explicit()
        {
            const int totalLength = 100;
            Table<Author> table = new Table<Author>(_session, new MappingConfiguration());
            table.Create();

            var mapper = new Mapper(_session, new MappingConfiguration().Define(new FluentUserMapping()));
            List<Author> expectedAuthors = Author.GetRandomList(totalLength);
            foreach (Author expectedAuthor in expectedAuthors)
            {
                mapper.Insert(expectedAuthor);
            }

            var ids = new HashSet<string>();
            byte[] pagingState = null;
            var safeCounter = 0;
            do
            {
                IPage<Author> authors = mapper.FetchPage<Author>(10, pagingState, "SELECT * from " + table.Name);
                foreach (var a in authors)
                {
                    ids.Add(a.AuthorId);
                }
                pagingState = authors.PagingState;
            } while (pagingState != null && safeCounter++ < 100);

            Assert.AreEqual(totalLength, ids.Count);
        }

        /// <summary>
        /// Page through results from a mapped FetchPage with query options
        /// 
        /// @Jira CSHARP-262 https://datastax-oss.atlassian.net/browse/CSHARP-262
        /// </summary>
        [Test]
        public void FetchPage_Manual_WithQueryOptions()
        {
            const int totalLength = 100;
            const int pageSize = 10;
            Table<Author> table = new Table<Author>(_session, new MappingConfiguration());
            table.Create();

            var mapper = new Mapper(_session, new MappingConfiguration().Define(new FluentUserMapping()));
            List<Author> expectedAuthors = Author.GetRandomList(totalLength);
            foreach (Author expectedAuthor in expectedAuthors)
            {
                mapper.Insert(expectedAuthor);
            }

            var ids = new HashSet<string>();
            byte[] pagingState = null;
            var safeCounter = 0;
            do
            {
                var state = pagingState;
                IPage<Author> authors = mapper.FetchPage<Author>(Cql.New("SELECT * from " + table.Name).WithOptions(opt => opt.SetPageSize(pageSize).SetPagingState(state)));
                foreach (var a in authors)
                {
                    ids.Add(a.AuthorId);
                }
                Assert.LessOrEqual(authors.Count, pageSize);
                pagingState = authors.PagingState;
            } while (pagingState != null && safeCounter++ < 100);

            Assert.AreEqual(totalLength, ids.Count);
        }

        [Test, TestCassandraVersion(2, 1, 0)]
        public void Fetch_With_Udt()
        {
            var mapper = new Mapper(_session, new MappingConfiguration());
            _session.Execute("CREATE TYPE song (id uuid, title text, artist text)");
            _session.Execute("CREATE TABLE albums (id uuid primary key, name text, songs list<frozen<song>>)");
            _session.UserDefinedTypes.Define(UdtMap.For<Cassandra.Tests.Mapping.Pocos.Song>());
            _session.Execute("INSERT INTO albums (id, name, songs) VALUES (uuid(), 'Legend', [{id: uuid(), title: 'Africa Unite', artist: 'Bob Marley'}])");
            var result = mapper.Fetch<Cassandra.Tests.Mapping.Pocos.Album>("SELECT * from albums LIMIT 1").ToList();
            Assert.AreEqual(1, result.Count);
            var album = result[0];
            Assert.AreEqual("Legend", album.Name);
            Assert.AreEqual(1, album.Songs.Count);
            var song = album.Songs[0];
            Assert.AreEqual("Bob Marley", song.Artist);
            Assert.AreEqual("Africa Unite", song.Title);
        }

        [Test]
        public void Fetch_Struct_Value_Should_Default()
        {
            _session.Execute("CREATE TABLE tbl_with_structs (id uuid primary key, bool_sample boolean, timestamp_sample timestamp)");
            var map = new Map<AllTypesEntity>()
                .Column(p => p.BooleanValue, c => c.WithName("bool_sample"))
                .Column(p => p.UuidValue, c => c.WithName("id"))
                .PartitionKey(p => p.UuidValue)
                .ExplicitColumns();
            var id = Guid.NewGuid();
            var query = string.Format("INSERT INTO tbl_with_structs (id, bool_sample, timestamp_sample) VALUES ({0}, null, null)", id);
            _session.Execute(query);
            var mapper = new Mapper(_session, new MappingConfiguration().Define(map));
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            var row = mapper.Fetch<AllTypesEntity>("SELECT * FROM tbl_with_structs WHERE id = ?", id).First();
            Assert.NotNull(row);
            Assert.AreEqual(row.UuidValue, id);
            Assert.AreEqual(row.BooleanValue, default(bool));
            Assert.AreEqual(row.DateTimeValue, default(DateTime));
        }

        [Test]
        public void Fetch_TimeUuid_Set()
        {
            _session.Execute("CREATE TABLE tbl_timeuuid_sortedset (id int primary key, my_set set<timeuuid>)");
            var map = new Map<PocoWithCollections>()
                .Column(p => p.Id)
                .Column(p => p.SortedSetTimeUuid, c => c.WithName("my_set"))
                .PartitionKey(p => p.Id)
                .TableName("tbl_timeuuid_sortedset")
                .ExplicitColumns();
            var mapper = new Mapper(_session, new MappingConfiguration().Define(map));
            var inserted = new PocoWithCollections
            {
                Id = 1,
                SortedSetTimeUuid = new SortedSet<TimeUuid> { TimeUuid.NewId(), TimeUuid.NewId() }
            };
            mapper.Insert(inserted);
            var retrieved = mapper.Fetch<PocoWithCollections>("WHERE id = ?", inserted.Id).First();
            CollectionAssert.AreEqual(inserted.SortedSetTimeUuid, retrieved.SortedSetTimeUuid);
        }

        [Test]
        public void Fetch_TimeUuid_Map_Key_Sorted()
        {
            _session.Execute("CREATE TABLE tbl_timeuuid_map (id int primary key, my_map map<timeuuid, text>)");
            var map = new Map<PocoWithCollections>()
                .Column(p => p.Id)
                .Column(p => p.SortedDictionaryTimeUuidString, c => c.WithName("my_map"))
                .PartitionKey(p => p.Id)
                .TableName("tbl_timeuuid_map")
                .ExplicitColumns();
            var mapper = new Mapper(_session, new MappingConfiguration().Define(map));
            var inserted = new PocoWithCollections
            {
                Id = 1,
                SortedDictionaryTimeUuidString = new SortedDictionary<TimeUuid, string> { { TimeUuid.NewId(), "one" } }
            };
            mapper.Insert(inserted);
            var retrieved = mapper.Fetch<PocoWithCollections>("WHERE id = ?", inserted.Id).First();
            CollectionAssert.AreEqual(inserted.SortedDictionaryTimeUuidString, retrieved.SortedDictionaryTimeUuidString);
        }

        [Test]
        public void Fetch_TimeUuid_Map_Key()
        {
            _session.Execute("CREATE TABLE tbl_timeuuid_map2 (id int primary key, my_map map<timeuuid, text>)");
            var map = new Map<PocoWithCollections>()
                .Column(p => p.Id)
                .Column(p => p.DictionaryTimeUuidString, c => c.WithName("my_map"))
                .PartitionKey(p => p.Id)
                .TableName("tbl_timeuuid_map2")
                .ExplicitColumns();
            var mapper = new Mapper(_session, new MappingConfiguration().Define(map));
            var inserted = new PocoWithCollections
            {
                Id = 1,
                DictionaryTimeUuidString = new Dictionary<TimeUuid, string> { { TimeUuid.NewId(), "one" } }
            };
            mapper.Insert(inserted);
            var retrieved = mapper.Fetch<PocoWithCollections>("WHERE id = ?", inserted.Id).First();
            CollectionAssert.AreEqual(inserted.DictionaryTimeUuidString, retrieved.DictionaryTimeUuidString);
        }
    }
}
