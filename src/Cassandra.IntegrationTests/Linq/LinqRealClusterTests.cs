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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq
{
    /// <summary>
    /// No support for paging state and traces in simulacron yet. Also haven't implemented an abstraction to prime UDTs yet.
    /// </summary>
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class LinqRealClusterTests : SharedClusterTest
    {
        private ISession _session;
        private readonly string _tableName = TestUtils.GetUniqueTableName().ToLower();
        private readonly string _tableNameAlbum = TestUtils.GetUniqueTableName().ToLower();
        private readonly MappingConfiguration _mappingConfig = new MappingConfiguration().Define(new Map<Song>().PartitionKey(s => s.Id));
        private Table<Movie> _movieTable;
        private const int TotalRows = 100;
        private readonly List<Movie> _movieList = Movie.GetDefaultMovieList();
        private readonly string _udtName = $"udt_song_{Randomm.RandomAlphaNum(12)}";
        private readonly Guid _sampleId = Guid.NewGuid();

        private Table<Song> GetTable()
        {
            return new Table<Song>(_session, _mappingConfig, _tableName, KeyspaceName);
        }

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            var table = GetTable();
            table.Create();
            var tasks = new List<Task>();
            for (var i = 0; i < LinqRealClusterTests.TotalRows; i++)
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

            var movieMappingConfig = new MappingConfiguration();
            _movieTable = new Table<Movie>(_session, movieMappingConfig);
            _movieTable.Create();
            
            //Insert some data
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();
        }

        [SetUp]
        public void SetUp()
        {
            Session.Execute($"CREATE TYPE IF NOT EXISTS {_udtName} (id uuid, title text, artist text)");
            Session.UserDefinedTypes.Define(UdtMap.For<Song2>(_udtName));
            Session.Execute($"CREATE TABLE IF NOT EXISTS {_tableNameAlbum} (id uuid primary key, name text, songs list<frozen<{_udtName}>>, publishingdate timestamp)");
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
            while (page.PagingState != null && safeCounter++ < LinqRealClusterTests.TotalRows)
            {
                page = table.SetPagingState(page.PagingState).ExecutePaged();
                Assert.LessOrEqual(page.Count, pageSize);
                foreach (var s in page)
                {
                    fullList.Add(s.Id);
                }
            }
            Assert.AreEqual(LinqRealClusterTests.TotalRows, fullList.Count);
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
            while (page.PagingState != null && safeCounter++ < LinqRealClusterTests.TotalRows)
            {
                page = table.Where(s => CqlFunction.Token(s.Id) > long.MinValue).SetPageSize(pageSize).SetPagingState(page.PagingState).ExecutePaged();
                Assert.LessOrEqual(page.Count, pageSize);
                foreach (var s in page)
                {
                    fullList.Add(s.Id);
                }
            }
            Assert.AreEqual(LinqRealClusterTests.TotalRows, fullList.Count);
        }

        [Test]
        public void LinqWhere_ExecuteSync_Trace()
        {
            var expectedMovie = _movieList.First();

            // test
            var linqWhere = _movieTable.Where(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker);
            linqWhere.EnableTracing();
            var movies = linqWhere.Execute().ToList();
            Assert.AreEqual(1, movies.Count);
            var actualMovie = movies.First();
            Movie.AssertEquals(expectedMovie, actualMovie);
            var trace = linqWhere.QueryTrace;
            Assert.NotNull(trace);
            Assert.AreEqual(TestCluster.InitialContactPoint, trace.Coordinator.ToString());
        }

        [Test, TestCassandraVersion(2, 1)]
        public void CreateTable_With_Frozen_Udt()
        {
            var config = new MappingConfiguration().Define(new Map<UdtAndTuplePoco>()
                .PartitionKey(p => p.Id1)
                .Column(p => p.Id1)
                .Column(p => p.Udt1, cm => cm.WithName("u").AsFrozen())
                .TableName("tbl_frozen_udt")
                .ExplicitColumns());
            Session.Execute("CREATE TYPE IF NOT EXISTS song (title text, releasedate timestamp, artist text)");
            Session.UserDefinedTypes.Define(UdtMap.For<Song>());
            var table = new Table<UdtAndTuplePoco>(Session, config);
            table.Create();
            var tableMeta = Cluster.Metadata.GetTable(KeyspaceName, "tbl_frozen_udt");
            Assert.AreEqual(2, tableMeta.TableColumns.Length);
            var column = tableMeta.ColumnsByName["u"];
            Assert.AreEqual(ColumnTypeCode.Udt, column.TypeCode);
        }

        [Test, TestCassandraVersion(2, 1)]
        public void CreateTable_With_Frozen_Key()
        {
            var config = new MappingConfiguration().Define(new Map<UdtAndTuplePoco>()
                .PartitionKey(p => p.Id1)
                .Column(p => p.Id1)
                .Column(p => p.UdtSet1, cm => cm.WithFrozenKey().WithName("s"))
                .Column(p => p.TupleMapKey1, cm => cm.WithFrozenKey().WithName("m"))
                .TableName("tbl_frozen_key")
                .ExplicitColumns());
            Session.Execute("CREATE TYPE IF NOT EXISTS song (title text, releasedate timestamp, artist text)");
            Session.UserDefinedTypes.Define(UdtMap.For<Song>());
            var table = new Table<UdtAndTuplePoco>(Session, config);
            table.Create();
            var tableMeta = Cluster.Metadata.GetTable(KeyspaceName, "tbl_frozen_key");
            Assert.AreEqual(3, tableMeta.TableColumns.Length);
            var column = tableMeta.ColumnsByName["s"];
            Assert.AreEqual(ColumnTypeCode.Set, column.TypeCode);
            column = tableMeta.ColumnsByName["m"];
            Assert.AreEqual(ColumnTypeCode.Map, column.TypeCode);
        }

        [Test, TestCassandraVersion(2, 1)]
        public void CreateTable_With_Frozen_Value()
        {
            var config = new MappingConfiguration().Define(new Map<UdtAndTuplePoco>()
                .PartitionKey(p => p.Id1)
                .Column(p => p.Id1)
                .Column(p => p.ListMapValue1, cm => cm.WithFrozenValue().WithName("m"))
                .Column(p => p.UdtList1, cm => cm.WithFrozenValue().WithName("l"))
                .TableName("tbl_frozen_value")
                .ExplicitColumns());
            Session.Execute("CREATE TYPE IF NOT EXISTS song (title text, releasedate timestamp, artist text)");
            Session.UserDefinedTypes.Define(UdtMap.For<Song>());
            var table = new Table<UdtAndTuplePoco>(Session, config);
            table.Create();
            var tableMeta = Cluster.Metadata.GetTable(KeyspaceName, "tbl_frozen_value");
            Assert.AreEqual(3, tableMeta.TableColumns.Length);
            var column = tableMeta.ColumnsByName["l"];
            Assert.AreEqual(ColumnTypeCode.List, column.TypeCode);
            column = tableMeta.ColumnsByName["m"];
            Assert.AreEqual(ColumnTypeCode.Map, column.TypeCode);
        }
        [Test, TestCassandraVersion(2, 1, 0)]
        public void LinqUdt_Select()
        {
            // Avoid interfering with other tests
            Session.Execute(
                new SimpleStatement(
                    $"INSERT INTO {_tableNameAlbum} (id, name, songs) VALUES (?, 'Legend', [{{id: uuid(), title: 'Africa Unite', artist: 'Bob Marley'}}])",
                    _sampleId));

            var table = GetAlbumTable();
            var album = table.Select(a => new Album { Id = a.Id, Name = a.Name, Songs = a.Songs })
                             .Where(a => a.Id == _sampleId).Execute().First();
            Assert.AreEqual(_sampleId, album.Id);
            Assert.AreEqual("Legend", album.Name);
            Assert.NotNull(album.Songs);
            Assert.AreEqual(1, album.Songs.Count);
            var song = album.Songs[0];
            Assert.AreEqual("Africa Unite", song.Title);
            Assert.AreEqual("Bob Marley", song.Artist);
        }

        [Test, TestCassandraVersion(2,1,0)]
        public void LinqUdt_Insert()
        {
            // Avoid interfering with other tests
            var table = GetAlbumTable();
            var id = Guid.NewGuid();
            var album = new Album
            {
                Id = id,
                Name = "Mothership",
                PublishingDate = DateTimeOffset.Parse("2010-01-01"),
                Songs = new List<Song2>
                {
                    new Song2
                    {
                        Id = Guid.NewGuid(),
                        Artist = "Led Zeppelin",
                        Title = "Good Times Bad Times"
                    },
                    new Song2
                    {
                        Id = Guid.NewGuid(),
                        Artist = "Led Zeppelin",
                        Title = "Communication Breakdown"
                    }
                }
            };
            table.Insert(album).Execute();
            //Check that the values exists using core driver
            var row = Session.Execute(new SimpleStatement($"SELECT * FROM {_tableNameAlbum} WHERE id = ?", id)).First();
            Assert.AreEqual("Mothership", row.GetValue<object>("name"));
            var songs = row.GetValue<List<Song2>>("songs");
            Assert.NotNull(songs);
            Assert.AreEqual(2, songs.Count);
            Assert.NotNull(songs.FirstOrDefault(s => s.Title == "Good Times Bad Times"));
            Assert.NotNull(songs.FirstOrDefault(s => s.Title == "Communication Breakdown"));
        }

        [Test, TestCassandraVersion(2,1,0)]
        public void LinqUdt_Where_Contains()
        {
            var songRecordsName = "song_records";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {songRecordsName} (id uuid, song frozen<{_udtName}>, broadcast int, primary key ((id), song))");

            var table = new Table<SongRecords>(Session, new MappingConfiguration().Define(new Map<SongRecords>().TableName(songRecordsName)));
            var song = new Song2
            {
                Id = Guid.NewGuid(),
                Artist = "Led Zeppelin",
                Title = "Good Times Bad Times"
            };
            var songs = new List<Song2> {song, new Song2 {Id = Guid.NewGuid(), Artist = "Led Zeppelin", Title = "Whola Lotta Love"}};
            var id = Guid.NewGuid();
            var songRecord = new SongRecords()
            {
                Id = id,
                Song = song,
                Broadcast = 100
            };
            table.Insert(songRecord).Execute();
            var records = table.Where(sr => sr.Id == id && songs.Contains(sr.Song)).Execute();
            Assert.NotNull(records);
            var recordsArr = records.ToArray();
            Assert.AreEqual(1, recordsArr.Length);
            Assert.NotNull(recordsArr[0].Song);
            Assert.AreEqual(song.Id, recordsArr[0].Song.Id);
            Assert.AreEqual(song.Artist, recordsArr[0].Song.Artist);
            Assert.AreEqual(song.Title, recordsArr[0].Song.Title);
        }

        private Table<Album> GetAlbumTable()
        {
            return new Table<Album>(Session, new MappingConfiguration().Define(new Map<Album>().TableName(_tableNameAlbum)));
        }

        internal class SongRecords
        {
            public Guid Id { get; set; }
            public Song2 Song { get; set; }
            public int Broadcast { get; set; }
        }
    }
}