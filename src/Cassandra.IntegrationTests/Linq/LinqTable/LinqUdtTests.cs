using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqTable
{
    [TestFixture, Category("short"), Category("realcluster")]
    [TestCassandraVersion(2,1)]
    public class LinqUdtTests : SharedClusterTest
    {
        private readonly Guid _sampleId = Guid.NewGuid();
        private readonly string _tableName = TestUtils.GetUniqueTableName();
        private readonly string _udtName = $"udt_song_{Randomm.RandomAlphaNum(12)}";

        private Table<Album> GetAlbumTable()
        {
            return new Table<Album>(Session, new MappingConfiguration().Define(new Map<Album>().TableName(_tableName)));
        }

        [SetUp]
        public void Setup()
        {
            Session.Execute($"CREATE TYPE IF NOT EXISTS {_udtName} (id uuid, title text, artist text)");
            Session.UserDefinedTypes.Define(UdtMap.For<Song>(_udtName));
            Session.Execute($"CREATE TABLE IF NOT EXISTS {_tableName} (id uuid primary key, name text, songs list<frozen<{_udtName}>>, publishingdate timestamp)");
        }

        [Test, TestCassandraVersion(2, 1, 0)]
        public void LinqUdt_Select()
        {
            // Avoid interfering with other tests
            Session.Execute(
                new SimpleStatement(
                    $"INSERT INTO {_tableName} (id, name, songs) VALUES (?, 'Legend', [{{id: uuid(), title: 'Africa Unite', artist: 'Bob Marley'}}])",
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
                Songs = new List<Song>
                {
                    new Song
                    {
                        Id = Guid.NewGuid(),
                        Artist = "Led Zeppelin",
                        Title = "Good Times Bad Times"
                    },
                    new Song
                    {
                        Id = Guid.NewGuid(),
                        Artist = "Led Zeppelin",
                        Title = "Communication Breakdown"
                    }
                }
            };
            table.Insert(album).Execute();
            //Check that the values exists using core driver
            var row = Session.Execute(new SimpleStatement($"SELECT * FROM {_tableName} WHERE id = ?", id)).First();
            Assert.AreEqual("Mothership", row.GetValue<object>("name"));
            var songs = row.GetValue<List<Song>>("songs");
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
            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "Led Zeppelin",
                Title = "Good Times Bad Times"
            };
            var songs = new List<Song> {song, new Song {Id = Guid.NewGuid(), Artist = "Led Zeppelin", Title = "Whola Lotta Love"}};
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
    }

    internal class SongRecords
    {
        public Guid Id { get; set; }
        public Song Song { get; set; }
        public int Broadcast { get; set; }
    }
}
