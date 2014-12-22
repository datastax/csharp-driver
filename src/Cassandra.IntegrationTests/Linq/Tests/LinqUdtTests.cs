using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.Tests
{
    [Category("short")]
    [TestCassandraVersion(2, 1)]
    public class LinqUdtTests : TestGlobals
    {
        ISession _session = null;
        private readonly string _keyspaceName = TestUtils.GetUniqueKeyspaceName();
        private readonly Guid _sampleId = Guid.NewGuid();
        private ITestCluster _testCluster;

        private Table<Album> GetAlbumTable()
        {
            return new Table<Album>(_session, new MappingConfiguration().Define(new Map<Album>().TableName("albums")));
        }

        [TestFixtureSetUp]
        public void SetupTest()
        {
            _testCluster = TestClusterManager.GetTestCluster(1);
            _session = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build().Connect();

            _session.Execute(String.Format(TestUtils.CreateKeyspaceSimpleFormat, _keyspaceName, 1));
            _session.ChangeKeyspace(_keyspaceName);
            _session.Execute("CREATE TYPE song (id uuid, title text, artist text)");
            _session.Execute("CREATE TABLE albums (id uuid primary key, name text, songs list<frozen<song>>, publishingdate timestamp)");
            _session.Execute(new SimpleStatement("INSERT INTO albums (id, name, songs) VALUES (?, 'Legend', [{id: uuid(), title: 'Africa Unite', artist: 'Bob Marley'}])").Bind(_sampleId));
            _session.UserDefinedTypes.Define(UdtMap.For<Song>());
        }

        [TestFixtureTearDown]
        public void TeardownTest()
        {
            _session.Dispose();
        }

        [Test]
        public void LinqUdt_Select()
        {
            var table = new Table<Album>(_session, new MappingConfiguration().Define(new Map<Album>().TableName("albums")));
            var album = table.First(a => a.Id == _sampleId).Execute();
            Assert.AreEqual(_sampleId, album.Id);
            Assert.AreEqual("Legend", album.Name);
            Assert.NotNull(album.Songs);
            Assert.AreEqual(1, album.Songs.Count);
            var song = album.Songs[0];
            Assert.AreEqual("Africa Unite", song.Title);
            Assert.AreEqual("Bob Marley", song.Artist);
        }

        [Test]
        public void LinqUdt_Insert()
        {
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
            var row = _session.Execute(new SimpleStatement("SELECT * FROM albums WHERE id = ?").Bind(id)).First();
            Assert.AreEqual("Mothership", row.GetValue<object>("name"));
            var songs = row.GetValue<List<Song>>("songs");
            Assert.NotNull(songs);
            Assert.AreEqual(2, songs.Count);
            Assert.NotNull(songs.FirstOrDefault(s => s.Title == "Good Times Bad Times"));
            Assert.NotNull(songs.FirstOrDefault(s => s.Title == "Communication Breakdown"));
        }
    }
}
