using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Mapping;
using Cassandra.Tasks;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    [TestFixture]
    public class UpdateTests : MappingTestBase
    {
        [Test]
        public void Update_With_Single_PartitionKey()
        {
            var song = new Song()
            {
                Id = Guid.NewGuid(),
                Artist = "Nirvana",
                ReleaseDate = DateTimeOffset.Now,
                Title = "Come As You Are"
            };
            string query = null;
            object[] parameters = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Callback<IStatement, string>((b, profile) =>
                {
                    query = ((BoundStatement)b).PreparedStatement.Cql;
                    parameters = b.QueryValues;
                })
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((cql, profile) => TestHelper.DelayedTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock, new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id)));
            mapper.Update(song);
            TestHelper.VerifyUpdateCqlColumns("Song", query, new []{"Title", "Artist", "ReleaseDate"},
                new [] {"Id"}, new object[] {song.Title, song.Artist, song.ReleaseDate, song.Id},
                parameters);
            sessionMock.Verify();
        }

        [Test]
        public void Update_With_Multiple_PartitionKeys()
        {
            var song = new Song()
            {
                Id = Guid.NewGuid(),
                Artist = "Nirvana",
                ReleaseDate = DateTimeOffset.Now,
                Title = "In Bloom"
            };
            string query = null;
            object[] parameters = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Callback<IStatement, string>((b, profile) =>
                {
                    query = ((BoundStatement)b).PreparedStatement.Cql;
                    parameters = b.QueryValues;
                })
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((cql, profile) => TestHelper.DelayedTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock, new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Title, s => s.Id)));
            mapper.Update(song);
            
            TestHelper.VerifyUpdateCqlColumns("Song", query, new []{"Artist", "ReleaseDate"},
                new [] {"Title", "Id"}, new object[] {song.Artist, song.ReleaseDate, song.Title, song.Id},
                parameters);
            
            //Different order in the partition key definitions
            mapper = GetMappingClient(sessionMock, new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id, s => s.Title)));
            mapper.Update(song);
            TestHelper.VerifyUpdateCqlColumns("Song", query, new []{"Artist", "ReleaseDate"},
                new [] {"Id", "Title"}, new object[] {song.Artist, song.ReleaseDate, song.Id, song.Title},
                parameters);
            sessionMock.Verify();
        }

        [Test]
        public void Update_With_ClusteringKey()
        {
            var song = new Song()
            {
                Id = Guid.NewGuid(),
                Artist = "Dream Theater",
                ReleaseDate = DateTimeOffset.Now,
                Title = "A Change of Seasons"
            };
            string query = null;
            object[] parameters = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Callback<IStatement, string>((b, profile) =>
                {
                    query = ((BoundStatement)b).PreparedStatement.Cql;
                    parameters = b.QueryValues;
                })
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((cql, profile) => TestHelper.DelayedTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock, new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).ClusteringKey(s => s.ReleaseDate)));
            mapper.Update(song);
            TestHelper.VerifyUpdateCqlColumns("Song", query, new []{"Title", "Artist"},
                new [] {"Id", "ReleaseDate"}, new object[] {song.Title, song.Artist, song.Id, song.ReleaseDate},
                parameters);
            sessionMock.Verify();
        }

        [Test]
        public void Update_Sets_Consistency()
        {
            var song = new Song()
            {
                Id = Guid.NewGuid(),
                Artist = "Dream Theater",
                ReleaseDate = DateTimeOffset.Now,
                Title = "Lines in the Sand"
            };
            ConsistencyLevel? consistency = null;
            ConsistencyLevel? serialConsistency = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Callback<IStatement, string>((b, profile) =>
                {
                    consistency = b.ConsistencyLevel;
                    serialConsistency = b.SerialConsistencyLevel;
                })
                .Returns(TestHelper.DelayedTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((cql, profile) => TestHelper.DelayedTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock, new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Title)));
            mapper.Update(song, new CqlQueryOptions().SetConsistencyLevel(ConsistencyLevel.LocalQuorum));
            Assert.AreEqual(ConsistencyLevel.LocalQuorum, consistency);
            Assert.AreEqual(ConsistencyLevel.Any, serialConsistency);
            mapper.Update(song,
                new CqlQueryOptions().SetConsistencyLevel(ConsistencyLevel.Two).SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial));
            Assert.AreEqual(ConsistencyLevel.Two, consistency);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, serialConsistency);
            sessionMock.Verify();
        }

        [Test]
        public void Update_Cql_Prepends()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, args) =>
            {
                query = q;
                parameters = args;
            }, new RowSet());
            var mapper = new Mapper(session, new MappingConfiguration());
            mapper.Update<Song>(Cql.New("SET title = ? WHERE id = ?", "White Room"));
            Assert.AreEqual("UPDATE Song SET title = ? WHERE id = ?", query);
        }

        [Test]
        public void UpdateIf_AppliedInfo_True_Test()
        {
            string query = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            object[] parameters = null;
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(TestHelper.DelayedTask(TestDataHelper.CreateMultipleValuesRowSet(new[] { "[applied]" }, new [] { true })))
                .Callback<BoundStatement>(b =>
                {
                    parameters = b.QueryValues;
                    query = b.PreparedStatement.Cql;
                })
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((cql, profile) => TestHelper.DelayedTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            const string partialQuery = "SET title = ?, releasedate = ? WHERE id = ? IF artist = ?";
            var updateGuid = Guid.NewGuid();
            var appliedInfo = mapper.UpdateIf<Song>(Cql.New(partialQuery, "Ramble On", new DateTime(1969, 1, 1), updateGuid, "Led Zeppelin"));
            sessionMock.Verify();

            TestHelper.VerifyUpdateCqlColumns("Song", query, new []{"title", "releasedate"},
                new [] {"id"}, new object[] {"Ramble On", new DateTime(1969, 1, 1), updateGuid, "Led Zeppelin"},
                parameters, "IF artist = ?");

            Assert.True(appliedInfo.Applied);
            Assert.Null(appliedInfo.Existing);
        }

        [Test]
        public void UpdateIf_AppliedInfo_False_Test()
        {
            var id = Guid.NewGuid();
            string query = null;
            object[] parameters = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(TestHelper.DelayedTask(TestDataHelper.CreateMultipleValuesRowSet(new [] { "[applied]", "id", "artist" }, new object[] { false, id, "Jimmy Page" })))
                .Callback<BoundStatement>(b =>
                {
                    parameters = b.QueryValues;
                    query = b.PreparedStatement.Cql;
                })
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((cql, profile) => TestHelper.DelayedTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            const string partialQuery = "SET title = ?, releasedate = ? WHERE id = ? IF artist = ?";
            var appliedInfo = mapper.UpdateIf<Song>(Cql.New(partialQuery, "Kashmir", new DateTime(1975, 1, 1), id, "Led Zeppelin"));
            sessionMock.Verify();
            TestHelper.VerifyUpdateCqlColumns("Song", query, new []{"title", "releasedate"},
                new [] {"id"}, new object[] {"Kashmir", new DateTime(1975, 1, 1), id, "Led Zeppelin"},
                parameters, "IF artist = ?");
            Assert.False(appliedInfo.Applied);
            Assert.NotNull(appliedInfo.Existing);
            Assert.AreEqual("Jimmy Page", appliedInfo.Existing.Artist);
        }

        [Test]
        public void Update_SetTimestamp_Test()
        {
            BoundStatement statement = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Cluster).Returns((ICluster)null);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(() => TestHelper.DelayedTask(RowSet.Empty()))
                .Callback<BoundStatement, string>((stmt, profile) => statement = stmt)
                .Verifiable();
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(() => TestHelper.DelayedTask(RowSet.Empty()))
                .Callback<BoundStatement>(stmt => statement = stmt)
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((cql, profile) => TestHelper.DelayedTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            var song = new Song { Id = Guid.NewGuid(), Title = "t2", ReleaseDate = DateTimeOffset.Now };
            var timestamp = DateTimeOffset.Now.Subtract(TimeSpan.FromDays(1));
            mapper.Update(song);
            Assert.Null(statement.Timestamp);
            mapper.Update(song, CqlQueryOptions.New().SetTimestamp(timestamp));
            Assert.AreEqual(timestamp, statement.Timestamp);
            timestamp = DateTimeOffset.Now.Subtract(TimeSpan.FromHours(10));
            mapper.UpdateIf<Song>(Cql.New("UPDATE tbl1 SET t1 = ? WHERE id = ?",
                new object[] {1, 2},
                CqlQueryOptions.New().SetTimestamp(timestamp)));
            Assert.AreEqual(timestamp, statement.Timestamp);
        }

        [Test]
        public void Update_Poco_With_Enum_Collections()
        {
            object[] parameters = null;
            var config = new MappingConfiguration().Define(PocoWithEnumCollections.DefaultMapping);
            var mapper = GetMappingClient(() => TaskHelper.ToTask(RowSet.Empty()), (_, p) =>
            {
                parameters = p;
            }, config);
            var collectionValues = new[]{ HairColor.Blonde, HairColor.Gray };
            var expectedCollection = collectionValues.Select(x => (int) x).ToArray();
            mapper.Update<PocoWithEnumCollections>("UPDATE tbl1 SET list1 = ? WHERE id = ?",
                mapper.ConvertCqlArgument<IEnumerable<HairColor>, IEnumerable<int>>(collectionValues), 3L);
            Assert.AreEqual(new object[]{ expectedCollection, 3L }, parameters);
            mapper.Update<PocoWithEnumCollections>("UPDATE tbl1 SET list1 = ? WHERE id = ?",
                mapper.ConvertCqlArgument<HairColor[], IEnumerable<int>>(collectionValues), 3L);
            Assert.AreEqual(new object[]{ expectedCollection, 3L }, parameters);
        }
    }
}