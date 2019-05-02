using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Mapping;
using Cassandra.Tasks;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    [TestFixture]
    public class DeleteTests : MappingTestBase
    {
        [Test]
        public void Delete_Cql_Prepends()
        {
            string query = null;
            var session = GetSession((q, args) => query = q, new RowSet());
            var mapper = new Mapper(session, new MappingConfiguration());
            mapper.Delete<Song>(Cql.New("WHERE id = ?", Guid.NewGuid()));
            Assert.AreEqual("DELETE FROM Song WHERE id = ?", query);
        }

        [Test]
        public void Delete_Poco_Generates_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, args) => 
            { 
                query = q;
                parameters = args;
            }, new RowSet());
            var mapper = new Mapper(session, new MappingConfiguration().Define(new Map<Song>().PartitionKey(s => s.Id)));
            var song = new Song {Id = Guid.NewGuid()};
            mapper.Delete(song);
            Assert.AreEqual("DELETE FROM Song WHERE Id = ?", query);
            CollectionAssert.AreEqual(new object[] { song.Id }, parameters);
        }

        [Test]
        public void DeleteIf_Cql_Prepends_Test()
        {
            string query = null;
            var session = GetSession((q, args) => query = q, new RowSet());
            var mapper = new Mapper(session, new MappingConfiguration());
            mapper.DeleteIf<Song>(Cql.New("WHERE id = ? IF title = ?", Guid.NewGuid(), "All of My love"));
            Assert.AreEqual("DELETE FROM Song WHERE id = ? IF title = ?", query);
        }

        [Test]
        public void DeleteIf_Cql_Applied_True_Test()
        {
            var session = GetSession((q, args) => { }, TestDataHelper.CreateMultipleValuesRowSet(new[] { "[applied]" }, new[] { true }));
            var mapper = new Mapper(session, new MappingConfiguration());
            var appliedInfo = mapper.DeleteIf<Song>(Cql.New("WHERE id = ? IF title = ?", Guid.NewGuid(), "All of My love"));
            Assert.True(appliedInfo.Applied);
            Assert.Null(appliedInfo.Existing);
        }

        [Test]
        public void DeleteIf_Cql_Applied_False_Test()
        {
            var session = GetSession((q, args) => { }, TestDataHelper.CreateMultipleValuesRowSet(new[] { "[applied]", "title" }, new object[] { false, "I Feel Free" }));
            var mapper = new Mapper(session, new MappingConfiguration());
            var appliedInfo = mapper.DeleteIf<Song>(Cql.New("WHERE id = ? IF title = ?", Guid.NewGuid(), "All of My love"));
            Assert.False(appliedInfo.Applied);
            Assert.NotNull(appliedInfo.Existing);
            Assert.AreEqual("I Feel Free", appliedInfo.Existing.Title);
        }

        [Test]
        public void Insert_SetTimestamp_Test()
        {
            BoundStatement statement = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock.Setup(s => s.Cluster).Returns((ICluster)null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(() => TestHelper.DelayedTask(RowSet.Empty()))
                .Callback<BoundStatement>(stmt => statement = stmt)
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(query => TaskHelper.ToTask(GetPrepared(query)))
                .Verifiable();
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(() => TestHelper.DelayedTask(RowSet.Empty()))
                .Callback<BoundStatement, string>((stmt, profile) => statement = stmt)
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>(), It.IsAny<string>()))
                .Returns<string, string>((query, profile) => TaskHelper.ToTask(GetPrepared(query)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            var song = new Song { Id = Guid.NewGuid(), Title = "t2", ReleaseDate = DateTimeOffset.Now };
            var timestamp = DateTimeOffset.Now.Subtract(TimeSpan.FromDays(1));
            mapper.Delete(song);
            Assert.Null(statement.Timestamp);
            mapper.Delete(song, CqlQueryOptions.New().SetTimestamp(timestamp));
            Assert.AreEqual(timestamp, statement.Timestamp);
        }
    }
}