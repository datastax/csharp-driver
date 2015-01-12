using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
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
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Callback<IStatement>(b =>
                {
                    query = ((BoundStatement)b).PreparedStatement.Cql;
                    parameters = b.QueryValues;
                })
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(new PreparedStatement(null, null, cql, null)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock, new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id)));
            mapper.Update(song);
            Assert.AreEqual("UPDATE Song SET Title = ?, Artist = ?, ReleaseDate = ? WHERE Id = ?", query);
            CollectionAssert.AreEqual(new object[] {song.Title, song.Artist, song.ReleaseDate, song.Id}, parameters);
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
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Callback<IStatement>(b =>
                {
                    query = ((BoundStatement)b).PreparedStatement.Cql;
                    parameters = b.QueryValues;
                })
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(new PreparedStatement(null, null, cql, null)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock, new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Title, s => s.Id)));
            mapper.Update(song);
            Assert.AreEqual("UPDATE Song SET Artist = ?, ReleaseDate = ? WHERE Id = ? AND Title = ?", query);
            CollectionAssert.AreEqual(new object[] { song.Artist, song.ReleaseDate, song.Id, song.Title }, parameters);
            
            //Different order in the partition key definitions
            mapper = GetMappingClient(sessionMock, new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id, s => s.Title)));
            mapper.Update(song);
            Assert.AreEqual("UPDATE Song SET Artist = ?, ReleaseDate = ? WHERE Id = ? AND Title = ?", query);
            CollectionAssert.AreEqual(new object[] { song.Artist, song.ReleaseDate, song.Id, song.Title }, parameters);
            sessionMock.Verify();
        }

        [Test]
        public void Update_With_ClusteringKey()
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
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Callback<IStatement>(b =>
                {
                    query = ((BoundStatement)b).PreparedStatement.Cql;
                    parameters = b.QueryValues;
                })
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(new PreparedStatement(null, null, cql, null)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock, new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).ClusteringKey(s => s.ReleaseDate)));
            mapper.Update(song);
            Assert.AreEqual("UPDATE Song SET Title = ?, Artist = ? WHERE Id = ? AND ReleaseDate = ?", query);
            CollectionAssert.AreEqual(new object[] { song.Title, song.Artist, song.Id, song.ReleaseDate }, parameters);
            sessionMock.Verify();
        }
    }
}
