using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.FluentMappings;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    public class InsertTests : MappingTestBase
    {
        [Test]
        public void InsertAsync_Poco()
        {
            // Get a "new" user by using the test data from an existing user and changing the primary key
            var user = TestDataHelper.GetUserList().First();
            var newUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = user.Name,
                Age = user.Age,
                CreatedDate = user.CreatedDate,
                IsActive = user.IsActive,
                LastLoginDate = user.LastLoginDate,
                LoginHistory = user.LoginHistory,
                LuckyNumbers = user.LuckyNumbers,
                ChildrenAges = new Dictionary<string, int>(user.ChildrenAges),
                FavoriteColor = user.FavoriteColor,
                TypeOfUser = user.TypeOfUser,
                PreferredContact = user.PreferredContactMethod,
                HairColor = user.HairColor
            };

            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            //Execute Insert and wait
            mappingClient.InsertAsync(newUser).Wait(3000);
            sessionMock.Verify(s => s.ExecuteAsync(It.Is<BoundStatement>(stmt => 
                stmt.QueryValues.Length == TestHelper.ToDictionary(newUser).Count &&
                stmt.PreparedStatement.Cql.StartsWith("INSERT INTO users (")
                )), Times.Exactly(1));
            sessionMock.Verify();
        }

        [Test]
        public void Insert_Poco()
        {
            //Just a few props as it is just to test that it runs
            var user = TestDataHelper.GetUserList().First();
            var newUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = user.Name
            };

            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            //Execute
            mappingClient.Insert(newUser);
            sessionMock.Verify(s => s.ExecuteAsync(It.Is<BoundStatement>(stmt =>
                stmt.QueryValues.Length == TestHelper.ToDictionary(newUser).Count &&
                stmt.PreparedStatement.Cql.StartsWith("INSERT INTO users (")
                )), Times.Exactly(1));
            sessionMock.Verify();
        }

        [Test]
        public void InsertAsync_FluentPoco()
        {
            // Get a "new" user by using the test data from an existing user and changing the primary key
            var user = TestDataHelper.GetUserList().First();
            var newUser = new FluentUser
            {
                Id = Guid.NewGuid(),
                Name = user.Name,
                Age = user.Age,
                CreatedDate = user.CreatedDate,
                IsActive = user.IsActive,
                LastLoginDate = user.LastLoginDate,
                LoginHistory = user.LoginHistory,
                LuckyNumbers = user.LuckyNumbers,
                ChildrenAges = new Dictionary<string,int>(user.ChildrenAges),
                FavoriteColor = user.FavoriteColor,
                TypeOfUser = user.TypeOfUser,
                PreferredContact = user.PreferredContactMethod,
                HairColor = user.HairColor
            };

            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();

            // Insert the new user
            var mappingClient = GetMappingClient(sessionMock);
            mappingClient.InsertAsync(newUser).Wait(3000);

            sessionMock.Verify(s => s.ExecuteAsync(It.Is<BoundStatement>(stmt =>
                stmt.QueryValues.Length > 0 &&
                stmt.PreparedStatement.Cql.StartsWith("INSERT INTO")
                )), Times.Exactly(1));
            sessionMock.Verify();
        }

        [Test]
        public void Insert_Udt()
        {
            var album = new Album
            {
                Id = Guid.NewGuid(),
                Name = "Images and Words",
                PublishingDate = DateTimeOffset.Now,
                Songs = new List<Song>
                {
                    new Song {Artist = "Dream Theater", Title = "Pull me under"},
                    new Song {Artist = "Dream Theater", Title = "Under a glass moon"}
                }
            };
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            mapper.Insert(album);
            sessionMock.Verify(s => s.ExecuteAsync(It.Is<BoundStatement>(stmt =>
                stmt.QueryValues.Length > 0 &&
                stmt.PreparedStatement.Cql == "INSERT INTO Album (Id, Name, PublishingDate, Songs) VALUES (?, ?, ?, ?)"
                )), Times.Exactly(1));
            sessionMock.Verify();
        }

        [Test]
        public void Insert_Poco_Returns_WhenResponse_IsReceived()
        {
            var newUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = "Dummy"
            };

            var rowsetReturned = false;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(TestHelper.DelayedTask(new RowSet(), 2000).ContinueWith(t =>
                {
                    rowsetReturned = true;
                    return t.Result;
                }))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            //Execute
            mappingClient.Insert(newUser);
            Assert.True(rowsetReturned);
            sessionMock.Verify();
        }
    }
}
