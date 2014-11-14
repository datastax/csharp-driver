using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Mapping.FluentMapping;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    [TestFixture]
    public class FetchTests
    {
        private ICqlClient GetMappingClient(RowSet rowset = null, Mock<ISession> sessionMock = null)
        {
            if (sessionMock == null)
            {
                sessionMock = new Mock<ISession>(MockBehavior.Strict);
                sessionMock
                    .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                    .Returns(TaskHelper.ToTask(rowset))
                    .Verifiable();
                sessionMock
                    .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                    .Returns(TaskHelper.ToTask(new PreparedStatement(null, null, null, null)))
                    .Verifiable();
            }
            var mappingClient = CqlClientConfiguration
                .ForSession(sessionMock.Object)
                .BuildCqlClient();
            return mappingClient;
        }

        [Test]
        public void FetchAsync_Pocos_WithCql_Empty()
        {
            var rowset = new RowSet();
            var mappingClient = GetMappingClient(rowset);
            var userTask = mappingClient.FetchAsync<PlainUser>("SELECT * FROM users");
            var users = userTask.Result;
            Assert.NotNull(users);
            Assert.AreEqual(0, users.Count);
        }

        [Test]
        public void FetchAsync_Pocos_WithCql_Single_Column_Maps()
        {
            //just the userid
            var usersExpected = TestDataHelper.GetUserList().Select(u => new PlainUser { UserId = u.UserId} ).ToList();
            var rowset = TestDataHelper.GetUsersRowSet(usersExpected);
            var mappingClient = GetMappingClient(rowset);
            var userTask = mappingClient.FetchAsync<PlainUser>("SELECT * FROM users");
            var users = userTask.Result;
            Assert.NotNull(users);
            CollectionAssert.AreEqual(usersExpected, users, new TestHelper.PropertyComparer());
        }

        [Test]
        public void FetchAsync_Pocos_Prepares_Just_Once()
        {
            const int times = 100;
            var users = TestDataHelper.GetUserList();
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(() => TaskHelper.ToTask(TestDataHelper.GetUsersRowSet(users)))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(new PreparedStatement(null, null, null, null)))
                .Verifiable();
            var mappingClient = GetMappingClient(null, sessionMock);
            var taskList = new List<Task<List<PlainUser>>>();
            for (var i = 0; i < times; i++)
            {
                var t = mappingClient.FetchAsync<PlainUser>("SELECT * FROM users");
                taskList.Add(t);
            }

            Task.WaitAll(taskList.Select(t => (Task)t).ToArray(), 5000);
            Assert.True(taskList.All(t => t.Result.Count == 10));
            sessionMock.Verify();
            //Prepare should be called just once
            sessionMock
                .Verify(s => s.PrepareAsync(It.IsAny<string>()), Times.Once());
            //ExecuteAsync should be called the exact number of times
            sessionMock
                .Verify(s => s.ExecuteAsync(It.IsAny<BoundStatement>()), Times.Exactly(times));
        }
    }
}
