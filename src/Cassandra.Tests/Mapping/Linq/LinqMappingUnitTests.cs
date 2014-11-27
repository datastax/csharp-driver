using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    public class LinqMappingUnitTests : MappingTestBase
    {
        private ISession GetSession(RowSet result)
        {
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskHelper.ToTask(result))
                .Verifiable();
            sessionMock.Setup(s => s.PrepareAsync(It.IsAny<string>())).Returns(TaskHelper.ToTask(new PreparedStatement(null, null, "Mock query", null)));
            sessionMock.Setup(s => s.BinaryProtocolVersion).Returns(2);
            return sessionMock.Object;
        }

        [Test]
        public void Linq_CqlQueryBase_Execute_Empty_RowSet()
        {
            var table = GetSession(new RowSet()).GetTable<PlainUser>();
            var entities = table.Where(a => a.UserId == Guid.Empty).Execute();
            Assert.AreEqual(0, entities.Count());
        }

        [Test]
        public void Linq_CqlQueryBase_Execute_Maps_Rows()
        {
            var usersExpected = TestDataHelper.GetUserList();
            var table = GetSession(TestDataHelper.GetUsersRowSet(usersExpected)).GetTable<PlainUser>();
            var users = table.Execute().ToList();
            CollectionAssert.AreEqual(usersExpected, users, new TestHelper.PropertyComparer());
        }

        [Test]
        public void Linq_CqlQueryBase_Execute_SingleColumn_Rows()
        {
            var table = GetSession(TestDataHelper.GetSingleValueRowSet("int_val", 1)).GetTable<int>();
            var result = table.Execute().ToList();
            CollectionAssert.AreEqual(new [] {1}, result.ToArray(), new TestHelper.PropertyComparer());
        }
    }
}
