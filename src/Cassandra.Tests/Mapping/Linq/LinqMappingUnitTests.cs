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
        [Test]
        public void Linq_CqlQueryBase_Execute_Empty_RowSet()
        {
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<SimpleStatement>()))
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock.Setup(s => s.BinaryProtocolVersion).Returns(2);
            var table = sessionMock.Object.GetTable<PlainUser>();
            var entities = table.Where(a => a.UserId == Guid.Empty).Execute();
            Assert.AreEqual(0, entities.Count());
        }

        [Test]
        public void Linq_CqlQueryBase_Execute_Maps_Rows()
        {
            var usersExpected = TestDataHelper.GetUserList();
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<SimpleStatement>()))
                .Returns(TaskHelper.ToTask(TestDataHelper.GetUsersRowSet(usersExpected)))
                .Verifiable();
            sessionMock.Setup(s => s.BinaryProtocolVersion).Returns(2);
            var table = sessionMock.Object.GetTable<PlainUser>();
            var users = table.Execute().ToList();
            CollectionAssert.AreEqual(usersExpected, users, new TestHelper.PropertyComparer());
        }

        [Test]
        public void Linq_CqlQueryBase_Execute_SingleColumn_Rows()
        {
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskHelper.ToTask(TestDataHelper.GetSingleValueRowSet("int_val", 1)))
                .Verifiable();
            sessionMock.Setup(s => s.BinaryProtocolVersion).Returns(2);
            var table = sessionMock.Object.GetTable<int>();
            var result = table.Execute().ToList();
            CollectionAssert.AreEqual(new [] {1}, result.ToArray(), new TestHelper.PropertyComparer());
        }
    }
}
