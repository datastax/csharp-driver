using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.Tasks;
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
            sessionMock.Setup(s => s.PrepareAsync(It.IsAny<string>())).Returns(TaskHelper.ToTask(GetPrepared("Mock query")));
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
            CollectionAssert.AreEqual(new[] { 1 }, result.ToArray(), new TestHelper.PropertyComparer());
        }

        [Test]
        public void Linq_CqlQueryBase_Execute_Anonymous_Type()
        {
            var table = GetSession(TestDataHelper.CreateMultipleValuesRowSet(new [] {"age", "long_value"}, new [] {25, 1000})).GetTable<AllTypesEntity>();
            var result = (from e in table select new { user_age = e.IntValue, identifier = e.Int64Value }).Execute().ToList();
            Assert.AreEqual(1000L, result[0].identifier);
            Assert.AreEqual(25, result[0].user_age);
        }

        [Test]
        public void Linq_CqlQueryBase_Execute_Anonymous_Single_Value_Type()
        {
            //It does not have much sense to use an anonymous type with a single value but here it goes!
            var table = GetSession(TestDataHelper.CreateMultipleValuesRowSet(new[] { "age" }, new[] { 25 })).GetTable<AllTypesEntity>();
            var result = (from e in table select new { user_age = e.IntValue }).Execute().ToList();
            Assert.AreEqual(25, result[0].user_age);
        }

        [Test]
        public void Linq_CqlQueryBase_Execute_NoDefaultConstructor()
        {
            var table = GetSession(TestDataHelper.CreateMultipleValuesRowSet(new[] { "age", "long_value" }, new[] { 25, 1000 })).GetTable<AllTypesEntity>();
            var result = (from e in table select new Tuple<int, long>(e.IntValue, e.Int64Value)).Execute().ToList();
            Assert.AreEqual(25, result[0].Item1);
            Assert.AreEqual(1000L, result[0].Item2);
        }
    }
}
