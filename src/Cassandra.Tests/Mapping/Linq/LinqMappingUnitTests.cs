using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Cassandra.Data.Linq;
using Cassandra.Mapping;
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
            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.Configuration).Returns(new Configuration());

            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TestHelper.DelayedTask(result, 200))
                .Verifiable();
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<IStatement>(), It.IsAny<string>()))
                .Returns(TestHelper.DelayedTask(result, 200))
                .Verifiable();
            sessionMock.Setup(s => s.PrepareAsync(It.IsAny<string>())).Returns(TaskHelper.ToTask(GetPrepared("Mock query")));
            sessionMock.Setup(s => s.PrepareAsync(It.IsAny<IPrepareRequest>())).Returns(TaskHelper.ToTask(GetPrepared("Mock query")));
            sessionMock.Setup(s => s.BinaryProtocolVersion).Returns(2);
            sessionMock.Setup(s => s.Cluster).Returns(clusterMock.Object);
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
            var table = new Table<PlainUser>(GetSession(TestDataHelper.GetUsersRowSet(usersExpected)));
            var users = table.Execute().ToList();
            CollectionAssert.AreEqual(usersExpected, users, new TestHelper.PropertyComparer());
        }

        [Test]
        public void Linq_CqlQueryBase_Execute_SingleColumn_Rows()
        {
            var table = new Table<int>(GetSession(TestDataHelper.GetSingleValueRowSet("int_val", 1)));
            var result = table.Execute().ToList();
            CollectionAssert.AreEqual(new[] { 1 }, result.ToArray(), new TestHelper.PropertyComparer());
        }

        [Test]
        public void Linq_CqlQueryBase_Execute_Anonymous_Type()
        {
            var table = GetSession(TestDataHelper.CreateMultipleValuesRowSet(new[] { "IntValue", "Int64Value" }, new[] { 25, 1000 })).GetTable<AllTypesEntity>();
            var result = (from e in table select new { user_age = e.IntValue, identifier = e.Int64Value }).Execute().ToList();
            Assert.AreEqual(1000L, result[0].identifier);
            Assert.AreEqual(25, result[0].user_age);
        }

        [Test]
        public void Linq_CqlQueryBase_Execute_Anonymous_Single_Value_Type()
        {
            //It does not have much sense to use an anonymous type with a single value but here it goes!
            var table = GetSession(TestDataHelper.CreateMultipleValuesRowSet(new[] { "intvalue" }, new[] { 25 })).GetTable<AllTypesEntity>();
            var result = (from e in table select new { user_age = e.IntValue }).Execute().ToList();
            Assert.AreEqual(25, result[0].user_age);
        }

        [Test]
        public void Linq_CqlQueryBase_Execute_NoDefaultConstructor()
        {
            var table = GetSession(TestDataHelper.CreateMultipleValuesRowSet(new[] { "intvalue", "int64value" }, new[] { 25, 1000 })).GetTable<AllTypesEntity>();
            var result = (from e in table select new Tuple<int, long>(e.IntValue, e.Int64Value)).Execute().ToList();
            Assert.AreEqual(25, result[0].Item1);
            Assert.AreEqual(1000L, result[0].Item2);
        }

        [Test]
        public void Linq_CqlQuery_ExecutePaged_Maps_Rows()
        {
            var usersExpected = TestDataHelper.GetUserList();
            var rs = TestDataHelper.GetUsersRowSet(usersExpected);
            rs.AutoPage = false;
            rs.PagingState = new byte[] { 1, 2, 3 };
            var table = new Table<PlainUser>(GetSession(rs));
            IPage<PlainUser> users = table.ExecutePaged();
            //It was executed without paging state
            Assert.Null(users.CurrentPagingState);
            Assert.NotNull(users.PagingState);
            CollectionAssert.AreEqual(rs.PagingState, users.PagingState);
            CollectionAssert.AreEqual(usersExpected, users, new TestHelper.PropertyComparer());
        }

        [Test]
        public void Linq_CqlQuery_ExecutePaged_Maps_SingleValues()
        {
            var rs = TestDataHelper.GetSingleColumnRowSet("int_val", new[] { 100, 200, 300 });
            rs.AutoPage = false;
            rs.PagingState = new byte[] { 2, 2, 2 };
            var table = new Table<int>(GetSession(rs));
            IPage<int> page = table.SetPagingState(new byte[] { 1, 1, 1 }).ExecutePaged();
            CollectionAssert.AreEqual(table.PagingState, page.CurrentPagingState);
            CollectionAssert.AreEqual(rs.PagingState, page.PagingState);
            CollectionAssert.AreEqual(new[] { 100, 200, 300 }, page.ToArray(), new TestHelper.PropertyComparer());
        }

        [Test]
        public void Linq_CqlQuery_Automatically_Pages()
        {
            const int pageLength = 100;
            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.Configuration).Returns(new Configuration());
            var rs = TestDataHelper.GetSingleColumnRowSet("int_val", Enumerable.Repeat(1, pageLength).ToArray());
            BoundStatement stmt = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Cluster).Returns(clusterMock.Object);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(TestHelper.DelayedTask(rs))
                .Callback<IStatement>(s => stmt = (BoundStatement)s)
                .Verifiable();
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(TestHelper.DelayedTask(rs))
                .Callback<IStatement, string>((s, profile) => stmt = (BoundStatement)s)
                .Verifiable();
            sessionMock.Setup(s => s.PrepareAsync(It.IsAny<string>())).Returns(TaskHelper.ToTask(GetPrepared("Mock query")));
            sessionMock.Setup(s => s.PrepareAsync(It.IsAny<IPrepareRequest>())).Returns(TaskHelper.ToTask(GetPrepared("Mock query")));
            sessionMock.Setup(s => s.BinaryProtocolVersion).Returns(2);
            rs.AutoPage = true;
            rs.PagingState = new byte[] { 0, 0, 0 };
            var counter = 0;
            rs.SetFetchNextPageHandler(state =>
            {
                var rs2 = TestDataHelper.GetSingleColumnRowSet("int_val", Enumerable.Repeat(1, pageLength).ToArray());
                if (++counter < 2)
                {
                    rs2.PagingState = new byte[] { 0, 0, (byte)counter };
                }
                return Task.FromResult(rs2);
            }, int.MaxValue);
            var table = new Table<int>(sessionMock.Object);
            IEnumerable<int> results = table.Execute();
            Assert.True(stmt.AutoPage);
            Assert.AreEqual(0, counter);
            Assert.AreEqual(pageLength * 3, results.Count());
            Assert.AreEqual(2, counter);
        }
    }
}