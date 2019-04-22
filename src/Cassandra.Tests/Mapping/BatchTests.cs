using System;
using System.Linq;
using System.Threading.Tasks;

using Cassandra.Mapping;
using Cassandra.Tasks;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;

using Moq;

using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Cassandra.Tests.Mapping
{
    /// <summary>
    /// Tests that the Batch execution calls to prepare async and executeasync.
    /// In the case of conditional batches, it tests that also the RowSet is adapted
    /// </summary>
    [TestFixture]
    public class BatchTests : MappingTestBase
    {
        /// <summary>
        /// Gets the mapper for batch statements
        /// </summary>
        private IMapper GetMapper(Func<Task<RowSet>> getRowSetFunc, Action<BatchStatement> statementCallback = null)
        {
            if (statementCallback == null)
            {
                statementCallback = _ => { };
            }
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BatchStatement>()))
                .Returns(getRowSetFunc)
                .Callback(statementCallback)
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            return GetMappingClient(sessionMock);
        }

        private MapperAndSessionTuple GetMapperAndSession(Func<Task<RowSet>> getRowSetFunc, Action<BatchStatement> statementCallback = null)
        {
            if (statementCallback == null)
            {
                statementCallback = _ => { };
            }
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BatchStatement>()))
                .Returns(getRowSetFunc)
                .Callback(statementCallback)
                .Verifiable();
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<IStatement>(), It.IsAny<string>()))
                .Returns(getRowSetFunc)
                .Callback<IStatement, string>((stmt, ep) => statementCallback((BatchStatement)stmt))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            return new MapperAndSessionTuple
            {
                Mapper = GetMappingClient(sessionMock),
                Session = sessionMock.Object
            };
        }

        [Test]
        public void Execute_Batch_Test()
        {
            // Generate 3 test users
            var testUsers = Enumerable.Range(100, 3).Select(idx => new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx % 2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            }).ToList();

            BatchStatement statement = null;
            // Create batch to insert users and execute
            var mapper = GetMapper(() => TestHelper.DelayedTask(new RowSet()), s => statement = s);
            var batch = mapper.CreateBatch(BatchType.Unlogged);
            batch.Insert(testUsers[0]);
            batch.Insert(testUsers[1]);
            batch.Insert(testUsers[2]);
            mapper.Execute(batch);
            Assert.NotNull(statement);
            Assert.AreEqual(BatchType.Unlogged, statement.BatchType);
        }

        [Test]
        public void Execute_MixedBatch_Test()
        {
            // Generate test user
            const int idx = 20;
            var testUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx % 2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            };

            // Get id of existing user for deleting and updating
            Guid deleteId = Guid.NewGuid();
            Guid updateId = Guid.NewGuid();

            // Create batch of mixed statements and execute
            var mapper = GetMapper(() => TestHelper.DelayedTask(new RowSet()));
            ICqlBatch batch = mapper.CreateBatch();
            batch.Insert(testUser);
            batch.Delete<InsertUser>("WHERE userid = ?", deleteId);
            batch.Update<InsertUser>("SET name = ? WHERE userid = ?", "SomeNewName", updateId);
            var queries = batch.Statements.Select(cql => cql.Statement).ToArray();
            Assert.AreEqual(3, queries.Length);
            Assert.AreEqual("INSERT INTO users (userid, Name, Age, CreatedDate, IsActive, " +
                            "LastLoginDate, LoginHistory, LuckyNumbers, ChildrenAges, " +
                            "FavoriteColor, TypeOfUser, preferredcontactmethod, HairColor) VALUES " +
                            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", queries[0]);
            Assert.AreEqual("DELETE FROM users WHERE userid = ?", queries[1]);
            Assert.AreEqual("UPDATE users SET name = ? WHERE userid = ?", queries[2]);
            mapper.Execute(batch);
        }

        [Test]
        public void Execute_Without_Nulls()
        {
            BatchStatement statement = null;
            var mapper = GetMapper(() => TestHelper.DelayedTask(new RowSet()), s => statement = s);
            var batch = mapper.CreateBatch();
            //It should not include null values
            batch.Insert(new Song { Id = Guid.NewGuid(), ReleaseDate = DateTimeOffset.Now }, false);
            //It should include null columns
            batch.Insert(new Song { Id = Guid.NewGuid() }, true);
            mapper.Execute(batch);
            Assert.NotNull(statement);
            Assert.AreEqual(BatchType.Logged, statement.BatchType);
            var queries = batch.Statements.Select(cql => cql.Statement).ToArray();
            var parameters = batch.Statements.Select(cql => cql.Arguments).ToArray();
            Assert.AreEqual("INSERT INTO Song (Id, ReleaseDate) VALUES (?, ?)", queries[0]);
            Assert.AreEqual(2, parameters[0].Length);
            Assert.AreEqual("INSERT INTO Song (Id, Title, Artist, ReleaseDate) VALUES (?, ?, ?, ?)", queries[1]);
            Assert.AreEqual(4, parameters[1].Length);
        }

        [Test]
        public void Execute_With_Options()
        {
            BatchStatement statement = null;
            var mapper = GetMapper(() => TestHelper.DelayedTask(new RowSet()), s => statement = s);
            var batch = mapper.CreateBatch();
            batch.Insert(new Song { Id = Guid.NewGuid() });
            batch.Insert(new Song { Id = Guid.NewGuid() });
            var consistency = ConsistencyLevel.EachQuorum;
            var timestamp = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromDays(1));
            mapper.Execute(batch
                .WithOptions(o => o.SetConsistencyLevel(consistency))
                .WithOptions(o => o.SetTimestamp(timestamp)));
            Assert.NotNull(statement);
            Assert.AreEqual(BatchType.Logged, statement.BatchType);
            Assert.AreEqual(consistency, statement.ConsistencyLevel);
            Assert.AreEqual(timestamp, statement.Timestamp);
            Assert.AreEqual(2, batch.Statements.Count());
        }

        [Test]
        public void Execute_With_Ttl()
        {
            BatchStatement statement = null;
            var mapper = GetMapper(() => TestHelper.DelayedTask(new RowSet()), s => statement = s);
            var batch = mapper.CreateBatch();
            //It should not include null values
            batch.Insert(new Song { Id = Guid.NewGuid(), ReleaseDate = DateTimeOffset.Now }, false);
            const int ttl = 3600;
            batch.Insert(new Song { Id = Guid.NewGuid() }, true, ttl);
            mapper.Execute(batch);
            Assert.NotNull(statement);
            Assert.AreEqual(BatchType.Logged, statement.BatchType);
            var queries = batch.Statements.Select(cql => cql.Statement).ToArray();
            var parameters = batch.Statements.Select(cql => cql.Arguments).ToArray();
            Assert.AreEqual("INSERT INTO Song (Id, ReleaseDate) VALUES (?, ?)", queries[0]);
            Assert.AreEqual(2, parameters[0].Length);
            Assert.AreEqual("INSERT INTO Song (Id, Title, Artist, ReleaseDate) VALUES (?, ?, ?, ?) USING TTL ?", queries[1]);
            Assert.AreEqual(5, parameters[1].Length);
            Assert.AreEqual(ttl, parameters[1].Last());
        }

        [Test]
        public void ExecuteConditional_Batch_Empty_RowSet_Test()
        {
            var mapper = GetMapper(() => TestHelper.DelayedTask(new RowSet()));
            var batch = mapper.CreateBatch();
            batch.Update<PlainUser>(Cql.New("SET val1 = ? WHERE id = ? IF val2 = ?", "value1", "id1", "value2"));
            batch.Update<PlainUser>(Cql.New("SET val3 = ? WHERE id = ?", "value3", "id1"));
            var info = mapper.ExecuteConditional<PlainUser>(batch);
            Assert.True(info.Applied);
        }

        [Test]
        public void ExecuteConditional_Batch_Applied_True_Test()
        {
            var rs = TestDataHelper.CreateMultipleValuesRowSet(new[] { "[applied]" }, new[] { true });
            var mapper = GetMapper(() => TestHelper.DelayedTask(rs));
            var batch = mapper.CreateBatch();
            batch.InsertIfNotExists(new PlainUser());
            batch.InsertIfNotExists(new PlainUser());
            var info = mapper.ExecuteConditional<PlainUser>(batch);
            Assert.True(info.Applied);
            Assert.Null(info.Existing);
        }

        [Test]
        public void ExecuteConditional_Batch_Applied_False_Test()
        {
            var id = Guid.NewGuid();
            var rs = TestDataHelper.CreateMultipleValuesRowSet(new[] { "[applied]", "userid", "lastlogindate" }, new object[] { false, id, DateTimeOffset.Parse("2015-03-01 +0") });
            var mapper = GetMapper(() => TestHelper.DelayedTask(rs));
            var batch = mapper.CreateBatch();
            batch.Delete<PlainUser>(Cql.New("WHERE userid = ? IF laslogindate = ?", id, DateTimeOffset.Now));
            batch.Insert(new PlainUser());
            var info = mapper.ExecuteConditional<PlainUser>(batch);
            Assert.False(info.Applied);
            Assert.NotNull(info.Existing);
            Assert.AreEqual(id, info.Existing.UserId);
            Assert.AreEqual(DateTimeOffset.Parse("2015-03-01 +0"), info.Existing.LastLoginDate);
        }

        [Test]
        public void ExecuteAsync_Batch_Test()
        {
            // Generate 3 test users
            var testUsers = Enumerable.Range(110, 3).Select(idx => new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx % 2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            }).ToList();

            // Create batch to insert users and execute
            var mapperAndSession = GetMapperAndSession(() => TestHelper.DelayedTask(new RowSet()));
            ICqlBatch batch = mapperAndSession.Mapper.CreateBatch();
            batch.Insert(testUsers[0]);
            batch.Insert(testUsers[1]);
            batch.Insert(testUsers[2]);
            mapperAndSession.Mapper.ExecuteAsync(batch);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), It.IsAny<string>()), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Once);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>(), It.IsAny<string>()), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }

        [Test]
        public void ExecuteAsync_MixedBatch_Test()
        {
            // Generate test user
            const int idx = 21;
            var testUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx % 2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            };

            // Get id of existing user for deleting and updating
            Guid deleteId = Guid.NewGuid();
            Guid updateId = Guid.NewGuid();

            // Create batch of mixed statements and execute
            var mapper = GetMapper(() => TestHelper.DelayedTask(new RowSet()));
            ICqlBatch batch = mapper.CreateBatch();
            batch.Insert(testUser);
            batch.Delete<InsertUser>("WHERE userid = ?", deleteId);
            batch.Update<InsertUser>("SET name = ? WHERE userid = ?", "SomeNewName", updateId);
            mapper.ExecuteAsync(batch).Wait();
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void ExecuteAsync_WithExecutionProfile_Batch_Test(bool async)
        {
            // Generate 3 test users
            var testUsers = Enumerable.Range(110, 3).Select(idx => new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx % 2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            }).ToList();

            // Create batch to insert users and execute
            var mapperAndSession = GetMapperAndSession(() => TestHelper.DelayedTask(new RowSet()));
            ICqlBatch batch = mapperAndSession.Mapper.CreateBatch();
            batch.Insert(testUsers[0]);
            batch.Insert(testUsers[1]);
            batch.Insert(testUsers[2]);
            if (async)
            {
                mapperAndSession.Mapper.ExecuteAsync(batch, "testProfile").Wait();
            }
            else
            {
                mapperAndSession.Mapper.Execute(batch, "testProfile");
            }
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void ExecuteConditionalAsync_WithExecutionProfile_Batch_Test(bool async)
        {
            // Generate 3 test users
            var testUsers = Enumerable.Range(110, 3).Select(idx => new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx % 2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            }).ToList();

            // Create batch to insert users and execute
            var mapperAndSession = GetMapperAndSession(() => TestHelper.DelayedTask(new RowSet()));
            ICqlBatch batch = mapperAndSession.Mapper.CreateBatch();
            batch.Insert(testUsers[0]);
            batch.Insert(testUsers[1]);
            batch.Insert(testUsers[2]);
            if (async)
            {
                mapperAndSession.Mapper.ExecuteConditionalAsync<InsertUser>(batch, "testProfile").Wait();
            }
            else
            {
                mapperAndSession.Mapper.ExecuteConditional<InsertUser>(batch, "testProfile");
            }
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
    }
}