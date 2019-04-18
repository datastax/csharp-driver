// 
//       Copyright (C) 2019 DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

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
    public class MapperExecutionProfileTests : MappingTestBase
    {
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteFetchWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var usersExpected = TestDataHelper.GetUserList();
            var rowset = TestDataHelper.GetUsersRowSet(usersExpected);
            var mapperAndSession = GetMappingClientAndSession(rowset);

            var cql = Cql.New("SELECT * FROM users")
                         .WithOptions(opt => opt.SetConsistencyLevel(ConsistencyLevel.Quorum))
                         .WithExecutionProfile("testProfile");
            var users = async ? mapperAndSession.Mapper.FetchAsync<PlainUser>(cql).Result : mapperAndSession.Mapper.Fetch<PlainUser>(cql);

            CollectionAssert.AreEqual(users, usersExpected, new TestHelper.PropertyComparer());
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteFetchPagedWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var usersExpected = TestDataHelper.GetUserList();
            var rowset = TestDataHelper.GetUsersRowSet(usersExpected);
            var mapperAndSession = GetMappingClientAndSession(rowset);

            var cql = Cql.New("SELECT * FROM users")
                         .WithOptions(opt => opt.SetConsistencyLevel(ConsistencyLevel.Quorum))
                         .WithExecutionProfile("testProfile");
            var users = async ? mapperAndSession.Mapper.FetchPageAsync<PlainUser>(cql).Result : mapperAndSession.Mapper.FetchPage<PlainUser>(cql);

            CollectionAssert.AreEqual(users.ToList(), usersExpected, new TestHelper.PropertyComparer());
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteFirstOrDefaultWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var usersExpected = TestDataHelper.GetUserList();
            var rowset = TestDataHelper.GetUsersRowSet(usersExpected);
            var mapperAndSession = GetMappingClientAndSession(rowset);

            var cql = Cql.New("SELECT * FROM users")
                         .WithOptions(opt => opt.SetConsistencyLevel(ConsistencyLevel.Quorum))
                         .WithExecutionProfile("testProfile");
            var user = async ? mapperAndSession.Mapper.FirstOrDefaultAsync<PlainUser>(cql).Result : mapperAndSession.Mapper.FirstOrDefault<PlainUser>(cql);

            CollectionAssert.AreEqual(new List<PlainUser> { usersExpected.First() }, new List<PlainUser> { user }, new TestHelper.PropertyComparer());
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteSingleOrDefaultWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var usersExpected = new List<PlainUser> { TestDataHelper.GetUserList().First() };
            var rowset = TestDataHelper.GetUsersRowSet(usersExpected);
            var mapperAndSession = GetMappingClientAndSession(rowset);

            var cql = Cql.New("SELECT * FROM users")
                         .WithOptions(opt => opt.SetConsistencyLevel(ConsistencyLevel.Quorum))
                         .WithExecutionProfile("testProfile");
            var user = async ? mapperAndSession.Mapper.SingleOrDefaultAsync<PlainUser>(cql).Result : mapperAndSession.Mapper.SingleOrDefault<PlainUser>(cql);

            CollectionAssert.AreEqual(usersExpected, new List<PlainUser> { user }, new TestHelper.PropertyComparer());
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteFirstWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var usersExpected = TestDataHelper.GetUserList();
            var rowset = TestDataHelper.GetUsersRowSet(usersExpected);
            var mapperAndSession = GetMappingClientAndSession(rowset);

            var cql = Cql.New("SELECT * FROM users")
                         .WithOptions(opt => opt.SetConsistencyLevel(ConsistencyLevel.Quorum))
                         .WithExecutionProfile("testProfile");
            var user = async ? mapperAndSession.Mapper.FirstAsync<PlainUser>(cql).Result : mapperAndSession.Mapper.First<PlainUser>(cql);

            CollectionAssert.AreEqual(new List<PlainUser> { usersExpected.First() }, new List<PlainUser> { user }, new TestHelper.PropertyComparer());
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteSingleWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            var usersExpected = new List<PlainUser> { TestDataHelper.GetUserList().First() };
            var rowset = TestDataHelper.GetUsersRowSet(usersExpected);
            var mapperAndSession = GetMappingClientAndSession(rowset);

            var cql = Cql.New("SELECT * FROM users")
                         .WithOptions(opt => opt.SetConsistencyLevel(ConsistencyLevel.Quorum))
                         .WithExecutionProfile("testProfile");
            var user = async ? mapperAndSession.Mapper.SingleAsync<PlainUser>(cql).Result : mapperAndSession.Mapper.Single<PlainUser>(cql);

            CollectionAssert.AreEqual(usersExpected, new List<PlainUser> { user }, new TestHelper.PropertyComparer());
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteDeleteWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
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

            if (async)
            {
                mapper.DeleteAsync(song, "testProfile").Wait();
            }
            else
            {
                mapper.Delete(song, "testProfile");
            }

            Assert.AreEqual("DELETE FROM Song WHERE Id = ?", query);
            CollectionAssert.AreEqual(new object[] { song.Id }, parameters);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteDeleteIfWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            string query = null;
            var session = GetSession((q, args) => query = q, new RowSet());
            var mapper = new Mapper(session, new MappingConfiguration());

            if (async)
            {
                mapper.DeleteIfAsync<Song>(Cql.New("WHERE id = ? IF title = ?", Guid.NewGuid(), "All of My love").WithExecutionProfile("testProfile")).Wait();
            }
            else
            {
                mapper.DeleteIf<Song>(Cql.New("WHERE id = ? IF title = ?", Guid.NewGuid(), "All of My love").WithExecutionProfile("testProfile"));
            }

            Assert.AreEqual("DELETE FROM Song WHERE id = ? IF title = ?", query);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteInsertWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            string query = null;
            object[] parameters = null;
            var mapperAndSession = GetMappingClientAndSession(() => TaskHelper.ToTask(RowSet.Empty()), (q, p) =>
            {
                query = q;
                parameters = p;
            });
            var song = new Song { Id = Guid.NewGuid() };
            const int ttl = 600;
            
            if (async)
            {
                mapperAndSession.Mapper.InsertAsync(song, "testProfile", false, ttl).Wait();
            }
            else
            {
                mapperAndSession.Mapper.Insert(song, "testProfile", false, ttl);
            }

            Assert.AreEqual("INSERT INTO Song (Id, ReleaseDate) VALUES (?, ?) USING TTL ?", query);
            Assert.AreEqual(song.Id, parameters[0]);
            Assert.AreEqual(ttl, parameters.Last());
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ExecuteInsertIfNotExistsWithExecutionProfile_When_ExecutionProfileIsProvided(bool async)
        {
            string query = null;
            object[] parameters = null;
            var mapperAndSession = GetMappingClientAndSession(() => TaskHelper.ToTask(RowSet.Empty()), (q, p) =>
            {
                query = q;
                parameters = p;
            });
            var song = new Song { Id = Guid.NewGuid(), Title = "t2", ReleaseDate = DateTimeOffset.Now };
            const int ttl = 600;
            
            if (async)
            {
                mapperAndSession.Mapper.InsertIfNotExistsAsync(song, "testProfile", false, ttl).Wait();
            }
            else
            {
                mapperAndSession.Mapper.InsertIfNotExists(song, "testProfile", false, ttl);
            }

            Assert.AreEqual("INSERT INTO Song (Id, Title, ReleaseDate) VALUES (?, ?, ?) IF NOT EXISTS USING TTL ?", query);
            Assert.AreEqual(song.Id, parameters[0]);
            Assert.AreEqual(song.Title, parameters[1]);
            Assert.AreEqual(song.ReleaseDate, parameters[2]);
            Assert.AreEqual(ttl, parameters[3]);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>(), "testProfile"), Times.Once);
            Mock.Get(mapperAndSession.Session).Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>(), "testProfile"), Times.Never);
            Mock.Get(mapperAndSession.Session).Verify(s => s.Execute(It.IsAny<IStatement>()), Times.Never);
        }
    }
}