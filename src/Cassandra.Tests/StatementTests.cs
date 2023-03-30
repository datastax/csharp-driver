//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tests.ExecutionProfiles;
using Moq;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.Tests
{
    [TestFixture]
    public class StatementTests
    {
        private const string Query = "SELECT * ...";

        private static PreparedStatement GetPrepared(string query = Query, RowSetMetadata metadata = null, RowSetMetadata resultRowsMetadata = null)
        {
            return new PreparedStatement(metadata, new byte[0], new ResultMetadata(null, resultRowsMetadata), query, null,
                new SerializerManager(ProtocolVersion.MaxSupported));
        }

        [Test]
        public void SimpleStatement_Constructor_Positional_Values()
        {
            var stmt = new SimpleStatement(Query, 1, "value 2", 3L);
            CollectionAssert.AreEqual(new object[] { 1, "value 2", 3L }, stmt.QueryValues);
        }

        [Test]
        public void SimpleStatement_Bind_Positional_Values()
        {
            var stmt = new SimpleStatement(Query).Bind(1, "value 2", 10030L);
            CollectionAssert.AreEqual(new object[] { 1, "value 2", 10030L }, stmt.QueryValues);
        }

        [Test]
        public void SimpleStatement_Constructor_No_Values()
        {
            var stmt = new SimpleStatement(Query);
            Assert.AreEqual(null, stmt.QueryValues);
        }

        [Test]
        public void SimpleStatement_Bind_No_Values()
        {
            var stmt = new SimpleStatement(Query).Bind();
            Assert.AreEqual(new object[0], stmt.QueryValues);
        }

        [Test]
        public void SimpleStatement_Constructor_Named_Values()
        {
            var values = new { Name = "Futurama", Description = "In Stereo where available", Time = DateTimeOffset.Parse("1963-08-28") };
            var stmt = new SimpleStatement(Query, values);
            var actualValues = new Dictionary<string, object>();
            Assert.AreEqual(3, stmt.QueryValueNames.Count);
            Assert.AreEqual(3, stmt.QueryValues.Length);
            //Order is not guaranteed
            for (var i = 0; i < stmt.QueryValueNames.Count; i++)
            {
                actualValues[stmt.QueryValueNames[i]] = stmt.QueryValues[i];
            }
            //Lowercased
            Assert.AreEqual(values.Name, actualValues["name"]);
            Assert.AreEqual(values.Description, actualValues["description"]);
            Assert.AreEqual(values.Time, actualValues["time"]);
        }

        [Test]
        public void SimpleStatement_Constructor_Dictionary_Named_Test()
        {
            var valuesDictionary = new Dictionary<string, object>
            {
                {"Name", "Futurama"}, 
                {"Description", "In Stereo where available"}, 
                {"Time", DateTimeOffset.Parse("1963-08-28")}
            };
            var stmt = new SimpleStatement(valuesDictionary, Query);
            var actualValues = new Dictionary<string, object>();
            Assert.AreEqual(3, stmt.QueryValueNames.Count);
            Assert.AreEqual(3, stmt.QueryValues.Length);
            //Order is not guaranteed
            for (var i = 0; i < stmt.QueryValueNames.Count; i++)
            {
                actualValues[stmt.QueryValueNames[i]] = stmt.QueryValues[i];
            }
            //Lowercased
            Assert.AreEqual(valuesDictionary["Name"], actualValues["name"]);
            Assert.AreEqual(valuesDictionary["Description"], actualValues["description"]);
            Assert.AreEqual(valuesDictionary["Time"], actualValues["time"]);
        }

        [Test]
        public void SimpleStatement_Bind_Named_Values()
        {
            var values = new { Name = "Futurama", Description = "In Stereo where available", Time = DateTimeOffset.Parse("1963-08-28") };
            var stmt = new SimpleStatement(Query).Bind(values);
            var actualValues = new Dictionary<string, object>();
            Assert.AreEqual(3, stmt.QueryValueNames.Count);
            Assert.AreEqual(3, stmt.QueryValues.Length);
            //Order is not guaranteed
            for (var i = 0; i < stmt.QueryValueNames.Count; i++)
            {
                actualValues[stmt.QueryValueNames[i]] = stmt.QueryValues[i];
            }
            //Lowercased
            Assert.AreEqual(values.Name, actualValues["name"]);
            Assert.AreEqual(values.Description, actualValues["description"]);
            Assert.AreEqual(values.Time, actualValues["time"]);
        }

        [Test]
        public void Statement_SetPagingState_Disables_AutoPage()
        {
            var statement = new SimpleStatement();
            Assert.True(statement.AutoPage);
            statement.SetPagingState(new byte[] { 1, 2, 3, 4, 5, 6 });
            Assert.False(statement.AutoPage);
            Assert.NotNull(statement.PagingState);
        }

        [Test]
        public void Statement_SetPagingState_Null_Does_Not_Disable_AutoPage()
        {
            var statement = new SimpleStatement();
            Assert.True(statement.AutoPage);
            statement.SetPagingState(null);
            Assert.True(statement.AutoPage);
            Assert.Null(statement.PagingState);
        }

        [Test]
        public void PreparedStatement_Bind_SetsRoutingKey_Single()
        {
            var metadata = new RowSetMetadata(null)
            {
                Columns = new[]
                {
                    new CqlColumn { Name = "name" }, 
                    new CqlColumn { Name = "id" }
                }
            };
            var ps = GetPrepared("SELECT * FROM tbl1 WHERE name = ? and id = ?", metadata);
            ps.SetPartitionKeys(new[] { new TableColumn() { Name = "id" } });
            //The routing key is at position 1
            CollectionAssert.AreEqual(new[] { 1 }, ps.RoutingIndexes);
            Assert.Null(ps.RoutingKey);
            var bound = ps.Bind("dummy name", 1000);
            Assert.NotNull(bound.RoutingKey);
            CollectionAssert.AreEqual(new SerializerManager(ProtocolVersion.MaxSupported).GetCurrentSerializer().Serialize(1000), bound.RoutingKey.RawRoutingKey);
        }

        [Test]
        public void PreparedStatement_Bind_SetsIdempotence()
        {
            var ps = GetPrepared("SELECT * FROM tbl1 WHERE name = ? and id = ?");
            ps.SetIdempotence(true);
            var bound = ps.Bind("dummy name 1", 1000);
            Assert.True(bound.IsIdempotent ?? false);
            ps.SetIdempotence(false);
            bound = ps.Bind("dummy name 2", 1000);
            Assert.False(bound.IsIdempotent ?? true);
        }

        [Test]
        public void PreparedStatement_Bind_SetsRoutingKey_Multiple()
        {
            var metadata = new RowSetMetadata(null)
            {
                Columns = new[]
                {
                    new CqlColumn { Name = "id2" },
                    new CqlColumn { Name = "id1" }
                }
            };
            var ps = GetPrepared("SELECT * FROM tbl1 WHERE id2 = ? and id1", metadata);
            ps.SetPartitionKeys(new[] { new TableColumn() { Name = "id1" }, new TableColumn() { Name = "id2" } });
            //The routing key is formed by the parameters at position 1 and 0
            CollectionAssert.AreEqual(new[] { 1, 0 }, ps.RoutingIndexes);
            Assert.Null(ps.RoutingKey);
            var bound = ps.Bind(2001, 1001);
            Assert.NotNull(bound.RoutingKey);
            var serializer = new SerializerManager(ProtocolVersion.MaxSupported).GetCurrentSerializer();
            var expectedRoutingKey = new byte[0]
                .Concat(new byte[] {0, 4})
                .Concat(serializer.Serialize(1001))
                .Concat(new byte[] {0})
                .Concat(new byte[] {0, 4})
                .Concat(serializer.Serialize(2001))
                .Concat(new byte[] {0});
            CollectionAssert.AreEqual(expectedRoutingKey, bound.RoutingKey.RawRoutingKey);
        }

        [Test]
        public void SimpleStatement_Bind_SetsRoutingValues_Single()
        {
            var stmt = new SimpleStatement(Query, "id1");
            Assert.Null(stmt.RoutingKey);
            stmt.SetRoutingValues("id1");
            stmt.Serializer = new SerializerManager(ProtocolVersion.MaxSupported).GetCurrentSerializer();
            CollectionAssert.AreEqual(stmt.Serializer.Serialize("id1"), stmt.RoutingKey.RawRoutingKey);
        }

        [Test]
        public void SimpleStatement_Bind_SetsRoutingValues_Multiple()
        {
            var stmt = new SimpleStatement(Query, "id1", "id2", "val1");
            Assert.Null(stmt.RoutingKey);
            stmt.SetRoutingValues("id1", "id2");
            stmt.Serializer = new SerializerManager(ProtocolVersion.MaxSupported).GetCurrentSerializer();
            var expectedRoutingKey = new byte[0]
                .Concat(new byte[] { 0, 3 })
                .Concat(stmt.Serializer.Serialize("id1"))
                .Concat(new byte[] { 0 })
                .Concat(new byte[] { 0, 3 })
                .Concat(stmt.Serializer.Serialize("id2"))
                .Concat(new byte[] { 0 });
            CollectionAssert.AreEqual(expectedRoutingKey, stmt.RoutingKey.RawRoutingKey);
        }

        [Test]
        public void BatchStatement_Bind_SetsRoutingValues_Single()
        {
            var batch = new BatchStatement();
            Assert.Null(batch.RoutingKey);
            batch.SetRoutingValues("id1-value");
            batch.Serializer = new SerializerManager(ProtocolVersion.MaxSupported).GetCurrentSerializer();
            CollectionAssert.AreEqual(batch.Serializer.Serialize("id1-value"), batch.RoutingKey.RawRoutingKey);
        }

        [Test]
        public void BatchStatement_Bind_SetsRoutingValues_Multiple()
        {
            var batch = new BatchStatement();
            Assert.Null(batch.RoutingKey);
            batch.SetRoutingValues("id11", "id22");
            batch.Serializer = new SerializerManager(ProtocolVersion.MaxSupported).GetCurrentSerializer();
            var expectedRoutingKey = new byte[0]
                .Concat(new byte[] { 0, 4 })
                .Concat(batch.Serializer.Serialize("id11"))
                .Concat(new byte[] { 0 })
                .Concat(new byte[] { 0, 4 })
                .Concat(batch.Serializer.Serialize("id22"))
                .Concat(new byte[] { 0 });
            CollectionAssert.AreEqual(expectedRoutingKey, batch.RoutingKey.RawRoutingKey);
        }

        [Test]
        public void BatchStatement_Should_Throw_When_Child_Statement_Has_Proxy_Auth_Set()
        {
            var batch = new BatchStatement();
            var childStatement = new SimpleStatement("DELETE FROM tbl1 WHERE KEY = ?", Guid.NewGuid());
            childStatement.ExecutingAs("bob");
            Assert.Throws<ArgumentException>(() => batch.Add(childStatement));
        }

        [Test]
        public void BatchStatement_Max_Queries_Test()
        {
            var batch = new BatchStatement();
            var id = Guid.NewGuid();
            for (int i = 0; i < ushort.MaxValue; i++)
            {
                batch.Add(new SimpleStatement("QUERY", id));
            }
            // It shouldn't allow more
            Assert.Throws<ArgumentOutOfRangeException>(() => batch.Add(new SimpleStatement("QUERY", id)));
        }

        [Test]
        public void BatchStatement_Should_Use_Routing_Key_Of_First_Statement_With_SimpleStatement_Instances()
        {
            var rawRoutingKey = new byte[] {1, 2, 3, 4};
            var s1 = new SimpleStatement("Q1").SetRoutingKey(new RoutingKey(rawRoutingKey));
            var s2 = new SimpleStatement("Q2").SetRoutingKey(new RoutingKey(new byte[] { 100, 101, 102 }));
            var batch = new BatchStatement().Add(s1).Add(s2);
            Assert.AreEqual(BitConverter.ToString(rawRoutingKey),
                BitConverter.ToString(batch.RoutingKey.RawRoutingKey));
        }

        [Test]
        public void BatchStatement_Should_Use_Routing_Key_Of_First_Statement_With_Statement_Instances()
        {
            var rawRoutingKey = new byte[] {1, 2, 3, 4};
            var s1MockCalled = 0;
            var s1MockCalledKeyspace = 0;
            var s2MockCalled = 0;
            var s2MockCalledKeyspace = 0;

            var s1Mock = new Mock<Statement>(MockBehavior.Loose);
            s1Mock.Setup(s => s.RoutingKey).Returns(new RoutingKey(rawRoutingKey)).Callback(() => s1MockCalled++);
            s1Mock.Setup(s => s.Keyspace).Returns("ks1").Callback(() => s1MockCalledKeyspace++);
            var s2Mock = new Mock<Statement>(MockBehavior.Loose);
            s2Mock.Setup(s => s.RoutingKey).Returns((RoutingKey)null).Callback(() => s2MockCalled++);
            s2Mock.Setup(s => s.Keyspace).Returns((string)null).Callback(() => s2MockCalledKeyspace++);

            var batch = new BatchStatement().Add(s1Mock.Object).Add(s2Mock.Object);
            Assert.AreEqual(BitConverter.ToString(rawRoutingKey),
                BitConverter.ToString(batch.RoutingKey.RawRoutingKey));
            Assert.AreEqual("ks1", batch.Keyspace);

            Assert.AreEqual(1, s1MockCalled);
            Assert.AreEqual(0, s2MockCalled);
            Assert.AreEqual(1, s1MockCalledKeyspace);
            Assert.AreEqual(0, s2MockCalledKeyspace);
        }

        [Test]
        public void BatchStatement_Should_UseRoutingKeyAndKeyspaceOfFirstStatement_When_TokenAwareLbpIsUsed()
        {
            var rawRoutingKey = new byte[] {1, 2, 3, 4};
            var lbp = new TokenAwarePolicy(new ClusterTests.FakeLoadBalancingPolicy());
            var clusterMock = Mock.Of<IInternalCluster>();
            Mock.Get(clusterMock).Setup(c => c.GetReplicas(It.IsAny<string>(), It.IsAny<byte[]>()))
                .Returns(new List<Host>());
            Mock.Get(clusterMock).Setup(c => c.AllHosts())
                .Returns(new List<Host>());
            lbp.Initialize(clusterMock);
            
            var s1Mock = new Mock<Statement>(MockBehavior.Loose);
            s1Mock.Setup(s => s.RoutingKey).Returns(new RoutingKey(rawRoutingKey));
            s1Mock.Setup(s => s.Keyspace).Returns("ks1");
            var batch = new BatchStatement().Add(s1Mock.Object);

            var _ = lbp.NewQueryPlan("ks2", batch).ToList();

            Mock.Get(clusterMock).Verify(c => c.GetReplicas("ks1", rawRoutingKey), Times.Once);
        }
    }
}
