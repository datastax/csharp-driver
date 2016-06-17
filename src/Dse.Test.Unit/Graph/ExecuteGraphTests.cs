//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Dse.Graph;
using Moq;
using NUnit.Framework;

namespace Dse.Test.Unit.Graph
{
    public class ExecuteGraphTests : BaseUnitTest
    {
        private static DseSession NewInstance(ISession coreSession, GraphOptions graphOptions = null)
        {
            //Cassandra Configuration does not have a public constructor
            //Create a dummy Cluster instance
            using (var cluster = DseCluster.Builder().AddContactPoint("127.0.0.1")
                .WithGraphOptions(graphOptions ?? new GraphOptions()).Build())
            {
                return new DseSession(coreSession, cluster.Configuration);   
            }
        }

        [Test]
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_SimpleStatement()
        {
            SimpleStatement coreStatement = null;
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskOf(new RowSet()))
                .Callback<SimpleStatement>(stmt => coreStatement = stmt)
                .Verifiable();
            var session = NewInstance(coreSessionMock.Object);
            session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            Assert.NotNull(coreStatement);
            Assert.Null(coreStatement.Timestamp);
            Assert.Null(coreStatement.ConsistencyLevel);
            coreSessionMock.Verify();
        }

        [Test]
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_Timestamp_Set()
        {
            SimpleStatement coreStatement = null;
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskOf(new RowSet()))
                .Callback<SimpleStatement>(stmt => coreStatement = stmt)
                .Verifiable();
            var session = NewInstance(coreSessionMock.Object);
            var timestamp = DateTimeOffset.Now;
            session.ExecuteGraph(new SimpleGraphStatement("g.V()").SetTimestamp(timestamp));
            Assert.NotNull(coreStatement);
            Assert.Null(coreStatement.ConsistencyLevel);
            Assert.AreEqual(coreStatement.Timestamp, timestamp);
            coreSessionMock.Verify();
        }

        [Test]
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_ConsistencyLevel_Set()
        {
            SimpleStatement coreStatement = null;
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskOf(new RowSet()))
                .Callback<SimpleStatement>(stmt => coreStatement = stmt)
                .Verifiable();
            var session = NewInstance(coreSessionMock.Object);
            const ConsistencyLevel consistency = ConsistencyLevel.Three;
            session.ExecuteGraph(new SimpleGraphStatement("g.V()").SetConsistencyLevel(consistency));
            Assert.NotNull(coreStatement);
            Assert.AreEqual(coreStatement.ConsistencyLevel, consistency);
            Assert.Null(coreStatement.Timestamp);
            coreSessionMock.Verify();
        }

        [Test]
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_ReadTimeout_Set_To_Default()
        {
            SimpleStatement coreStatement = null;
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskOf(new RowSet()))
                .Callback<SimpleStatement>(stmt => coreStatement = stmt)
                .Verifiable();
            const int readTimeout = 5000;
            var session = NewInstance(coreSessionMock.Object, new GraphOptions().SetReadTimeoutMillis(readTimeout));
            session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            Assert.NotNull(coreStatement);
            Assert.AreEqual(readTimeout, coreStatement.ReadTimeoutMillis);
            //Another one with the statement level timeout set to zero
            session.ExecuteGraph(new SimpleGraphStatement("g.V()").SetReadTimeoutMillis(0));
            Assert.NotNull(coreStatement);
            Assert.AreEqual(readTimeout, coreStatement.ReadTimeoutMillis);
            Assert.True(coreStatement.OutgoingPayload.ContainsKey("request-timeout"));
            Assert.That(coreStatement.OutgoingPayload["request-timeout"], Is.EqualTo(ToBuffer(readTimeout)));
            coreSessionMock.Verify();
        }

        [Test]
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_ReadTimeout_Set_From_Statement()
        {
            SimpleStatement coreStatement = null;
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskOf(new RowSet()))
                .Callback<SimpleStatement>(stmt => coreStatement = stmt)
                .Verifiable();
            const int defaultReadTimeout = 15000;
            const int readTimeout = 6000;
            var session = NewInstance(coreSessionMock.Object, 
                new GraphOptions().SetReadTimeoutMillis(defaultReadTimeout));
            session.ExecuteGraph(new SimpleGraphStatement("g.V()").SetReadTimeoutMillis(readTimeout));
            Assert.NotNull(coreStatement);
            Assert.AreEqual(readTimeout, coreStatement.ReadTimeoutMillis);
            Assert.True(coreStatement.OutgoingPayload.ContainsKey("request-timeout"));
            Assert.That(coreStatement.OutgoingPayload["request-timeout"], Is.EqualTo(ToBuffer(readTimeout)));
            coreSessionMock.Verify();
        }

        [Test]
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_Dictionary_Parameters_Set()
        {
            SimpleStatement coreStatement = null;
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskOf(new RowSet()))
                .Callback<SimpleStatement>(stmt => coreStatement = stmt)
                .Verifiable();
            var session = NewInstance(coreSessionMock.Object);
            var parameters = new Dictionary<string, object>
            {
                { "myName", "is what"}
            };
            session.ExecuteGraph(new SimpleGraphStatement(parameters, "g.V().has('name', myName)"));
            Assert.NotNull(coreStatement);
            Assert.AreEqual("g.V().has('name', myName)", coreStatement.QueryString);
            //A single parameter with the key/values json stringified
            Assert.AreEqual(new object[] { "{\"myName\":\"is what\"}" }, coreStatement.QueryValues);
            coreSessionMock.Verify();
        }

        [Test]
        public void ExecuteGraph_Should_Wrap_RowSet()
        {
            var rowMock1 = new Mock<Row>();
            rowMock1.Setup(r => r.GetValue<string>(It.Is<string>(n => n == "gremlin"))).Returns("{\"result\": 100}");
            var rowMock2 = new Mock<Row>();
            rowMock2.Setup(r => r.GetValue<string>(It.Is<string>(n => n == "gremlin"))).Returns("{\"result\": 101}");
            IEnumerable<Row> rows = new []
            {
                rowMock1.Object,
                rowMock2.Object
            };
            var rsMock = new Mock<RowSet>();
            rsMock.Setup(r => r.GetEnumerator()).Returns(() => rows.GetEnumerator());

            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskOf(rsMock.Object))
                .Verifiable();
            var session = NewInstance(coreSessionMock.Object);
            var rsGraph = session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            coreSessionMock.Verify();
            Assert.NotNull(rsGraph);
            var resultArray = rsGraph.ToArray();
            Assert.AreEqual(2, resultArray.Length);
            CollectionAssert.AreEqual(new[] {100, 101}, resultArray.Select(g => g.ToInt32()));
        }

        [Test]
        public void ExecuteGraph_Should_Build_Payload_With_Default_Values()
        {
            SimpleStatement coreStatement = null;
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskOf(new RowSet()))
                .Callback<SimpleStatement>(stmt => coreStatement = stmt);
            var session = NewInstance(coreSessionMock.Object);
            session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            Assert.NotNull(coreStatement);
            Assert.NotNull(coreStatement.OutgoingPayload);
            //The default graph payload
            Assert.AreEqual(2, coreStatement.OutgoingPayload.Count);
            CollectionAssert.AreEqual(new[] { "graph-language", "graph-source" }, coreStatement.OutgoingPayload.Keys);
        }

        [Test]
        public void ExecuteGraph_Should_Build_Payload_With_GraphOptions()
        {
            SimpleStatement coreStatement = null;
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskOf(new RowSet()))
                .Callback<SimpleStatement>(stmt => coreStatement = stmt);
            var session = NewInstance(coreSessionMock.Object,
                new GraphOptions()
                    .SetName("name1")
                    .SetSource("My source!")
                    .SetReadTimeoutMillis(22222)
                    .SetReadConsistencyLevel(ConsistencyLevel.LocalQuorum)
                    .SetWriteConsistencyLevel(ConsistencyLevel.EachQuorum));
            session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            Assert.NotNull(coreStatement);
            Assert.NotNull(coreStatement.OutgoingPayload);
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-source"]), Is.EqualTo("My source!"));
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-name"]), "name1");
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-read-consistency"]), "LOCAL_QUORUM");
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-write-consistency"]), "EACH_QUORUM");
            //default
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-language"]), "gremlin-groovy");
            Assert.That(coreStatement.OutgoingPayload["request-timeout"], Is.EqualTo(ToBuffer(22222)));
        }

        [Test]
        public void ExecuteGraph_Should_Build_Payload_With_Statement_Properties()
        {
            SimpleStatement coreStatement = null;
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskOf(new RowSet()))
                .Callback<SimpleStatement>(stmt => coreStatement = stmt);
            var session = NewInstance(coreSessionMock.Object,
                new GraphOptions()
                    .SetName("name1")
                    .SetSource("My source!")
                    .SetReadConsistencyLevel(ConsistencyLevel.LocalQuorum)
                    .SetWriteConsistencyLevel(ConsistencyLevel.EachQuorum));
            session.ExecuteGraph(new SimpleGraphStatement("g.V()")
                .SetGraphLanguage("my-lang")
                .SetReadTimeoutMillis(5555)
                .SetSystemQuery()
                .SetGraphReadConsistencyLevel(ConsistencyLevel.Two)
                .SetGraphSource("Statement source"));
            Assert.NotNull(coreStatement);
            Assert.NotNull(coreStatement.OutgoingPayload);
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-source"]), Is.EqualTo("Statement source"));
            //is a sistem query
            Assert.False(coreStatement.OutgoingPayload.ContainsKey("graph-name"));
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-read-consistency"]), "TWO");
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-write-consistency"]), "EACH_QUORUM");
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-language"]), "my-lang");
            Assert.That(coreStatement.OutgoingPayload["request-timeout"], Is.EqualTo(ToBuffer(5555)));
        }

        [Test]
        public void ExecuteGraph_Should_Allow_GraphNode_As_Parameters()
        {
            SimpleStatement coreStatement = null;
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskOf(new RowSet()))
                .Callback<SimpleStatement>(stmt => coreStatement = stmt);
            var session = NewInstance(coreSessionMock.Object);
            const string expectedJson =
                "{\"member_id\":123,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}";
            var id = new GraphNode("{\"result\":" + expectedJson + "}");
            session.ExecuteGraph(new SimpleGraphStatement("g.V(vertexId)", new {vertexId = id}));
            Assert.NotNull(coreStatement);
            Assert.AreEqual(1, coreStatement.QueryValues.Length);
            Assert.AreEqual("{\"vertexId\":" + expectedJson + "}", coreStatement.QueryValues[0]);
        }

        public void Should_Make_Rpc_Call_When_Using_Analytics_Source()
        {
            var coreStatements = new List<SimpleStatement>();
            var coreClusterMock = new Mock<ICluster>(MockBehavior.Strict);
            coreClusterMock.Setup(c => c.GetHost(It.IsAny<IPEndPoint>()))
                .Returns<IPEndPoint>(address => new Host(address, ReconnectionPolicy));
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<SimpleStatement>()))
                .Returns<SimpleStatement>(stmt =>
                {
                    if (stmt.QueryString.StartsWith("CALL "))
                    {
                        var rowMock = new Mock<Row>();
                        rowMock
                            .Setup(r => r.GetValue<IDictionary<string, string>>(It.Is<string>(c => c == "result")))
                            .Returns(new Dictionary<string, string> { {"location", "1.2.3.4:8888"} });
                        var rows = new []
                        {
                            rowMock.Object
                        };
                        var mock = new Mock<RowSet>();
                        mock
                            .Setup(r => r.GetEnumerator()).Returns(() => ((IEnumerable<Row>)rows).GetEnumerator());
                        return TaskOf(mock.Object);
                    }
                    return TaskOf(new RowSet());
                })
                .Callback<SimpleStatement>(stmt => coreStatements.Add(stmt));
            coreSessionMock.Setup(s => s.Cluster).Returns(coreClusterMock.Object);
            var session = NewInstance(coreSessionMock.Object);
            session.ExecuteGraph(new SimpleGraphStatement("g.V()").SetGraphSourceAnalytics());
            Assert.AreEqual(2, coreStatements.Count);
            Assert.AreEqual("CALL DseClientTool.getAnalyticsGraphServer()", coreStatements[0].QueryString);
            Assert.AreEqual("g.V()", coreStatements[1].QueryString);
            var targettedStatement = coreStatements[1] as TargettedSimpleStatement;
            Assert.NotNull(targettedStatement);
            Assert.NotNull(targettedStatement.PreferredHost);
            Assert.AreEqual("1.2.3.4:9042", targettedStatement.PreferredHost.Address.ToString());
        }

        [Test]
        public void Should_Not_Make_Rpc_Calls_When_Using_Other_Sources()
        {
            var coreStatements = new List<SimpleStatement>();
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<SimpleStatement>()))
                .Returns<SimpleStatement>(stmt => TaskOf(new RowSet()))
                .Callback<SimpleStatement>(stmt => coreStatements.Add(stmt));
            var session = NewInstance(coreSessionMock.Object);
            session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            Assert.AreEqual(1, coreStatements.Count);
            Assert.AreEqual("g.V()", coreStatements[0].QueryString);
            var targettedStatement = coreStatements[0] as TargettedSimpleStatement;
            Assert.NotNull(targettedStatement);
            Assert.Null(targettedStatement.PreferredHost);
        }
        
        [Test]
        public void Should_Identity_Timeout_Infinite_ReadTimeout()
        {
            SimpleStatement coreStatement = null;
            var coreSessionMock = new Mock<ISession>(MockBehavior.Strict);
            coreSessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(TaskOf(new RowSet()))
                .Callback<SimpleStatement>(stmt => coreStatement = stmt);
            var session = NewInstance(coreSessionMock.Object,
                new GraphOptions().SetReadTimeoutMillis(32000));
            session.ExecuteGraph(new SimpleGraphStatement("g.V()")
                .SetReadTimeoutMillis(Timeout.Infinite));
            Assert.NotNull(coreStatement);
            Assert.NotNull(coreStatement.OutgoingPayload);
            Assert.False(coreStatement.OutgoingPayload.ContainsKey("request-timeout"));
            Assert.That(coreStatement.ReadTimeoutMillis, Is.EqualTo(int.MaxValue));
        }

        private static byte[] ToBuffer(long value)
        {
            return Cassandra.Serialization.TypeSerializer.PrimitiveLongSerializer.Serialize(4, value);
        }
    }
}
