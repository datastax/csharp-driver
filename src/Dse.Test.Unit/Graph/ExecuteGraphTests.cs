using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            CollectionAssert.AreEqual(new object[] { "is what" }, coreStatement.QueryValues);
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
                    .SetAlias("Z")
                    .SetReadConsistencyLevel(ConsistencyLevel.LocalQuorum)
                    .SetWriteConsistencyLevel(ConsistencyLevel.EachQuorum));
            session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            Assert.NotNull(coreStatement);
            Assert.NotNull(coreStatement.OutgoingPayload);
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-source"]), "My source!");
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-name"]), "name1");
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-alias"]), "Z");
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-read-consistency"]), "LOCAL_QUORUM");
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-write-consistency"]), "EACH_QUORUM");
            //default
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-language"]), "gremlin-groovy");
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
                    .SetAlias("Z")
                    .SetReadConsistencyLevel(ConsistencyLevel.LocalQuorum)
                    .SetWriteConsistencyLevel(ConsistencyLevel.EachQuorum));
            session.ExecuteGraph(new SimpleGraphStatement("g.V()")
                .SetGraphLanguage("my-lang")
                .SetSystemQuery()
                .SetGraphAlias("X")
                .SetGraphReadConsistencyLevel(ConsistencyLevel.Two)
                .SetGraphSource("Statement source"));
            Assert.NotNull(coreStatement);
            Assert.NotNull(coreStatement.OutgoingPayload);
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-source"]), "Statement source");
            //is a sistem query
            Assert.False(coreStatement.OutgoingPayload.ContainsKey("graph-name"));
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-alias"]), "X");
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-read-consistency"]), "TWO");
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-write-consistency"]), "EACH_QUORUM");
            Assert.AreEqual(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-language"]), "my-lang");
        }
    }
}
