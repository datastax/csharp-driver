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
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_SimpleStatemet()
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
        public void ExecuteGraph_Should_Build_Payload()
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
            CollectionAssert.AreEqual(new [] { "graph-language", "graph-source" }, coreStatement.OutgoingPayload.Keys);
        }
    }
}
