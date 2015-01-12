using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    [TestFixture]
    public class ExecuteTests : MappingTestBase
    {
        [Test]
        public void ExecuteAsync_Sets_Consistency()
        {
            ConsistencyLevel? consistency = null;
            ConsistencyLevel? serialConsistency = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Callback<IStatement>(b =>
                {
                    consistency = b.ConsistencyLevel;
                    serialConsistency = b.SerialConsistencyLevel;
                })
                .Returns(() => TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(new PreparedStatement(null, null, null, null)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            mapper.ExecuteAsync(new Cql("UPDATE").WithOptions(o => o.SetConsistencyLevel(ConsistencyLevel.EachQuorum).SetSerialConsistencyLevel(ConsistencyLevel.Serial))).Wait();
            Assert.AreEqual(ConsistencyLevel.EachQuorum, consistency);
            Assert.AreEqual(ConsistencyLevel.Serial, serialConsistency);
        }
    }
}
