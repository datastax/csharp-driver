using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.FluentMappings;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    [TestFixture]
    public abstract class MappingTestBase
    {
        protected ICqlClient GetMappingClient(RowSet rowset)
        {
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(TaskHelper.ToTask(rowset))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(new PreparedStatement(null, null, null, null)))
                .Verifiable();
            return GetMappingClient(sessionMock);
        }

        protected ICqlClient GetMappingClient(Mock<ISession> sessionMock)
        {
            sessionMock.Setup(s => s.Cluster).Returns((ICluster)null);
            var mappingClient = CqlClientConfiguration
                .ForSession(sessionMock.Object)
                .UseIndividualMapping<FluentUserMapping>()
                .BuildCqlClient();
            return mappingClient;
        }
    }
}
