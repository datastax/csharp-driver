using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
            return GetMappingClient(() => TaskHelper.ToTask(rowset));
        }

        protected ICqlClient GetMappingClient(Func<Task<RowSet>> getRowSetFunc)
        {
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(getRowSetFunc)
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

        protected ISession GetSession(Action<string, object[]> callback, RowSet rs = null)
        {
            if (rs == null)
            {
                rs = new RowSet();
            }
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(() => TaskHelper.ToTask(rs))
                .Callback<BoundStatement>(stmt => callback(stmt.PreparedStatement.Cql, stmt.QueryValues))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(query => TaskHelper.ToTask(new PreparedStatement(null, null, query, null)))
                .Verifiable();
            return sessionMock.Object;
        }
    }
}
