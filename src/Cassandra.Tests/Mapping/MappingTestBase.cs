using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Serialization;
using Cassandra.Tasks;
using Cassandra.Tests.Mapping.FluentMappings;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    public abstract class MappingTestBase
    {
        protected IMapper GetMappingClient(RowSet rowset, MappingConfiguration config = null)
        {
            return GetMappingClient(() => TaskHelper.ToTask(rowset), config);
        }

        protected IMapper GetMappingClient(Func<Task<RowSet>> getRowSetFunc, MappingConfiguration config = null)
        {
            return GetMappingClient(getRowSetFunc, null, config);
        }

        protected IMapper GetMappingClient(Func<Task<RowSet>> getRowSetFunc, Action<string, object[]> queryCallback, MappingConfiguration config = null)
        {
            if (queryCallback == null)
            {
                //noop
                queryCallback = (q, p) => { };
            }
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(getRowSetFunc)
                .Callback<BoundStatement>(s => queryCallback(s.PreparedStatement.Cql, s.QueryValues))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(q => TaskHelper.ToTask(GetPrepared(q)))
                .Verifiable();
            return GetMappingClient(sessionMock, config);
        }

        protected IMapper GetMappingClient(Mock<ISession> sessionMock, MappingConfiguration config = null)
        {
            if (config == null)
            {
                config = new MappingConfiguration().Define(new FluentUserMapping());
            }
            sessionMock.Setup(s => s.Cluster).Returns((ICluster)null);
            return new Mapper(sessionMock.Object, config);
        }

        protected ISession GetSession(Action<string, object[]> callback, RowSet rs = null)
        {
            return GetSession(rs, stmt => callback(stmt.PreparedStatement.Cql, stmt.QueryValues));
        }

        protected ISession GetSession(RowSet rs, Action<BoundStatement> callback)
        {
            if (rs == null)
            {
                rs = new RowSet();
            }
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Cluster).Returns((ICluster)null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(() => TaskHelper.ToTask(rs))
                .Callback(callback)
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(query => TaskHelper.ToTask(GetPrepared(query)))
                .Verifiable();
            return sessionMock.Object;
        }

        /// <summary>
        /// Gets a IQueryProvider with a new mapping configuration containing the definition provided
        /// </summary>
        protected Table<T> GetTable<T>(ISession session, ITypeDefinition definition)
        {
            return new Table<T>(session, new MappingConfiguration().Define(definition));
        }

        /// <summary>
        /// Gets a dummy prepared statement with the query provided
        /// </summary>
        protected PreparedStatement GetPrepared(string query = null)
        {
            return new PreparedStatement(null, null, query, null, new Serializer(ProtocolVersion.MaxSupported));
        }

        [TearDown]
        public virtual void TearDown()
        {
            //Clear the global mapping between tests
            MappingConfiguration.Global.Clear();
        }
    }
}
