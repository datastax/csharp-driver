﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Tasks;
using Cassandra.Tests.Mapping.FluentMappings;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    public abstract class MappingTestBase
    {
        protected IMapper GetMappingClient(RowSet rowset)
        {
            return GetMappingClient(() => TaskHelper.ToTask(rowset));
        }

        protected IMapper GetMappingClient(Func<Task<RowSet>> getRowSetFunc)
        {
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(getRowSetFunc)
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            return GetMappingClient(sessionMock);
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
            if (rs == null)
            {
                rs = new RowSet();
            }
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Cluster).Returns((ICluster)null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(() => TaskHelper.ToTask(rs))
                .Callback<BoundStatement>(stmt => callback(stmt.PreparedStatement.Cql, stmt.QueryValues))
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
            return new PreparedStatement(null, null, query, null, 2);
        }

        [TearDown]
        public virtual void TearDown()
        {
            //Clear the global mapping between tests
            MappingConfiguration.Global.Clear();
        }
    }
}
