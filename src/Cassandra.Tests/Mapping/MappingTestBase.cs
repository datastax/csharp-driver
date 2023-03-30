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
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Serialization;
using Cassandra.Tasks;
using Cassandra.Tests.Mapping.FluentMappings;
using Cassandra.Tests.Mapping.Pocos;
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
            return GetMappingClientAndSession(getRowSetFunc, queryCallback, config).Mapper;
        }

        protected MapperAndSessionTuple GetMappingClientAndSession(RowSet rowset, MappingConfiguration config = null)
        {
            return GetMappingClientAndSession(() => TaskHelper.ToTask(rowset), config);
        }

        protected MapperAndSessionTuple GetMappingClientAndSession(Func<Task<RowSet>> getRowSetFunc, MappingConfiguration config = null)
        {
            return GetMappingClientAndSession(getRowSetFunc, null, config);
        }
        
        protected MapperAndSessionTuple GetMappingClientAndSession(Func<Task<RowSet>> getRowSetFunc, Action<string, object[]> queryCallback, MappingConfiguration config = null)
        {
            if (queryCallback == null)
            {
                //noop
                queryCallback = (q, p) => { };
            }
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(getRowSetFunc)
                .Callback<BoundStatement>(s => queryCallback(s.PreparedStatement.Cql, s.QueryValues))
                .Verifiable();
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(getRowSetFunc)
                .Callback<BoundStatement, string>((s, profile) => queryCallback(s.PreparedStatement.Cql, s.QueryValues))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(q => TaskHelper.ToTask(GetPrepared(q)))
                .Verifiable();
            return new MapperAndSessionTuple
            {
                Mapper = GetMappingClient(sessionMock, config),
                Session = sessionMock.Object
            };
        }

        protected IMapper GetMappingClient(Mock<ISession> sessionMock, MappingConfiguration config = null)
        {
            if (config == null)
            {
                config = new MappingConfiguration().Define(new FluentUserMapping());
            }
            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.Configuration).Returns(new Configuration());
            sessionMock.Setup(s => s.Cluster).Returns(clusterMock.Object);
            return new Mapper(sessionMock.Object, config);
        }

        protected ISession GetSession(Action<string, object[]> callback, RowSet rs = null)
        {
            return GetSession<BoundStatement>(rs, stmt => callback(stmt.PreparedStatement.Cql, stmt.QueryValues));
        }

        protected ISession GetSession<TStatement>(RowSet rs, Action<TStatement> callback, ProtocolVersion protocolVersion = ProtocolVersion.MaxSupported)
            where TStatement : IStatement
        {
            if (rs == null)
            {
                rs = new RowSet();
            }
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.Configuration).Returns(new Configuration());
            sessionMock.Setup(s => s.Cluster).Returns(clusterMock.Object);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .Returns(() => TaskHelper.ToTask(rs))
                .Callback(callback)
                .Verifiable();
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<IStatement>(), It.IsAny<string>()))
                .Returns(() => TaskHelper.ToTask(rs))
                .Callback<IStatement, string>(
                    (stmt, execProfile) => 
                        callback((TStatement)stmt))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(query => TaskHelper.ToTask(GetPrepared(query)))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(query => TaskHelper.ToTask(GetPrepared(query)))
                .Verifiable();
            sessionMock
                .Setup(s => s.BinaryProtocolVersion)
                .Returns((int)protocolVersion);
            return sessionMock.Object;
        }

        /// <summary>
        /// Gets a IQueryProvider with a new mapping configuration containing the definition provided
        /// </summary>
        protected Table<T> GetTable<T>(ISession session, ITypeDefinition definition = null)
        {
            var config = new MappingConfiguration();
            if (definition != null)
            {
                config.Define(definition);
            }
            return new Table<T>(session, config);
        }

        /// <summary>
        /// Gets a dummy prepared statement with the query provided
        /// </summary>
        protected PreparedStatement GetPrepared(string query = null)
        {
            return new PreparedStatement(
                null, null, null, query, null, new SerializerManager(ProtocolVersion.MaxSupported));
        }

        protected void TestQueryTrace(Func<Table<AllTypesEntity>, QueryTrace> queryExecutor)
        {
            var rs = new RowSet();

            var clusterMock = new Mock<ICluster>();
            clusterMock.Setup(c => c.Configuration).Returns(new Configuration());
            
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock.Setup(s => s.Cluster).Returns(clusterMock.Object);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .ReturnsAsync(() => rs)
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(query => TaskHelper.ToTask(GetPrepared(query)))
                .Verifiable();
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<IStatement>(), It.IsAny<string>()))
                .ReturnsAsync(() => rs)
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(query => TaskHelper.ToTask(GetPrepared(query)))
                .Verifiable();

            var trace = new Mock<QueryTrace>(MockBehavior.Strict, Guid.NewGuid(), sessionMock.Object);
            trace.Setup(t => t.ToString()).Returns("instance");
            rs.Info.SetQueryTrace(trace.Object);

            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.IntValue, cm => cm.WithName("id"))
                .Column(t => t.StringValue, cm => cm.WithName("val"))
                .PartitionKey(t => t.IntValue)
                .TableName("tbl1");
            var table = GetTable<AllTypesEntity>(sessionMock.Object, map);
            var resultTrace = queryExecutor(table);
            Assert.AreSame(rs.Info.QueryTrace, resultTrace);
        }

        [TearDown]
        public virtual void TearDown()
        {
            //Clear the global mapping between tests
            MappingConfiguration.Global.Clear();
        }

        protected class MapperAndSessionTuple
        {
            public IMapper Mapper { get; set; }

            public ISession Session { get; set; }
        }
    }
}
