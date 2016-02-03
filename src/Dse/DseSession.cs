using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using Dse.Graph;

namespace Dse
{
    internal class DseSession : IDseSession
    {
        private readonly ISession _coreSession;

        public DseSession(ISession coreSession)
        {
            if (coreSession == null)
            {
                throw new ArgumentNullException("coreSession");
            }
            _coreSession = coreSession;
        }

        public GraphResultSet ExecuteGraph(IGraphStatement statement)
        {
            throw new NotImplementedException();
        }

        public Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement statement)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            _coreSession.Dispose();
        }

        public IAsyncResult BeginExecute(IStatement statement, AsyncCallback callback, object state)
        {
            return _coreSession.BeginExecute(statement, callback, state);
        }

        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, AsyncCallback callback, object state)
        {
            return _coreSession.BeginExecute(cqlQuery, consistency, callback, state);
        }

        public IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state)
        {
            return _coreSession.BeginPrepare(cqlQuery, callback, state);
        }

        public void ChangeKeyspace(string keyspaceName)
        {
            _coreSession.ChangeKeyspace(keyspaceName);
        }

        public void CreateKeyspace(string keyspaceName, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            _coreSession.CreateKeyspace(keyspaceName, replication, durableWrites);
        }

        public void CreateKeyspaceIfNotExists(string keyspaceName, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            _coreSession.CreateKeyspaceIfNotExists(keyspaceName, replication, durableWrites);
        }

        public void DeleteKeyspace(string keyspaceName)
        {
            _coreSession.DeleteKeyspace(keyspaceName);
        }

        public void DeleteKeyspaceIfExists(string keyspaceName)
        {
            _coreSession.DeleteKeyspaceIfExists(keyspaceName);
        }

        public RowSet EndExecute(IAsyncResult ar)
        {
            return _coreSession.EndExecute(ar);
        }

        public PreparedStatement EndPrepare(IAsyncResult ar)
        {
            return _coreSession.EndPrepare(ar);
        }

        public RowSet Execute(IStatement statement)
        {
            return _coreSession.Execute(statement);
        }

        public RowSet Execute(string cqlQuery)
        {
            return _coreSession.Execute(cqlQuery);
        }

        public RowSet Execute(string cqlQuery, ConsistencyLevel consistency)
        {
            return _coreSession.Execute(cqlQuery, consistency);
        }

        public RowSet Execute(string cqlQuery, int pageSize)
        {
            return _coreSession.Execute(cqlQuery, pageSize);
        }

        public Task<RowSet> ExecuteAsync(IStatement statement)
        {
            return _coreSession.ExecuteAsync(statement);
        }

        public PreparedStatement Prepare(string cqlQuery)
        {
            return _coreSession.Prepare(cqlQuery);
        }

        public PreparedStatement Prepare(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            return _coreSession.Prepare(cqlQuery, customPayload);
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery)
        {
            return _coreSession.PrepareAsync(cqlQuery);
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            return _coreSession.PrepareAsync(cqlQuery, customPayload);
        }

        public void WaitForSchemaAgreement(RowSet rs)
        {
            #pragma warning disable 618
            _coreSession.WaitForSchemaAgreement(rs);
            #pragma warning restore 618
        }

        public bool WaitForSchemaAgreement(IPEndPoint forHost)
        {
            #pragma warning disable 618
            return _coreSession.WaitForSchemaAgreement(forHost);
            #pragma warning restore 618
        }

        public int BinaryProtocolVersion
        {
            get { return _coreSession.BinaryProtocolVersion; }
        }

        public ICluster Cluster
        {
            get { return _coreSession.Cluster; }
        }

        public bool IsDisposed
        {
            get { return _coreSession.IsDisposed; }
        }

        public string Keyspace
        {
            get { return _coreSession.Keyspace; }
        }

        public UdtMappingDefinitions UserDefinedTypes
        {
            get { return _coreSession.UserDefinedTypes; }
        }
    }
}
