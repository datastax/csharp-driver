using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Cassandra;

namespace Dse
{
    public class DseSession : IDseSession
    {
        private int _binaryProtocolVersion;
        private ICluster _cluster;
        private bool _isDisposed;
        private string _keyspace;
        private UdtMappingDefinitions _userDefinedTypes;

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public IAsyncResult BeginExecute(IStatement statement, AsyncCallback callback, object state)
        {
            throw new NotImplementedException();
        }

        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, AsyncCallback callback, object state)
        {
            throw new NotImplementedException();
        }

        public IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state)
        {
            throw new NotImplementedException();
        }

        public void ChangeKeyspace(string keyspaceName)
        {
            throw new NotImplementedException();
        }

        public void CreateKeyspace(string keyspaceName, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            throw new NotImplementedException();
        }

        public void CreateKeyspaceIfNotExists(string keyspaceName, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            throw new NotImplementedException();
        }

        public void DeleteKeyspace(string keyspaceName)
        {
            throw new NotImplementedException();
        }

        public void DeleteKeyspaceIfExists(string keyspaceName)
        {
            throw new NotImplementedException();
        }

        public RowSet EndExecute(IAsyncResult ar)
        {
            throw new NotImplementedException();
        }

        public PreparedStatement EndPrepare(IAsyncResult ar)
        {
            throw new NotImplementedException();
        }

        public RowSet Execute(IStatement statement)
        {
            throw new NotImplementedException();
        }

        public RowSet Execute(string cqlQuery)
        {
            throw new NotImplementedException();
        }

        public RowSet Execute(string cqlQuery, ConsistencyLevel consistency)
        {
            throw new NotImplementedException();
        }

        public RowSet Execute(string cqlQuery, int pageSize)
        {
            throw new NotImplementedException();
        }

        public Task<RowSet> ExecuteAsync(IStatement statement)
        {
            throw new NotImplementedException();
        }

        public PreparedStatement Prepare(string cqlQuery)
        {
            throw new NotImplementedException();
        }

        public PreparedStatement Prepare(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            throw new NotImplementedException();
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery)
        {
            throw new NotImplementedException();
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            throw new NotImplementedException();
        }

        public void WaitForSchemaAgreement(RowSet rs)
        {
            throw new NotImplementedException();
        }

        public bool WaitForSchemaAgreement(IPEndPoint forHost)
        {
            throw new NotImplementedException();
        }

        public int BinaryProtocolVersion
        {
            get { return _binaryProtocolVersion; }
        }

        public ICluster Cluster
        {
            get { return _cluster; }
        }

        public bool IsDisposed
        {
            get { return _isDisposed; }
        }

        public string Keyspace
        {
            get { return _keyspace; }
        }

        public UdtMappingDefinitions UserDefinedTypes
        {
            get { return _userDefinedTypes; }
        }
    }
}
