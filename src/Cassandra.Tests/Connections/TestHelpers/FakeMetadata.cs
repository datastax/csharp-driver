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
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

using Cassandra.Connections.Control;

namespace Cassandra.Tests.Connections.TestHelpers
{
    internal class FakeMetadata : IMetadata
    {
        public FakeMetadata(Configuration config)
        {
            Configuration = config;
            InternalMetadata = new FakeInternalMetadata(config);
            SetupEventForwarding(InternalMetadata);
        }

        public FakeMetadata(IInternalMetadata internalMetadata)
        {
            Configuration = internalMetadata.Configuration;
            InternalMetadata = internalMetadata;
            SetupEventForwarding(internalMetadata);
        }

        public event HostsEventHandler HostsEvent;

        public event SchemaChangedEventHandler SchemaChangedEvent;

        public event Action<Host> HostAdded;

        public event Action<Host> HostRemoved;

        public Configuration Configuration { get; }

        internal IInternalMetadata InternalMetadata { get; }

        public Task<ClusterDescription> GetClusterDescriptionAsync()
        {
            throw new NotImplementedException();
        }

        public ClusterDescription GetClusterDescription()
        {
            throw new NotImplementedException();
        }

        public ICollection<Host> AllHostsSnapshot()
        {
            return InternalMetadata.AllHosts();
        }

        public IEnumerable<IPEndPoint> AllReplicasSnapshot()
        {
            return InternalMetadata.AllReplicas();
        }

        public ICollection<Host> GetReplicasSnapshot(string keyspaceName, byte[] partitionKey)
        {
            return InternalMetadata.GetReplicas(keyspaceName, partitionKey);
        }

        public ICollection<Host> GetReplicasSnapshot(byte[] partitionKey)
        {
            return InternalMetadata.GetReplicas(partitionKey);
        }

        public Host GetHost(IPEndPoint address)
        {
            throw new NotImplementedException();
        }

        public Task<Host> GetHostAsync(IPEndPoint address)
        {
            throw new NotImplementedException();
        }

        public ICollection<Host> AllHosts()
        {
            throw new NotImplementedException();
        }

        public Task<ICollection<Host>> AllHostsAsync()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IPEndPoint> AllReplicas()
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<IPEndPoint>> AllReplicasAsync()
        {
            throw new NotImplementedException();
        }

        public ICollection<Host> GetReplicas(string keyspaceName, byte[] partitionKey)
        {
            throw new NotImplementedException();
        }

        public ICollection<Host> GetReplicas(byte[] partitionKey)
        {
            throw new NotImplementedException();
        }

        public Task<ICollection<Host>> GetReplicasAsync(string keyspaceName, byte[] partitionKey)
        {
            throw new NotImplementedException();
        }

        public Task<ICollection<Host>> GetReplicasAsync(byte[] partitionKey)
        {
            throw new NotImplementedException();
        }

        public KeyspaceMetadata GetKeyspace(string keyspace)
        {
            throw new NotImplementedException();
        }

        public Task<KeyspaceMetadata> GetKeyspaceAsync(string keyspace)
        {
            throw new NotImplementedException();
        }

        public ICollection<string> GetKeyspaces()
        {
            throw new NotImplementedException();
        }

        public Task<ICollection<string>> GetKeyspacesAsync()
        {
            throw new NotImplementedException();
        }

        public ICollection<string> GetTables(string keyspace)
        {
            throw new NotImplementedException();
        }

        public Task<ICollection<string>> GetTablesAsync(string keyspace)
        {
            throw new NotImplementedException();
        }

        public TableMetadata GetTable(string keyspace, string tableName)
        {
            throw new NotImplementedException();
        }

        public Task<TableMetadata> GetTableAsync(string keyspace, string tableName)
        {
            throw new NotImplementedException();
        }

        public MaterializedViewMetadata GetMaterializedView(string keyspace, string name)
        {
            throw new NotImplementedException();
        }

        public Task<MaterializedViewMetadata> GetMaterializedViewAsync(string keyspace, string name)
        {
            throw new NotImplementedException();
        }

        public UdtColumnInfo GetUdtDefinition(string keyspace, string typeName)
        {
            throw new NotImplementedException();
        }

        public Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspace, string typeName)
        {
            throw new NotImplementedException();
        }

        public FunctionMetadata GetFunction(string keyspace, string name, string[] signature)
        {
            throw new NotImplementedException();
        }

        public Task<FunctionMetadata> GetFunctionAsync(string keyspace, string name, string[] signature)
        {
            throw new NotImplementedException();
        }

        public AggregateMetadata GetAggregate(string keyspace, string name, string[] signature)
        {
            throw new NotImplementedException();
        }

        public Task<AggregateMetadata> GetAggregateAsync(string keyspace, string name, string[] signature)
        {
            throw new NotImplementedException();
        }

        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            throw new NotImplementedException();
        }

        public Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)
        {
            throw new NotImplementedException();
        }

        public Task<bool> CheckSchemaAgreementAsync()
        {
            throw new NotImplementedException();
        }
        
        private void OnInternalHostRemoved(Host h)
        {
            HostRemoved?.Invoke(h);
        }

        private void OnInternalHostAdded(Host h)
        {
            HostAdded?.Invoke(h);
        }
        
        private void OnInternalHostsEvent(object sender, HostsEventArgs args)
        {
            HostsEvent?.Invoke(sender, args);
        }

        private void OnInternalSchemaChangedEvent(object sender, SchemaChangedEventArgs args)
        {
            SchemaChangedEvent?.Invoke(sender, args);
        }

        private void SetupEventForwarding(IInternalMetadata internalMetadata)
        {
            internalMetadata.HostAdded += OnInternalHostAdded;
            internalMetadata.HostRemoved += OnInternalHostRemoved;
            internalMetadata.HostsEvent += OnInternalHostsEvent;
            internalMetadata.SchemaChangedEvent += OnInternalSchemaChangedEvent;
        }
    }
}