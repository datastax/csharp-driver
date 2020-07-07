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
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Collections;
using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.MetadataHelpers;
using Cassandra.Serialization;

namespace Cassandra.Tests.Connections.TestHelpers
{
    internal class FakeInternalMetadata : IInternalMetadata
    {
        private volatile CopyOnWriteDictionary<IContactPoint, IEnumerable<IConnectionEndPoint>> _resolvedContactPoints =
            new CopyOnWriteDictionary<IContactPoint, IEnumerable<IConnectionEndPoint>>();

        public FakeInternalMetadata(Configuration config)
        {
            Configuration = config;
            Hosts = new Hosts();
            Hosts.Down += OnHostDown;
            Hosts.Up += OnHostUp;
        }

        public event HostsEventHandler HostsEvent;

        public event SchemaChangedEventHandler SchemaChangedEvent;

        public event Action<Host> HostAdded;

        public event Action<Host> HostRemoved;

        public Configuration Configuration { get; }

        public IControlConnection ControlConnection { get; }

        public ISerializerManager SerializerManager => throw new NotImplementedException();

        public ISchemaParser SchemaParser => throw new NotImplementedException();

        public string Partitioner => throw new NotImplementedException();

        public Hosts Hosts { get; }

        public IReadOnlyDictionary<IContactPoint, IEnumerable<IConnectionEndPoint>> ResolvedContactPoints =>
            _resolvedContactPoints;

        public IReadOnlyTokenMap TokenToReplicasMap => throw new NotImplementedException();

        public bool IsDbaas => throw new NotImplementedException();

        public string ClusterName => throw new NotImplementedException();

        public ProtocolVersion ProtocolVersion => throw new NotImplementedException();

        public KeyValuePair<string, KeyspaceMetadata>[] KeyspacesSnapshot => throw new NotImplementedException();

        public Task InitAsync(CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public Task ShutdownAsync()
        {
            throw new NotImplementedException();
        }

        public void SetResolvedContactPoints(IDictionary<IContactPoint, IEnumerable<IConnectionEndPoint>> resolvedContactPoints)
        {
            _resolvedContactPoints =
                new CopyOnWriteDictionary<IContactPoint, IEnumerable<IConnectionEndPoint>>(resolvedContactPoints);
        }
        
        public Host GetHost(IPEndPoint address)
        {
            Hosts.TryGet(address, out var h);
            return h;
        }

        public Host AddHost(IPEndPoint address)
        {
            return Hosts.Add(address);
        }

        public Host AddHost(IPEndPoint address, IContactPoint contactPoint)
        {
            return Hosts.Add(address, contactPoint);
        }

        public void RemoveHost(IPEndPoint address)
        {
            Hosts.RemoveIfExists(address);
        }
        
        public void OnHostRemoved(Host h)
        {
            HostRemoved?.Invoke(h);
        }

        public void OnHostAdded(Host h)
        {
            HostAdded?.Invoke(h);
        }

        public void FireSchemaChangedEvent(SchemaChangedEventArgs.Kind what, string keyspace, string table, object sender = null)
        {
            SchemaChangedEvent?.Invoke(sender ?? this, new SchemaChangedEventArgs { Keyspace = keyspace, What = what, Table = table });
        }

        public void OnHostDown(Host h)
        {
            HostsEvent?.Invoke(this, new HostsEventArgs { Address = h.Address, What = HostsEventArgs.Kind.Down });
        }

        public void OnHostUp(Host h)
        {
            HostsEvent?.Invoke(h, new HostsEventArgs { Address = h.Address, What = HostsEventArgs.Kind.Up });
        }

        public ICollection<Host> AllHosts()
        {
            return Hosts.ToCollection();
        }

        public IEnumerable<IPEndPoint> AllReplicas()
        {
            throw new NotImplementedException();
        }

        public Task RebuildTokenMapAsync(bool retry, bool fetchKeyspaces)
        {
            throw new NotImplementedException();
        }

        public bool RemoveKeyspaceFromTokenMap(string name)
        {
            throw new NotImplementedException();
        }

        public Task<KeyspaceMetadata> UpdateTokenMapForKeyspace(string name)
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

        public Task<KeyspaceMetadata> GetKeyspaceAsync(string keyspace)
        {
            throw new NotImplementedException();
        }

        public Task<ICollection<string>> GetKeyspacesAsync()
        {
            throw new NotImplementedException();
        }

        public Task<ICollection<string>> GetTablesAsync(string keyspace)
        {
            throw new NotImplementedException();
        }

        public Task<TableMetadata> GetTableAsync(string keyspace, string tableName)
        {
            throw new NotImplementedException();
        }

        public Task<MaterializedViewMetadata> GetMaterializedViewAsync(string keyspace, string name)
        {
            throw new NotImplementedException();
        }

        public Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspace, string typeName)
        {
            throw new NotImplementedException();
        }

        public Task<FunctionMetadata> GetFunctionAsync(string keyspace, string name, string[] signature)
        {
            throw new NotImplementedException();
        }

        public Task<AggregateMetadata> GetAggregateAsync(string keyspace, string name, string[] signature)
        {
            throw new NotImplementedException();
        }

        public Task<QueryTrace> GetQueryTraceAsync(QueryTrace trace)
        {
            throw new NotImplementedException();
        }

        public Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)
        {
            throw new NotImplementedException();
        }

        public bool RemoveKeyspace(string name)
        {
            throw new NotImplementedException();
        }

        public Task<KeyspaceMetadata> RefreshSingleKeyspaceAsync(string name)
        {
            throw new NotImplementedException();
        }

        public void ClearTable(string keyspaceName, string tableName)
        {
            throw new NotImplementedException();
        }

        public void ClearView(string keyspaceName, string name)
        {
            throw new NotImplementedException();
        }

        public void ClearFunction(string keyspaceName, string functionName, string[] signature)
        {
            throw new NotImplementedException();
        }

        public void ClearAggregate(string keyspaceName, string aggregateName, string[] signature)
        {
            throw new NotImplementedException();
        }

        public Task<bool> CheckSchemaAgreementAsync()
        {
            throw new NotImplementedException();
        }

        public Task<bool> WaitForSchemaAgreementAsync(IConnection connection)
        {
            throw new NotImplementedException();
        }

        public void SetCassandraVersion(Version version)
        {
            throw new NotImplementedException();
        }

        public void SetProductTypeAsDbaas()
        {
            throw new NotImplementedException();
        }

        public void SetClusterName(string clusterName)
        {
            throw new NotImplementedException();
        }

        public void SetPartitioner(string partitioner)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IConnectionEndPoint> UpdateResolvedContactPoint(
            IContactPoint contactPoint, IEnumerable<IConnectionEndPoint> endpoints)
        {
            return _resolvedContactPoints.AddOrUpdate(contactPoint, _ => endpoints, (_, __) => endpoints);
        }
    }
}