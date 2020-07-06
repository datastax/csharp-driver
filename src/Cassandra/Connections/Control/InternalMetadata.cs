//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Collections;
using Cassandra.MetadataHelpers;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;

namespace Cassandra.Connections.Control
{
    internal class InternalMetadata : IInternalMetadata
    {
        private const string SelectSchemaVersionPeers = "SELECT schema_version FROM system.peers";
        private const string SelectSchemaVersionLocal = "SELECT schema_version FROM system.local";

        private static readonly Logger Logger = new Logger(typeof(ControlConnection));

        private volatile TokenMap _tokenMap;

        private volatile ConcurrentDictionary<string, KeyspaceMetadata> _keyspaces =
            new ConcurrentDictionary<string, KeyspaceMetadata>();

        private volatile ISchemaParser _schemaParser;

        private volatile CopyOnWriteDictionary<IContactPoint, IEnumerable<IConnectionEndPoint>> _resolvedContactPoints =
            new CopyOnWriteDictionary<IContactPoint, IEnumerable<IConnectionEndPoint>>();

        private volatile bool _isDbaas = false;
        private volatile string _clusterName;
        private volatile string _partitioner;

        /// <inheritdoc />
        public Configuration Configuration { get; }

        /// <inheritdoc />
        public IControlConnection ControlConnection { get; }

        public ISerializerManager SerializerManager { get; }

        public ISchemaParser SchemaParser => _schemaParser;

        public string Partitioner => _partitioner;

        public Hosts Hosts { get; }

        public IReadOnlyDictionary<IContactPoint, IEnumerable<IConnectionEndPoint>> ResolvedContactPoints
            => _resolvedContactPoints;

        public IReadOnlyTokenMap TokenToReplicasMap => _tokenMap;

        public bool IsDbaas => _isDbaas;

        public string ClusterName => _clusterName;

        public ProtocolVersion ProtocolVersion => SerializerManager.CurrentProtocolVersion;

        public event HostsEventHandler HostsEvent;

        public event SchemaChangedEventHandler SchemaChangedEvent;

        public event Action<Host> HostAdded;

        public event Action<Host> HostRemoved;

        internal InternalMetadata(
            IInternalCluster cluster,
            Configuration configuration,
            IEnumerable<IContactPoint> parsedContactPoints,
            SchemaParser schemaParser = null)
        {
            SerializerManager = configuration.SerializerManager;
            Configuration = configuration;
            Hosts = new Hosts();

            ControlConnection = configuration.ControlConnectionFactory.Create(
                cluster,
                Configuration,
                this,
                parsedContactPoints);

            Hosts.Down += OnHostDown;
            Hosts.Up += OnHostUp;

            _schemaParser = schemaParser;
        }
        
        internal InternalMetadata(
            IControlConnection cc,
            Configuration configuration,
            SchemaParser schemaParser = null)
        {
            SerializerManager = configuration.SerializerManager;
            Configuration = configuration;
            Hosts = new Hosts();

            ControlConnection = cc;

            Hosts.Down += OnHostDown;
            Hosts.Up += OnHostUp;

            _schemaParser = schemaParser;
        }

        public Task InitAsync(CancellationToken token = default(CancellationToken))
        {
            return ControlConnection.InitAsync(token);
        }

        public Task ShutdownAsync()
        {
            ControlConnection.Dispose();
            return Configuration.ProtocolEventDebouncer.ShutdownAsync();
        }

        public void SetResolvedContactPoints(
            IDictionary<IContactPoint, IEnumerable<IConnectionEndPoint>> resolvedContactPoints)
        {
            _resolvedContactPoints =
                new CopyOnWriteDictionary<IContactPoint, IEnumerable<IConnectionEndPoint>>(resolvedContactPoints);
        }

        public Host GetHost(IPEndPoint address)
        {
            return Hosts.TryGet(address, out var host) ? host : null;
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

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        /// <returns>collection of all known hosts of this cluster.</returns>
        public ICollection<Host> AllHosts()
        {
            return Hosts.ToCollection();
        }

        public IEnumerable<IPEndPoint> AllReplicas()
        {
            return Hosts.AllEndPointsToCollection();
        }

        // for tests
        public KeyValuePair<string, KeyspaceMetadata>[] KeyspacesSnapshot => _keyspaces.ToArray();

        public async Task RebuildTokenMapAsync(bool retry, bool fetchKeyspaces)
        {
            IEnumerable<KeyspaceMetadata> ksList = null;
            if (fetchKeyspaces)
            {
                InternalMetadata.Logger.Info("Retrieving keyspaces metadata");
                ksList = await _schemaParser.GetKeyspacesAsync(retry).ConfigureAwait(false);
            }

            ConcurrentDictionary<string, KeyspaceMetadata> keyspaces;
            if (ksList != null)
            {
                InternalMetadata.Logger.Info("Updating keyspaces metadata");
                var ksMap = ksList.Select(ks => new KeyValuePair<string, KeyspaceMetadata>(ks.Name, ks));
                keyspaces = new ConcurrentDictionary<string, KeyspaceMetadata>(ksMap);
            }
            else
            {
                keyspaces = _keyspaces;
            }

            InternalMetadata.Logger.Info("Rebuilding token map");
            if (Partitioner == null)
            {
                throw new DriverInternalError("Partitioner can not be null");
            }

            var tokenMap = TokenMap.Build(Partitioner, Hosts.ToCollection(), keyspaces.Values);
            _keyspaces = keyspaces;
            _tokenMap = tokenMap;
        }

        /// <summary>
        /// this method should be called by the event debouncer
        /// </summary>
        public bool RemoveKeyspaceFromTokenMap(string name)
        {
            InternalMetadata.Logger.Verbose("Removing keyspace metadata: " + name);
            var dropped = _keyspaces.TryRemove(name, out _);
            _tokenMap?.RemoveKeyspace(name);
            return dropped;
        }

        public async Task<KeyspaceMetadata> UpdateTokenMapForKeyspace(string name)
        {
            var keyspaceMetadata = await _schemaParser.GetKeyspaceAsync(name).ConfigureAwait(false);

            var dropped = false;
            var updated = false;
            if (_tokenMap == null)
            {
                await RebuildTokenMapAsync(false, false).ConfigureAwait(false);
            }

            if (keyspaceMetadata == null)
            {
                InternalMetadata.Logger.Verbose("Removing keyspace metadata: " + name);
                dropped = _keyspaces.TryRemove(name, out _);
                _tokenMap?.RemoveKeyspace(name);
            }
            else
            {
                InternalMetadata.Logger.Verbose("Updating keyspace metadata: " + name);
                _keyspaces.AddOrUpdate(keyspaceMetadata.Name, keyspaceMetadata, (k, v) =>
                {
                    updated = true;
                    return keyspaceMetadata;
                });
                InternalMetadata.Logger.Info("Rebuilding token map for keyspace {0}", keyspaceMetadata.Name);
                if (Partitioner == null)
                {
                    throw new DriverInternalError("Partitioner can not be null");
                }

                _tokenMap.UpdateKeyspace(keyspaceMetadata);
            }

            if (Configuration.MetadataSyncOptions.MetadataSyncEnabled)
            {
                if (dropped)
                {
                    FireSchemaChangedEvent(SchemaChangedEventArgs.Kind.Dropped, name, null, this);
                }
                else if (updated)
                {
                    FireSchemaChangedEvent(SchemaChangedEventArgs.Kind.Updated, name, null, this);
                }
                else
                {
                    FireSchemaChangedEvent(SchemaChangedEventArgs.Kind.Created, name, null, this);
                }
            }

            return keyspaceMetadata;
        }

        /// <summary>
        /// Get the replicas for a given partition key and keyspace
        /// </summary>
        public ICollection<Host> GetReplicas(string keyspaceName, byte[] partitionKey)
        {
            if (_tokenMap == null)
            {
                InternalMetadata.Logger.Warning("Metadata.GetReplicas was called but there was no token map.");
                return new Host[0];
            }

            return _tokenMap.GetReplicas(keyspaceName, _tokenMap.Factory.Hash(partitionKey));
        }

        /// <summary>
        /// Get the replicas for a given partition key
        /// </summary>
        public ICollection<Host> GetReplicas(byte[] partitionKey)
        {
            return GetReplicas(null, partitionKey);
        }

        /// <summary>
        ///  Returns metadata of specified keyspace.
        /// </summary>
        /// <param name="keyspace"> the name of the keyspace for which metadata should be
        ///  returned. </param>
        /// <returns>the metadata of the requested keyspace or <c>null</c> if
        ///  <c>* keyspace</c> is not a known keyspace.</returns>
        public Task<KeyspaceMetadata> GetKeyspaceAsync(string keyspace)
        {
            if (Configuration.MetadataSyncOptions.MetadataSyncEnabled)
            {
                //Use local cache
                _keyspaces.TryGetValue(keyspace, out var ksInfo);
                return Task.FromResult(ksInfo);
            }

            return SchemaParser.GetKeyspaceAsync(keyspace);
        }

        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// <returns>a collection of all defined keyspaces names.</returns>
        public Task<ICollection<string>> GetKeyspacesAsync()
        {
            if (Configuration.MetadataSyncOptions.MetadataSyncEnabled)
            {
                //Use local cache
                return Task.FromResult(_keyspaces.Keys);
            }

            return SchemaParser.GetKeyspacesNamesAsync();
        }

        /// <summary>
        ///  Returns names of all tables which are defined within specified keyspace.
        /// </summary>
        /// <param name="keyspace">the name of the keyspace for which all tables metadata should be
        ///  returned.</param>
        /// <returns>an ICollection of the metadata for the tables defined in this
        ///  keyspace.</returns>
        public Task<ICollection<string>> GetTablesAsync(string keyspace)
        {
            if (Configuration.MetadataSyncOptions.MetadataSyncEnabled)
            {
                return !_keyspaces.TryGetValue(keyspace, out var ksMetadata)
                    ? Task.FromResult<ICollection<string>>(new string[0])
                    : ksMetadata.GetTablesNamesAsync();
            }

            return SchemaParser.GetTableNamesAsync(keyspace);
        }

        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is defined.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        public Task<TableMetadata> GetTableAsync(string keyspace, string tableName)
        {
            if (Configuration.MetadataSyncOptions.MetadataSyncEnabled)
            {
                return !_keyspaces.TryGetValue(keyspace, out var ksMetadata)
                    ? Task.FromResult<TableMetadata>(null)
                    : ksMetadata.GetTableMetadataAsync(tableName);
            }

            return SchemaParser.GetTableAsync(keyspace, tableName);
        }

        /// <summary>
        ///  Returns the view metadata for the provided view name in the keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified view is defined.</param>
        /// <param name="name">name of view.</param>
        /// <returns>a MaterializedViewMetadata for the view in the specified keyspace.</returns>
        public Task<MaterializedViewMetadata> GetMaterializedViewAsync(string keyspace, string name)
        {
            if (Configuration.MetadataSyncOptions.MetadataSyncEnabled)
            {
                return !_keyspaces.TryGetValue(keyspace, out var ksMetadata)
                    ? null
                    : ksMetadata.GetMaterializedViewMetadataAsync(name);
            }

            return SchemaParser.GetViewAsync(keyspace, name);
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspace, string typeName)
        {
            if (Configuration.MetadataSyncOptions.MetadataSyncEnabled)
            {
                return !_keyspaces.TryGetValue(keyspace, out var ksMetadata)
                    ? Task.FromResult<UdtColumnInfo>(null)
                    : ksMetadata.GetUdtDefinitionAsync(typeName);
            }

            return SchemaParser.GetUdtDefinitionAsync(keyspace, typeName);
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Function from Cassandra
        /// </summary>
        /// <returns>The function metadata or null if not found.</returns>
        public Task<FunctionMetadata> GetFunctionAsync(string keyspace, string name, string[] signature)
        {
            if (Configuration.MetadataSyncOptions.MetadataSyncEnabled)
            {
                return !_keyspaces.TryGetValue(keyspace, out var ksMetadata)
                    ? Task.FromResult<FunctionMetadata>(null)
                    : ksMetadata.GetFunctionAsync(name, signature);
            }

            var signatureString = SchemaParser.ComputeFunctionSignatureString(signature);
            return SchemaParser.GetFunctionAsync(keyspace, name, signatureString);
        }

        /// <summary>
        /// Gets the definition associated with a aggregate from Cassandra
        /// </summary>
        /// <returns>The aggregate metadata or null if not found.</returns>
        public Task<AggregateMetadata> GetAggregateAsync(string keyspace, string name, string[] signature)
        {
            if (Configuration.MetadataSyncOptions.MetadataSyncEnabled)
            {
                return !_keyspaces.TryGetValue(keyspace, out var ksMetadata)
                    ? Task.FromResult<AggregateMetadata>(null)
                    : ksMetadata.GetAggregateAsync(name, signature);
            }

            var signatureString = SchemaParser.ComputeFunctionSignatureString(signature);
            return SchemaParser.GetAggregateAsync(keyspace, name, signatureString);
        }

        /// <summary>
        /// Gets the query trace.
        /// </summary>
        /// <param name="trace">The query trace that contains the id, which properties are going to be populated.</param>
        /// <returns></returns>
        public Task<QueryTrace> GetQueryTraceAsync(QueryTrace trace)
        {
            return _schemaParser.GetQueryTraceAsync(trace, Configuration.Timer);
        }

        /// <summary>
        /// Updates keyspace metadata (including token metadata for token aware routing) for a given keyspace or a specific keyspace table.
        /// If no keyspace is provided then this method will update the metadata and token map for all the keyspaces of the cluster.
        /// </summary>
        public async Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)
        {
            if (keyspace == null)
            {
                await ControlConnection.ScheduleAllKeyspacesRefreshAsync(true).ConfigureAwait(false);
                return true;
            }

            await ControlConnection.ScheduleKeyspaceRefreshAsync(keyspace, true).ConfigureAwait(false);
            _keyspaces.TryGetValue(keyspace, out var ks);
            if (ks == null)
            {
                return false;
            }

            if (table != null)
            {
                ks.ClearTableMetadata(table);
            }
            return true;
        }

        /// <summary>
        /// this method should be called by the event debouncer
        /// </summary>
        public bool RemoveKeyspace(string name)
        {
            var existed = RemoveKeyspaceFromTokenMap(name);
            if (!existed)
            {
                return false;
            }

            FireSchemaChangedEvent(SchemaChangedEventArgs.Kind.Dropped, name, null, this);
            return true;
        }

        /// <summary>
        /// this method should be called by the event debouncer
        /// </summary>
        public Task<KeyspaceMetadata> RefreshSingleKeyspaceAsync(string name)
        {
            return UpdateTokenMapForKeyspace(name);
        }

        public void ClearTable(string keyspaceName, string tableName)
        {
            if (_keyspaces.TryGetValue(keyspaceName, out var ksMetadata))
            {
                ksMetadata.ClearTableMetadata(tableName);
            }
        }

        public void ClearView(string keyspaceName, string name)
        {
            if (_keyspaces.TryGetValue(keyspaceName, out var ksMetadata))
            {
                ksMetadata.ClearViewMetadata(name);
            }
        }

        public void ClearFunction(string keyspaceName, string functionName, string[] signature)
        {
            if (_keyspaces.TryGetValue(keyspaceName, out var ksMetadata))
            {
                ksMetadata.ClearFunction(functionName, signature);
            }
        }

        public void ClearAggregate(string keyspaceName, string aggregateName, string[] signature)
        {
            if (_keyspaces.TryGetValue(keyspaceName, out var ksMetadata))
            {
                ksMetadata.ClearAggregate(aggregateName, signature);
            }
        }

        /// <summary>
        /// Initiates a schema agreement check.
        /// <para/>
        /// Schema changes need to be propagated to all nodes in the cluster.
        /// Once they have settled on a common version, we say that they are in agreement.
        /// <para/>
        /// This method does not perform retries so
        /// <see cref="ProtocolOptions.MaxSchemaAgreementWaitSeconds"/> does not apply.
        /// </summary>
        /// <returns>True if schema agreement was successful and false if it was not successful.</returns>
        public async Task<bool> CheckSchemaAgreementAsync()
        {
            if (Hosts.Count == 1)
            {
                // If there is just one node, the schema is up to date in all nodes :)
                return true;
            }

            try
            {
                var queries = new[]
                {
                    ControlConnection.QueryAsync(SelectSchemaVersionLocal),
                    ControlConnection.QueryAsync(SelectSchemaVersionPeers)
                };

                await Task.WhenAll(queries).ConfigureAwait(false);

                return CheckSchemaVersionResults(queries[0].Result, queries[1].Result);
            }
            catch (Exception ex)
            {
                Logger.Error("Error while checking schema agreement.", ex);
            }

            return false;
        }

        /// <summary>
        /// Checks if there is only one schema version between the provided query results.
        /// </summary>
        /// <param name="localVersionQuery">
        /// Results obtained from a query to <code>system.local</code> table.
        /// Must contain the <code>schema_version</code> column.
        /// </param>
        /// <param name="peerVersionsQuery">
        /// Results obtained from a query to <code>system.peers</code> table.
        /// Must contain the <code>schema_version</code> column.
        /// </param>
        /// <returns><code>True</code> if there is a schema agreement (only 1 schema version). <code>False</code> otherwise.</returns>
        private static bool CheckSchemaVersionResults(
            IEnumerable<IRow> localVersionQuery, IEnumerable<IRow> peerVersionsQuery)
        {
            return new HashSet<Guid>(
               peerVersionsQuery
                   .Concat(localVersionQuery)
                   .Select(r => r.GetValue<Guid>("schema_version"))).Count == 1;
        }

        /// <summary>
        /// Waits until that the schema version in all nodes is the same or the waiting time passed.
        /// This method blocks the calling thread.
        /// </summary>
        public async Task<bool> WaitForSchemaAgreementAsync(IConnection connection)
        {
            if (Hosts.Count == 1)
            {
                //If there is just one node, the schema is up to date in all nodes :)
                return true;
            }
            var start = DateTime.Now;
            var waitSeconds = Configuration.ProtocolOptions.MaxSchemaAgreementWaitSeconds;
            InternalMetadata.Logger.Info("Waiting for schema agreement");
            try
            {
                var totalVersions = 0;
                while (DateTime.Now.Subtract(start).TotalSeconds < waitSeconds)
                {
                    var serializer = SerializerManager.GetCurrentSerializer();
                    var schemaVersionLocalQuery =
                        new QueryRequest(
                            serializer,
                            InternalMetadata.SelectSchemaVersionLocal,
                            QueryProtocolOptions.Default,
                            false,
                            null);
                    var schemaVersionPeersQuery =
                        new QueryRequest(
                            serializer,
                            InternalMetadata.SelectSchemaVersionPeers,
                            QueryProtocolOptions.Default,
                            false,
                            null);
                    var queries = new[] { connection.Send(schemaVersionLocalQuery), connection.Send(schemaVersionPeersQuery) };
                    // ReSharper disable once CoVariantArrayConversion
                    await Task.WhenAll(queries).ConfigureAwait(false);

                    if (InternalMetadata.CheckSchemaVersionResults(
                        Configuration.MetadataRequestHandler.GetRowSet(await queries[0].ConfigureAwait(false)),
                        Configuration.MetadataRequestHandler.GetRowSet(await queries[1].ConfigureAwait(false))))
                    {
                        return true;
                    }

                    await Task.Delay(500).ConfigureAwait(false);
                }
                InternalMetadata.Logger.Info($"Waited for schema agreement, still {totalVersions} schema versions in the cluster.");
            }
            catch (Exception ex)
            {
                //Exceptions are not fatal
                InternalMetadata.Logger.Error("There was an exception while trying to retrieve schema versions", ex);
            }

            return false;
        }

        /// <summary>
        /// Sets the Cassandra version in order to identify how to parse the metadata information
        /// </summary>
        /// <param name="version"></param>
        public void SetCassandraVersion(Version version)
        {
            _schemaParser = Configuration.SchemaParserFactory.Create(version, this, GetUdtDefinitionAsync, _schemaParser);
        }

        public void SetProductTypeAsDbaas()
        {
            _isDbaas = true;
        }

        public void SetClusterName(string clusterName)
        {
            _clusterName = clusterName;
        }

        public void SetPartitioner(string partitioner)
        {
            _partitioner = partitioner;
        }

        public IEnumerable<IConnectionEndPoint> UpdateResolvedContactPoint(
            IContactPoint contactPoint, IEnumerable<IConnectionEndPoint> endpoints)
        {
            return _resolvedContactPoints.AddOrUpdate(contactPoint, _ => endpoints, (_, __) => endpoints);
        }
    }
}