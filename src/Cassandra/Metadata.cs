//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.MetadataHelpers;
using Cassandra.Requests;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    ///  Keeps metadata on the connected cluster, including known nodes and schema
    ///  definitions.
    /// </summary>
    public class Metadata : IDisposable
    {
        private const string SelectSchemaVersionPeers = "SELECT schema_version FROM system.peers";
        private const string SelectSchemaVersionLocal = "SELECT schema_version FROM system.local";
        private static readonly Logger Logger = new Logger(typeof(ControlConnection));
        private volatile TokenMap _tokenMap;
        private volatile ConcurrentDictionary<string, KeyspaceMetadata> _keyspaces = new ConcurrentDictionary<string, KeyspaceMetadata>();
        private volatile SchemaParser _schemaParser;
        private volatile ConcurrentDictionary<string, long> _keyspacesUpdateCounters = new ConcurrentDictionary<string, long>();
        private long _counterRebuild = 0;

        private readonly TaskFactory _tokenMapTaskFactory = new TaskFactory(
            CancellationToken.None, TaskCreationOptions.DenyChildAttach,
            TaskContinuationOptions.ExecuteSynchronously, new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler);

        public event HostsEventHandler HostsEvent;

        public event SchemaChangedEventHandler SchemaChangedEvent;

        /// <summary>
        ///  Returns the name of currently connected cluster.
        /// </summary>
        /// <returns>the Cassandra name of currently connected cluster.</returns>
        public String ClusterName { get; internal set; }

        /// <summary>
        /// Gets the configuration associated with this instance.
        /// </summary>
        internal Configuration Configuration { get; private set; }

        /// <summary>
        /// Control connection to be used to execute the queries to retrieve the metadata
        /// </summary>
        internal IMetadataQueryProvider ControlConnection { get; set; }

        internal SchemaParser SchemaParser { get { return _schemaParser; } }

        internal string Partitioner { get; set; }

        internal Hosts Hosts { get; private set; }

        internal IReadOnlyTokenMap TokenToReplicasMap => _tokenMap;

        internal Metadata(Configuration configuration)
        {
            Configuration = configuration;
            Hosts = new Hosts();
            Hosts.Down += OnHostDown;
            Hosts.Up += OnHostUp;
        }

        internal Metadata(Configuration configuration, SchemaParser schemaParser) : this(configuration)
        {
            _schemaParser = schemaParser;
        }

        public void Dispose()
        {
            ShutDown();
        }

        public Host GetHost(IPEndPoint address)
        {
            Host host;
            if (Hosts.TryGet(address, out host))
                return host;
            return null;
        }

        internal Host AddHost(IPEndPoint address)
        {
            return Hosts.Add(address);
        }

        internal void RemoveHost(IPEndPoint address)
        {
            Hosts.RemoveIfExists(address);
        }

        internal void FireSchemaChangedEvent(SchemaChangedEventArgs.Kind what, string keyspace, string table, object sender = null)
        {
            if (SchemaChangedEvent != null)
            {
                SchemaChangedEvent(sender ?? this, new SchemaChangedEventArgs { Keyspace = keyspace, What = what, Table = table });
            }
        }

        private void OnHostDown(Host h)
        {
            if (HostsEvent != null)
            {
                HostsEvent(this, new HostsEventArgs { Address = h.Address, What = HostsEventArgs.Kind.Down });
            }
        }

        private void OnHostUp(Host h)
        {
            if (HostsEvent != null)
            {
                HostsEvent(h, new HostsEventArgs { Address = h.Address, What = HostsEventArgs.Kind.Up });
            }
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

        private bool AnotherRebuildCompleted(long currentCounter)
        {
            var lastCounter = Interlocked.Read(ref _counterRebuild);
            if (lastCounter != currentCounter)
            {
                return true;
            }

            return false;
        }

        private bool RebuildNecessary(long currentCounter)
        {
            if (AnotherRebuildCompleted(currentCounter))
            {
                return false;
            }
            
            var newCounter = currentCounter == long.MaxValue ? 0 : currentCounter + 1;
            Interlocked.Exchange(ref _counterRebuild, newCounter);
            return true;
        }

        internal async Task<bool> RebuildTokenMapAsync(bool retry)
        {
            var currentCounter = Interlocked.Read(ref _counterRebuild);

            Metadata.Logger.Info("Retrieving keyspaces metadata");
            // running this statement synchronously inside the exclusive scheduler deadlocks
            var ksList = await _schemaParser.GetKeyspaces(retry).ConfigureAwait(false);

            var task = _tokenMapTaskFactory.StartNew(() =>
            {
                if (!RebuildNecessary(currentCounter))
                {
                    return true;
                }

                Metadata.Logger.Info("Updating keyspaces metadata");
                var ksMap = ksList.Select(ks => new KeyValuePair<string, KeyspaceMetadata>(ks.Name, ks));
                _keyspaces = new ConcurrentDictionary<string, KeyspaceMetadata>(ksMap);
                Metadata.Logger.Info("Rebuilding token map");
                if (Partitioner == null)
                {
                    throw new DriverInternalError("Partitioner can not be null");
                }

                _tokenMap = TokenMap.Build(Partitioner, Hosts.ToCollection(), _keyspaces.Values);
                return true;
            });

            return await task.ConfigureAwait(false);
        }

        internal Task<bool> RemoveKeyspaceFromTokenMap(string name)
        {
            var currentCounter = Interlocked.Read(ref _counterRebuild);
            var currentKeyspaceCounter = _keyspacesUpdateCounters.GetOrAdd(name, s => 0);
            return _tokenMapTaskFactory.StartNew(
                () =>
                {
                    if (AnotherRebuildCompleted(currentCounter))
                    {
                        return true;
                    }
                    
                    var newCounter = currentKeyspaceCounter == long.MaxValue ? 0 : currentKeyspaceCounter + 1;
                    if (!_keyspacesUpdateCounters.TryUpdate(name, newCounter, currentKeyspaceCounter))
                    {
                        return true;
                    }

                    Metadata.Logger.Verbose("Removing keyspace metadata: " + name);
                    if (!_keyspaces.TryRemove(name, out var ks))
                    {
                        //The keyspace didn't exist
                        return false;
                    }
                    _tokenMap?.RemoveKeyspace(ks);
                    return true;
                });
        }

        internal async Task<KeyspaceMetadata> UpdateTokenMapForKeyspace(string name)
        {
            var currentCounter = Interlocked.Read(ref _counterRebuild);
            var currentKeyspaceCounter = _keyspacesUpdateCounters.GetOrAdd(name, s => 0);

            // running this statement synchronously inside the exclusive scheduler deadlocks
            var keyspaceMetadata = await _schemaParser.GetKeyspace(name).ConfigureAwait(false);

            var task = _tokenMapTaskFactory.StartNew(
                () =>
                {
                    if (AnotherRebuildCompleted(currentCounter))
                    {
                        return true;
                    }

                    var newCounter = currentKeyspaceCounter == long.MaxValue ? 0 : currentKeyspaceCounter + 1;
                    if (!_keyspacesUpdateCounters.TryUpdate(name, newCounter, currentKeyspaceCounter))
                    {
                        return true;
                    }

                    if (_tokenMap == null)
                    {
                        return false;
                    }

                    Metadata.Logger.Verbose("Updating keyspace metadata: " + name);
                    if (keyspaceMetadata == null)
                    {
                        return (bool?)null;
                    }

                    _keyspaces.AddOrUpdate(keyspaceMetadata.Name, keyspaceMetadata, (k, v) => keyspaceMetadata);
                    Metadata.Logger.Info("Rebuilding token map for keyspace {0}", keyspaceMetadata.Name);
                    if (Partitioner == null)
                    {
                        throw new DriverInternalError("Partitioner can not be null");
                    }

                    _tokenMap.UpdateKeyspace(keyspaceMetadata);
                    return true;
                });

            var existsTokenMap = await task.ConfigureAwait(false);
            if (existsTokenMap.HasValue && !existsTokenMap.Value)
            {
                await RefreshKeyspaces().ConfigureAwait(false);
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
                return new Host[0];
            }
            return _tokenMap.GetReplicas(keyspaceName, _tokenMap.Factory.Hash(partitionKey));
        }

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
        public KeyspaceMetadata GetKeyspace(string keyspace)
        {
            //Use local cache
            KeyspaceMetadata ksInfo;
            _keyspaces.TryGetValue(keyspace, out ksInfo);
            return ksInfo;
        }

        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// <returns>a collection of all defined keyspaces names.</returns>
        public ICollection<string> GetKeyspaces()
        {
            //Use local cache
            return _keyspaces.Keys;
        }

        /// <summary>
        ///  Returns names of all tables which are defined within specified keyspace.
        /// </summary>
        /// <param name="keyspace">the name of the keyspace for which all tables metadata should be
        ///  returned.</param>
        /// <returns>an ICollection of the metadata for the tables defined in this
        ///  keyspace.</returns>
        public ICollection<string> GetTables(string keyspace)
        {
            KeyspaceMetadata ksMetadata;
            if (!_keyspaces.TryGetValue(keyspace, out ksMetadata))
            {
                return new string[0];
            }
            return ksMetadata.GetTablesNames();
        }

        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is defined.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        public TableMetadata GetTable(string keyspace, string tableName)
        {
            KeyspaceMetadata ksMetadata;
            if (!_keyspaces.TryGetValue(keyspace, out ksMetadata))
            {
                return null;
            }
            return ksMetadata.GetTableMetadata(tableName);
        }

        internal Task<TableMetadata> GetTableAsync(string keyspace, string tableName)
        {
            KeyspaceMetadata ksMetadata;
            if (!_keyspaces.TryGetValue(keyspace, out ksMetadata))
            {
                return TaskHelper.ToTask((TableMetadata)null);
            }
            return ksMetadata.GetTableMetadataAsync(tableName);
        }

        /// <summary>
        ///  Returns the view metadata for the provided view name in the keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified view is defined.</param>
        /// <param name="name">name of view.</param>
        /// <returns>a MaterializedViewMetadata for the view in the specified keyspace.</returns>
        public MaterializedViewMetadata GetMaterializedView(string keyspace, string name)
        {
            KeyspaceMetadata ksMetadata;
            if (!_keyspaces.TryGetValue(keyspace, out ksMetadata))
            {
                return null;
            }
            return ksMetadata.GetMaterializedViewMetadata(name);
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public UdtColumnInfo GetUdtDefinition(string keyspace, string typeName)
        {
            KeyspaceMetadata ksMetadata;
            if (!_keyspaces.TryGetValue(keyspace, out ksMetadata))
            {
                return null;
            }
            return ksMetadata.GetUdtDefinition(typeName);
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspace, string typeName)
        {
            KeyspaceMetadata ksMetadata;
            if (!_keyspaces.TryGetValue(keyspace, out ksMetadata))
            {
                return TaskHelper.ToTask<UdtColumnInfo>(null);
            }
            return ksMetadata.GetUdtDefinitionAsync(typeName);
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Function from Cassandra
        /// </summary>
        /// <returns>The function metadata or null if not found.</returns>
        public FunctionMetadata GetFunction(string keyspace, string name, string[] signature)
        {
            KeyspaceMetadata ksMetadata;
            if (!_keyspaces.TryGetValue(keyspace, out ksMetadata))
            {
                return null;
            }
            return ksMetadata.GetFunction(name, signature);
        }

        /// <summary>
        /// Gets the definition associated with a aggregate from Cassandra
        /// </summary>
        /// <returns>The aggregate metadata or null if not found.</returns>
        public AggregateMetadata GetAggregate(string keyspace, string name, string[] signature)
        {
            KeyspaceMetadata ksMetadata;
            if (!_keyspaces.TryGetValue(keyspace, out ksMetadata))
            {
                return null;
            }
            return ksMetadata.GetAggregate(name, signature);
        }

        /// <summary>
        /// Gets the query trace.
        /// </summary>
        /// <param name="trace">The query trace that contains the id, which properties are going to be populated.</param>
        /// <returns></returns>
        internal Task<QueryTrace> GetQueryTraceAsync(QueryTrace trace)
        {
            return _schemaParser.GetQueryTrace(trace, Configuration.Timer);
        }

        /// <summary>
        /// Updates the keyspace and token information
        /// </summary>
        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            if (table == null)
            {
                //Refresh all the keyspaces and tables information
                return TaskHelper.WaitToComplete(RefreshKeyspaces(true), Configuration.ClientOptions.QueryAbortTimeout);
            }
            var ks = GetKeyspace(keyspace);
            if (ks == null)
            {
                return false;
            }
            ks.ClearTableMetadata(table);
            return true;
        }

        /// <summary>
        /// Retrieves the keyspaces, stores the information in the internal state and rebuilds the token map
        /// </summary>
        internal Task<bool> RefreshKeyspaces(bool retry = false)
        {
            return RebuildTokenMapAsync(retry);
        }

        public void ShutDown(int timeoutMs = Timeout.Infinite)
        {
            //it is really not required to be called, left as it is part of the public API
            //dereference the control connection
            ControlConnection = null;
        }

        internal async Task<bool> RemoveKeyspace(string name)
        {
            var existed = await RemoveKeyspaceFromTokenMap(name).ConfigureAwait(false);
            if (!existed)
            {
                return false;
            }

            FireSchemaChangedEvent(SchemaChangedEventArgs.Kind.Dropped, name, null, this);
            return true;
        }

        internal async Task<KeyspaceMetadata> RefreshSingleKeyspace(bool added, string name)
        {
            var ks = await UpdateTokenMapForKeyspace(name).ConfigureAwait(false);
            if (ks == null)
            {
                return null;
            }
            var eventKind = added ? SchemaChangedEventArgs.Kind.Created : SchemaChangedEventArgs.Kind.Updated;
            FireSchemaChangedEvent(eventKind, name, null, this);
            return ks;
        }

        internal void RefreshTable(string keyspaceName, string tableName)
        {
            KeyspaceMetadata ksMetadata;
            if (_keyspaces.TryGetValue(keyspaceName, out ksMetadata))
            {
                ksMetadata.ClearTableMetadata(tableName);
            }
        }

        internal void RefreshView(string keyspaceName, string name)
        {
            KeyspaceMetadata ksMetadata;
            if (_keyspaces.TryGetValue(keyspaceName, out ksMetadata))
            {
                ksMetadata.ClearViewMetadata(name);
            }
        }

        internal void ClearFunction(string keyspaceName, string functionName, string[] signature)
        {
            KeyspaceMetadata ksMetadata;
            if (_keyspaces.TryGetValue(keyspaceName, out ksMetadata))
            {
                ksMetadata.ClearFunction(functionName, signature);
            }
        }

        internal void ClearAggregate(string keyspaceName, string aggregateName, string[] signature)
        {
            KeyspaceMetadata ksMetadata;
            if (_keyspaces.TryGetValue(keyspaceName, out ksMetadata))
            {
                ksMetadata.ClearAggregate(aggregateName, signature);
            }
        }

        /// <summary>
        /// Waits until that the schema version in all nodes is the same or the waiting time passed.
        /// This method blocks the calling thread.
        /// </summary>
        internal void WaitForSchemaAgreement(Connection connection)
        {
            if (Hosts.Count == 1)
            {
                //If there is just one node, the schema is up to date in all nodes :)
                return;
            }
            var start = DateTime.Now;
            var waitSeconds = Configuration.ProtocolOptions.MaxSchemaAgreementWaitSeconds;
            Logger.Info("Waiting for schema agreement");
            try
            {
                var totalVersions = 0;
                while (DateTime.Now.Subtract(start).TotalSeconds < waitSeconds)
                {
                    var schemaVersionLocalQuery = new QueryRequest(ControlConnection.ProtocolVersion, SelectSchemaVersionLocal, false, QueryProtocolOptions.Default);
                    var schemaVersionPeersQuery = new QueryRequest(ControlConnection.ProtocolVersion, SelectSchemaVersionPeers, false, QueryProtocolOptions.Default);
                    var queries = new[] { connection.Send(schemaVersionLocalQuery), connection.Send(schemaVersionPeersQuery) };
                    // ReSharper disable once CoVariantArrayConversion
                    Task.WaitAll(queries, Configuration.ClientOptions.QueryAbortTimeout);
                    var versions = new HashSet<Guid>
                    {
                        Cassandra.ControlConnection.GetRowSet(queries[0].Result).First().GetValue<Guid>("schema_version")
                    };
                    var peerVersions = Cassandra.ControlConnection.GetRowSet(queries[1].Result).Select(r => r.GetValue<Guid>("schema_version"));
                    foreach (var v in peerVersions)
                    {
                        versions.Add(v);
                    }
                    totalVersions = versions.Count;
                    if (versions.Count == 1)
                    {
                        return;
                    }
                    Thread.Sleep(500);
                }
                Logger.Info(String.Format("Waited for schema agreement, still {0} schema versions in the cluster.", totalVersions));
            }
            catch (Exception ex)
            {
                //Exceptions are not fatal
                Logger.Error("There was an exception while trying to retrieve schema versions", ex);
            }
        }

        /// <summary>
        /// Sets the Cassandra version in order to identify how to parse the metadata information
        /// </summary>
        /// <param name="version"></param>
        internal void SetCassandraVersion(Version version)
        {
            _schemaParser = SchemaParser.GetInstance(version, this, GetUdtDefinitionAsync, _schemaParser);
        }
    }
}