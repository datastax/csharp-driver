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

namespace Cassandra
{
    /// <summary>
    ///  Keeps metadata on the connected cluster, including known nodes and schema
    ///  definitions.
    /// </summary>
    public class Metadata : IDisposable
    {
        private const string SelectKeyspaces = "SELECT * FROM system.schema_keyspaces";
        private const string SelectSingleKeyspace = "SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '{0}'";
        private static readonly Logger Logger = new Logger(typeof(ControlConnection));
        private readonly Hosts _hosts;
        private volatile TokenMap _tokenMap;
        private volatile ConcurrentDictionary<string, KeyspaceMetadata> _keyspaces;
        public event HostsEventHandler HostsEvent;
        public event SchemaChangedEventHandler SchemaChangedEvent;

        /// <summary>
        ///  Returns the name of currently connected cluster.
        /// </summary>
        /// <returns>the Cassandra name of currently connected cluster.</returns>
        public String ClusterName { get; internal set; }

        internal ControlConnection ControlConnection { get; private set; }

        internal Metadata(IReconnectionPolicy rp)
        {
            _hosts = new Hosts(rp);
            _hosts.HostDown += OnHostDown;
        }

        public void Dispose()
        {
            ShutDown();
        }

        internal void SetupControlConnection(ControlConnection controlConnection)
        {
            ControlConnection = controlConnection;
            ControlConnection.Init();
        }


        public Host GetHost(IPAddress address)
        {
            Host host;
            if (_hosts.TryGet(address, out host))
                return host;
            return null;
        }

        internal Host AddHost(IPAddress address)
        {
            _hosts.AddIfNotExistsOrBringUpIfDown(address);
            return GetHost(address);
        }

        internal void RemoveHost(IPAddress address)
        {
            _hosts.RemoveIfExists(address);
        }

        internal void FireSchemaChangedEvent(SchemaChangedEventArgs.Kind what, string keyspace, string table, object sender = null)
        {
            if (SchemaChangedEvent != null)
            {
                SchemaChangedEvent(sender ?? this, new SchemaChangedEventArgs {Keyspace = keyspace, What = what, Table = table});
            }
        }

        internal void SetDownHost(IPAddress address, object sender = null)
        {
            _hosts.SetDownIfExists(address);
        }

        private void OnHostDown(Host h, DateTimeOffset nextUpTime)
        {
            if (HostsEvent != null)
            {
                HostsEvent(this, new HostsEventArgs { IPAddress = h.Address, What = HostsEventArgs.Kind.Down });
            }
        }

        internal void BringUpHost(IPAddress address, object sender = null)
        {
            if (_hosts.AddIfNotExistsOrBringUpIfDown(address))
            {
                if (HostsEvent != null)
                {
                    HostsEvent(sender ?? this, new HostsEventArgs {IPAddress = address, What = HostsEventArgs.Kind.Up});
                }
            }
        }

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        /// <returns>collection of all known hosts of this cluster.</returns>
        public ICollection<Host> AllHosts()
        {
            return _hosts.ToCollection();
        }


        public IEnumerable<IPAddress> AllReplicas()
        {
            return _hosts.AllEndPointsToCollection();
        }

        internal void RebuildTokenMap(string partitioner, Dictionary<IPAddress, HashSet<string>> allTokens)
        {
            _tokenMap = TokenMap.Build(partitioner, allTokens);
        }

        public ICollection<IPAddress> GetReplicas(string keyspace, byte[] partitionKey)
        {
            if (_tokenMap == null)
            {
                return new List<IPAddress>();
            }
            return _tokenMap.GetReplicas(_tokenMap.Factory.Hash(partitionKey));   
        }

        public ICollection<IPAddress> GetReplicas(byte[] partitionKey)
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
            var keyspacesMap = _keyspaces;
            if (keyspacesMap == null)
            {
                return null;
            }
            KeyspaceMetadata ksInfo;
            keyspacesMap.TryGetValue(keyspace, out ksInfo);
            return ksInfo;
        }

        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// <returns>a collection of all defined keyspaces names.</returns>
        public ICollection<string> GetKeyspaces()
        {
            //Use local cache
            var keyspacesMap = _keyspaces;
            if (keyspacesMap == null)
            {
                return new string[0];
            }
            return keyspacesMap.Keys;
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
            var keyspacesMap = _keyspaces;
            if (keyspacesMap == null)
            {
                return new string[0];
            }
            KeyspaceMetadata ksMetadata;
            if (!keyspacesMap.TryGetValue(keyspace, out ksMetadata))
            {
                return new string[0];
            }
            return ksMetadata.GetTablesNames();
        }


        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is definied.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        public TableMetadata GetTable(string keyspace, string tableName)
        {
            var keyspacesMap = _keyspaces;
            if (keyspacesMap == null)
            {
                return null;
            }
            KeyspaceMetadata ksMetadata;
            if (!keyspacesMap.TryGetValue(keyspace, out ksMetadata))
            {
                return null;
            }
            return ksMetadata.GetTableMetadata(tableName);
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public UdtColumnInfo GetUdtDefinition(string keyspace, string typeName)
        {
            return ControlConnection.GetUdtDefinition(keyspace, typeName);
        }

        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            //backward-compatibility purposes, it is part of the public API
            return RefreshKeyspaces();
        }

        /// <summary>
        /// Retrieves the keyspaces and stores the information in the internal state
        /// </summary>
        public bool RefreshKeyspaces()
        {
            Logger.Info("Retrieving keyspaces metadata");
            //Use the control connection to get the keyspace
            var rs = ControlConnection.Query(SelectKeyspaces);
            //parse the info
            var keyspaces = rs.Select(ParseKeyspaceRow).ToDictionary(ks => ks.Name);
            //Assign to local state
            _keyspaces = new ConcurrentDictionary<string, KeyspaceMetadata>(keyspaces);
            //Backward-compatibility: return that it was updated
            return true;
        }

        private KeyspaceMetadata ParseKeyspaceRow(Row row)
        {
            return new KeyspaceMetadata(
                ControlConnection, 
                row.GetValue<string>("keyspace_name"), 
                row.GetValue<bool>("durable_writes"), 
                row.GetValue<string>("strategy_class"), 
                Utils.ConvertStringToMapInt(row.GetValue<string>("strategy_options")));
        }

        public void ShutDown(int timeoutMs = Timeout.Infinite)
        {
            if (ControlConnection != null)
            {
                ControlConnection.Shutdown(timeoutMs);
            }
        }

        internal bool RemoveKeyspace(string name)
        {
            Logger.Verbose("Removing keyspace metadata: " + name);
            var keyspacesMap = _keyspaces;
            if (keyspacesMap == null)
            {
                return false;
            }
            KeyspaceMetadata ks;
            FireSchemaChangedEvent(SchemaChangedEventArgs.Kind.Dropped, name, null, this);
            var removed = keyspacesMap.TryRemove(name, out ks);
            if (removed)
            {
                FireSchemaChangedEvent(SchemaChangedEventArgs.Kind.Dropped, name, null, this);
            }
            return removed;
        }

        internal void RefreshSingleKeyspace(bool added, string name)
        {
            Logger.Verbose("Updating keyspace metadata: " + name);
            var row = ControlConnection.Query(String.Format(SelectSingleKeyspace, name)).FirstOrDefault();
            if (row == null)
            {
                return;
            }
            var keyspacesMap = _keyspaces;
            if (keyspacesMap == null)
            {
                return;
            }
            var ksMetadata = ParseKeyspaceRow(row);
            keyspacesMap.AddOrUpdate(name, ksMetadata, (k, v) => ksMetadata);
            var eventKind = added ? SchemaChangedEventArgs.Kind.Created : SchemaChangedEventArgs.Kind.Updated;
            FireSchemaChangedEvent(eventKind, name, null, this);
        }
    }
}
