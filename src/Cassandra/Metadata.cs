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
        private volatile TokenMap _tokenMap;
        private volatile ConcurrentDictionary<string, KeyspaceMetadata> _keyspaces = new ConcurrentDictionary<string,KeyspaceMetadata>(1, 0);
        public event HostsEventHandler HostsEvent;
        public event SchemaChangedEventHandler SchemaChangedEvent;

        /// <summary>
        ///  Returns the name of currently connected cluster.
        /// </summary>
        /// <returns>the Cassandra name of currently connected cluster.</returns>
        public String ClusterName { get; internal set; }

        /// <summary>
        /// Control connection to be used to execute the queries to retrieve the metadata
        /// </summary>
        internal ControlConnection ControlConnection { get; set; }

        internal string Partitioner { get; set; }

        internal Hosts Hosts { get; private set; }

        internal Metadata(IReconnectionPolicy rp)
        {
            Hosts = new Hosts(rp);
            Hosts.HostDown += OnHostDown;
        }

        public void Dispose()
        {
            ShutDown();
        }

        public Host GetHost(IPAddress address)
        {
            Host host;
            if (Hosts.TryGet(address, out host))
                return host;
            return null;
        }

        internal Host AddHost(IPAddress address)
        {
            Hosts.AddIfNotExistsOrBringUpIfDown(address);
            return GetHost(address);
        }

        internal void RemoveHost(IPAddress address)
        {
            Hosts.RemoveIfExists(address);
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
            Hosts.SetDownIfExists(address);
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
            if (Hosts.AddIfNotExistsOrBringUpIfDown(address))
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
            return Hosts.ToCollection();
        }


        public IEnumerable<IPAddress> AllReplicas()
        {
            return Hosts.AllEndPointsToCollection();
        }

        internal void RebuildTokenMap()
        {
            Logger.Info("Rebuilding token map");
            if (Partitioner == null)
            {
                throw new DriverInternalError("Partitioner can not be null");
            }
            _tokenMap = TokenMap.Build(Partitioner, Hosts.ToCollection(), _keyspaces.Values);
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

        public ICollection<IPAddress> GetReplicas(byte[] partitionKey)
        {
            return GetReplicas(null, partitionKey).Select(h => h.Address).ToList();
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
        /// <param name="keyspace">name of the keyspace within specified table is definied.</param>
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
        /// Updates the keyspace and token information
        /// </summary>
        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            if (table == null)
            {
                //Refresh all the keyspaces and tables information
                RefreshKeyspaces(true);
                return true;
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
        internal void RefreshKeyspaces(bool retry)
        {
            Logger.Info("Retrieving keyspaces metadata");
            //Use the control connection to get the keyspace
            var rs = ControlConnection.Query(SelectKeyspaces, retry);
            //parse the info
            var keyspaces = rs.Select(ParseKeyspaceRow).ToDictionary(ks => ks.Name);
            //Assign to local state
            _keyspaces = new ConcurrentDictionary<string, KeyspaceMetadata>(keyspaces);
            RebuildTokenMap();
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
            //it is really not required to be called, left as it is part of the public API
            //unreference the control connection
            ControlConnection = null;
        }

        internal bool RemoveKeyspace(string name)
        {
            Logger.Verbose("Removing keyspace metadata: " + name);
            KeyspaceMetadata ks;
            FireSchemaChangedEvent(SchemaChangedEventArgs.Kind.Dropped, name, null, this);
            var removed = _keyspaces.TryRemove(name, out ks);
            if (removed)
            {
                RebuildTokenMap();
                FireSchemaChangedEvent(SchemaChangedEventArgs.Kind.Dropped, name, null, this);
            }
            return removed;
        }

        internal void RefreshSingleKeyspace(bool added, string name)
        {
            Logger.Verbose("Updating keyspace metadata: " + name);
            var row = ControlConnection.Query(String.Format(SelectSingleKeyspace, name), true).FirstOrDefault();
            if (row == null)
            {
                return;
            }
            var ksMetadata = ParseKeyspaceRow(row);
            _keyspaces.AddOrUpdate(name, ksMetadata, (k, v) => ksMetadata);
            var eventKind = added ? SchemaChangedEventArgs.Kind.Created : SchemaChangedEventArgs.Kind.Updated;
            RebuildTokenMap();
            FireSchemaChangedEvent(eventKind, name, null, this);
        }

        internal void RefreshTable(string keyspaceName, string tableName)
        {
            KeyspaceMetadata ksMetadata;
            if (_keyspaces.TryGetValue(keyspaceName, out ksMetadata))
            {
                ksMetadata.ClearTableMetadata(tableName);
            }
        }
    }
}
