//
//      Copyright (C) 2012 DataStax Inc.
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

namespace Cassandra
{
    public class HostsEventArgs : EventArgs
    {
        public enum Kind { Up, Down }
        public Kind What;
        public IPAddress IPAddress;
    }

    public class SchemaChangedEventArgs : EventArgs
    {
        public enum Kind { Created, Dropped, Updated }
        public Kind What;
        public string Keyspace;
        public string Table;
    }

    public delegate void HostsEventHandler(object sender, HostsEventArgs e);
    public delegate void SchemaChangedEventHandler(object sender, SchemaChangedEventArgs e);

    /// <summary>
    ///  Keeps metadata on the connected cluster, including known nodes and schema
    ///  definitions.
    /// </summary>
    public class Metadata : IDisposable
    {
        private readonly Hosts _hosts;
        private ControlConnection _controlConnection = null;

        internal Metadata(IReconnectionPolicy rp) 
        {
            this._hosts = new Hosts(rp);
        }

        internal void SetupControllConnection(ControlConnection controlConnection)
        {
            this._controlConnection = controlConnection;
            _controlConnection.Init();
        }

        public event HostsEventHandler HostsEvent;
        public event SchemaChangedEventHandler SchemaChangedEvent;
        
        /// <summary>
        ///  Returns the name of currently connected cluster.
        /// </summary>
        /// 
        /// <returns>the Cassandra name of currently connected cluster.</returns>
        public String ClusterName { get; internal set; }


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
                SchemaChangedEvent(sender ?? this, new SchemaChangedEventArgs() { Keyspace = keyspace, What = what, Table = table });
        }

        internal void SetDownHost(IPAddress address, object sender = null)
        {
            if (_hosts.SetDownIfExists(address))
                if (HostsEvent != null)
                    HostsEvent(sender ?? this, new HostsEventArgs() { IPAddress = address, What = HostsEventArgs.Kind.Down });
        }

        internal void BringUpHost(IPAddress address, object sender = null)
        {
            if (_hosts.AddIfNotExistsOrBringUpIfDown(address))
                if (HostsEvent != null)
                    HostsEvent(sender ?? this, new HostsEventArgs() { IPAddress = address, What = HostsEventArgs.Kind.Up });
        }

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        /// 
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
            this._tokenMap = TokenMap.Build(partitioner, allTokens);
        }

        private volatile TokenMap _tokenMap;

        public ICollection<IPAddress> GetReplicas(byte[] partitionKey)
        {
            if(_tokenMap==null)
            {
                return new List<IPAddress>();
            }
            else
            {
                return _tokenMap.GetReplicas(_tokenMap.Factory.Hash(partitionKey));
            }
        }


        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// 
        /// <returns>a collection of all defined keyspaces names.</returns>
        public ICollection<string> GetKeyspaces()
        {
            return _controlConnection.GetKeyspaces();
        }


        /// <summary>
        ///  Returns metadata of specified keyspace.
        /// </summary>
        /// <param name="keyspace"> the name of the keyspace for which metadata should be
        ///  returned. </param>
        /// 
        /// <returns>the metadat of the requested keyspace or <code>null</code> if
        ///  <code>* keyspace</code> is not a known keyspace.</returns>
        public KeyspaceMetadata GetKeyspace(string keyspace)
        {
            return _controlConnection.GetKeyspace(keyspace);
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
            return _controlConnection.GetTables(keyspace);
        }


        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is definied.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        public TableMetadata GetTable(string keyspace, string tableName)
        {
            return _controlConnection.GetTable(keyspace, tableName);
        }

        public void Dispose()
        {
            ShutDown();
        }

        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            _controlConnection.SubmitSchemaRefresh(keyspace, table);
            if (keyspace == null && table == null)
                return _controlConnection.RefreshHosts();
            return true;
        }

        public void ShutDown(int timeoutMs = Timeout.Infinite)
        {
            if (_controlConnection != null)
                _controlConnection.Shutdown(timeoutMs);
        }
    }

    internal class TokenMap
    {

        private readonly Dictionary<IToken, HashSet<IPAddress>> _tokenToCassandraClusterHosts;
        private readonly IToken[] _ring;
        internal readonly TokenFactory Factory;

        private TokenMap(TokenFactory factory, Dictionary<IToken, HashSet<IPAddress>> tokenToCassandraClusterHosts, List<IToken> ring)
        {
            this.Factory = factory;
            this._tokenToCassandraClusterHosts = tokenToCassandraClusterHosts;
            this._ring = ring.ToArray();
            Array.Sort(this._ring);
        }

        public static TokenMap Build(String partitioner, Dictionary<IPAddress, HashSet<string>> allTokens)
        {

            TokenFactory factory = TokenFactory.GetFactory(partitioner);
            if (factory == null)
                return null;

            Dictionary<IToken, HashSet<IPAddress>> tokenToCassandraClusterHosts = new Dictionary<IToken, HashSet<IPAddress>>();
            HashSet<IToken> allSorted = new HashSet<IToken>();

            foreach (var entry in allTokens)
            {
                var cassandraClusterHost = entry.Key;
                foreach (string tokenStr in entry.Value)
                {
                    try
                    {
                        IToken t = factory.Parse(tokenStr);
                        allSorted.Add(t);
                        if (!tokenToCassandraClusterHosts.ContainsKey(t))
                            tokenToCassandraClusterHosts.Add(t, new HashSet<IPAddress>());
                        tokenToCassandraClusterHosts[t].Add(cassandraClusterHost);
                    }
                    catch (ArgumentException)
                    {
                        // If we failed parsing that token, skip it
                    }
                }
            }
            return new TokenMap(factory, tokenToCassandraClusterHosts, new List<IToken>(allSorted));
        }

        public HashSet<IPAddress> GetReplicas(IToken token)
        {

            // Find the primary replica
            int i = Array.BinarySearch(_ring,token);
            if (i < 0)
            {
                i = (i + 1) * (-1);
                if (i >= _ring.Length)
                    i = 0;
            }

            return _tokenToCassandraClusterHosts[_ring[i]];
        }
    }
}