using System;
using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    public class HostsEventArgs : EventArgs
    {
        public enum Kind { Up, Down }
        public Kind What;
        public IPAddress IPAddress;
    }

    public delegate void HostsEventHandler(object sender, HostsEventArgs e);

    /// <summary>
    ///  Keeps metadata on the connected cluster, including known nodes and schema
    ///  definitions.
    /// </summary>
    public class Metadata
    {
        internal string ClusterName;
        private readonly Hosts _hosts;
        private readonly ControlConnection _controlConnection;

        internal Metadata(Hosts hosts, ControlConnection controlConnection)
        {
            this._hosts = hosts;
            this._controlConnection = controlConnection;
        }

        public event HostsEventHandler HostsEvent;
        
        /// <summary>
        ///  Returns the name of currently connected cluster.
        /// </summary>
        /// 
        /// <returns>the Cassandra name of currently connected cluster.</returns>
        public String GetClusterName()
        {
            return ClusterName;
        }


        public Host GetHost(IPAddress address)
        {
            return _hosts[address];
        }

        public Host AddHost(IPAddress address)
        {
            _hosts.AddIfNotExistsOrBringUpIfDown(address);
            return _hosts[address];
        }

        public void RemoveHost(IPAddress address)
        {
            _hosts.RemoveIfExists(address);
        }

        public void SetDownHost(IPAddress address, object sender = null)
        {
            if (_hosts.SetDownIfExists(address))
                if (HostsEvent != null)
                    HostsEvent(sender ?? this, new HostsEventArgs() { IPAddress = address, What = HostsEventArgs.Kind.Down });
        }

        public void BringUpHost(IPAddress address, object sender = null)
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
            return _hosts.All();
        }


        public IEnumerable<IPAddress> AllReplicas()
        {
            return _hosts.AllEndPoints();
        }

        internal void RebuildTokenMap(string partitioner, Dictionary<IPAddress, DictSet<string>> allTokens)
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
    }

    internal class TokenMap
    {

        private readonly Dictionary<IToken, DictSet<IPAddress>> _tokenToCassandraClusterHosts;
        private readonly List<IToken> _ring;
        internal readonly TokenFactory Factory;

        private TokenMap(TokenFactory factory, Dictionary<IToken, DictSet<IPAddress>> tokenToCassandraClusterHosts, List<IToken> ring)
        {
            this.Factory = factory;
            this._tokenToCassandraClusterHosts = tokenToCassandraClusterHosts;
            this._ring = ring;
        }

        public static TokenMap Build(String partitioner, Dictionary<IPAddress, DictSet<string>> allTokens)
        {

            TokenFactory factory = TokenFactory.GetFactory(partitioner);
            if (factory == null)
                return null;

            Dictionary<IToken, DictSet<IPAddress>> tokenToCassandraClusterHosts = new Dictionary<IToken, DictSet<IPAddress>>();
            DictSet<IToken> allSorted = new DictSet<IToken>();

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
                            tokenToCassandraClusterHosts.Add(t, new DictSet<IPAddress>());
                        tokenToCassandraClusterHosts[t].Add(cassandraClusterHost);
                    }
                    catch (ArgumentException e)
                    {
                        // If we failed parsing that token, skip it
                    }
                }
            }
            return new TokenMap(factory, tokenToCassandraClusterHosts, new List<IToken>(allSorted));
        }

        public DictSet<IPAddress> GetReplicas(IToken token)
        {

            // Find the primary replica
            int i = Array.BinarySearch(_ring.ToArray(),token);
            if (i < 0)
            {
                i = (i + 1) * (-1);
                if (i >= _ring.Count)
                    i = 0;
            }

            return _tokenToCassandraClusterHosts[_ring[i]];
        }
    }
}