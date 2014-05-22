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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading;

namespace Cassandra
{
    /// <summary>
    /// Implementation of <see cref="ICluster"/>
    /// </summary>
    /// <inheritdoc />
    public class Cluster : ICluster
    {
        /// <summary>
        ///  Build a new cluster based on the provided initializer. <p> Note that for
        ///  building a cluster programmatically, Cluster.NewBuilder provides a slightly less
        ///  verbose shortcut with <link>NewBuilder#Build</link>. </p><p> Also note that that all
        ///  the contact points provided by <c>initializer</c> must share the same
        ///  port.</p>
        /// </summary>
        /// <param name="initializer">the Cluster.Initializer to use</param>
        /// <returns>the newly created Cluster instance </returns>
        public static Cluster BuildFrom(IInitializer initializer)
        {
            ICollection<IPAddress> contactPoints = initializer.ContactPoints;
            if (contactPoints.Count == 0)
                throw new ArgumentException("Cannot build a cluster without contact points");

            return new Cluster(contactPoints, initializer.GetConfiguration());
        }

        /// <summary>
        ///  Creates a new <link>Cluster.NewBuilder</link> instance. <p> This is a shortcut
        ///  for <c>new Cluster.NewBuilder()</c></p>.
        /// </summary>
        /// <returns>the new cluster builder.</returns>
        public static Builder Builder()
        {
            return new Builder();
        }

        private readonly int _binaryProtocolVersion;
        private readonly Configuration _configuration;
        private readonly ConcurrentDictionary<Guid, Session> _connectedSessions = new ConcurrentDictionary<Guid, Session>();
        private readonly IEnumerable<IPAddress> _contactPoints;
        private readonly Logger _logger = new Logger(typeof (Cluster));
        private readonly Metadata _metadata;

        /// <summary>
        ///  Gets the cluster configuration.
        /// </summary>
        public Configuration Configuration
        {
            get { return _configuration; }
        }

        /// <summary>
        ///  Gets read-only metadata on the connected cluster. <p> This includes the
        ///  know nodes (with their status as seen by the driver) as well as the schema
        ///  definitions.</p>
        /// </summary>
        public Metadata Metadata
        {
            get { return _metadata; }
        }

        private Cluster(IEnumerable<IPAddress> contactPoints, Configuration configuration)
        {
            _contactPoints = contactPoints;
            _configuration = configuration;
            _metadata = new Metadata(configuration.Policies.ReconnectionPolicy);

            var controlpolicies = new Policies(
                //new ControlConnectionLoadBalancingPolicy(_configuration.Policies.LoadBalancingPolicy),
                _configuration.Policies.LoadBalancingPolicy,
                new ExponentialReconnectionPolicy(2*1000, 5*60*1000),
                Policies.DefaultRetryPolicy);

            foreach (IPAddress ep in _contactPoints)
                Metadata.AddHost(ep);

            PoolingOptions poolingOptions = new PoolingOptions()
                .SetCoreConnectionsPerHost(HostDistance.Local, 0)
                .SetMaxConnectionsPerHost(HostDistance.Local, 1)
                .SetMinSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 0)
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 127)
                ;

            var controlConnection = new ControlConnection(this, new List<IPAddress>(), controlpolicies,
                                                          new ProtocolOptions(_configuration.ProtocolOptions.Port,
                                                                              configuration.ProtocolOptions.SslOptions),
                                                          poolingOptions, _configuration.SocketOptions,
                                                          new ClientOptions(
                                                              true,
                                                              _configuration.ClientOptions.QueryAbortTimeout, null),
                                                          _configuration.AuthProvider,
                                                          _configuration.AuthInfoProvider,
                                                          2 //lets start from protocol version 2
                );

            _metadata.SetupControllConnection(controlConnection);
            _binaryProtocolVersion = controlConnection.BinaryProtocolVersion;
            _logger.Info("Binary protocol version: [" + _binaryProtocolVersion + "]");
        }

        /// <inheritdoc />
        public ICollection<Host> AllHosts()
        {
            return Metadata.AllHosts();
        }

        /// <inheritdoc />
        public ISession Connect()
        {
            return Connect(_configuration.ClientOptions.DefaultKeyspace);
        }

        /// <inheritdoc />
        public ISession Connect(string keyspace)
        {
            var scs = new Session(this, _configuration.Policies,
                                  _configuration.ProtocolOptions,
                                  _configuration.PoolingOptions,
                                  _configuration.SocketOptions,
                                  _configuration.ClientOptions,
                                  _configuration.AuthProvider,
                                  _configuration.AuthInfoProvider,
                                  keyspace,
                                  _binaryProtocolVersion);
            scs.Init();
            _connectedSessions.TryAdd(scs.Guid, scs);
            _logger.Info("Session connected!");

            return scs;
        }

        /// <summary>
        /// Creates new session on this cluster, and sets it to default keyspace. 
        /// If default keyspace does not exist then it will be created and session will be set to it.
        /// Name of default keyspace can be specified during creation of cluster object with <c>Cluster.Builder().WithDefaultKeyspace("keyspace_name")</c> method.
        /// </summary>
        /// <param name="replication">Replication property for this keyspace. To set it, refer to the <see cref="ReplicationStrategies"/> class methods. 
        /// It is a dictionary of replication property sub-options where key is a sub-option name and value is a value for that sub-option. 
        /// <p>Default value is <c>SimpleStrategy</c> with <c>'replication_factor' = 2</c></p></param>
        /// <param name="durableWrites">Whether to use the commit log for updates on this keyspace. Default is set to <c>true</c>.</param>
        /// <returns>a new session on this cluster set to default keyspace.</returns>
        public ISession ConnectAndCreateDefaultKeyspaceIfNotExists(Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            var session = Connect("");
            session.CreateKeyspaceIfNotExists(_configuration.ClientOptions.DefaultKeyspace, replication, durableWrites);
            session.ChangeKeyspace(_configuration.ClientOptions.DefaultKeyspace);
            return session;
        }

        public void Dispose()
        {
            Shutdown();
        }

        /// <inheritdoc />
        public Host GetHost(IPAddress address)
        {
            return Metadata.GetHost(address);
        }

        /// <inheritdoc />
        public ICollection<IPAddress> GetReplicas(byte[] partitionKey)
        {
            return Metadata.GetReplicas(partitionKey);
        }

        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            return _metadata.RefreshSchema(keyspace, table);
        }

        /// <inheritdoc />
        public void Shutdown(int timeoutMs = Timeout.Infinite)
        {
            foreach (KeyValuePair<Guid, Session> kv in _connectedSessions)
            {
                Session ses;
                if (_connectedSessions.TryRemove(kv.Key, out ses))
                {
                    ses.WaitForAllPendingActions(timeoutMs);
                    ses.InternalDispose();
                }
            }
            _metadata.ShutDown(timeoutMs);

            _logger.Info("Cluster [" + _metadata.ClusterName + "] has been shut down.");
        }

        internal void SessionDisposed(Session s)
        {
            Session ses;
            _connectedSessions.TryRemove(s.Guid, out ses);
        }

        ~Cluster()
        {
            Shutdown();
        }
    }
}