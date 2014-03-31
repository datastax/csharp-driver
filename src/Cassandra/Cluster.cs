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
using System.ComponentModel;
using System.Net;
using System.Threading;
using System.Diagnostics;
using System.Collections.Concurrent;

namespace Cassandra
{
    /// <summary>
    ///  Informations and known state of a Cassandra cluster. <p> This is the main
    ///  entry point of the driver. A simple example of access to a Cassandra cluster
    ///  would be: 
    /// <pre> Cluster cluster = Cluster.Builder.AddContactPoint("192.168.0.1").Build(); 
    ///  Session session = Cluster.Connect("db1"); 
    ///  foreach (var row in session.execute("SELECT * FROM table1")) 
    ///    //do something ... </pre> 
    ///  </p><p> A cluster object maintains a
    ///  permanent connection to one of the cluster node that it uses solely to
    ///  maintain informations on the state and current topology of the cluster. Using
    ///  the connection, the driver will discover all the nodes composing the cluster
    ///  as well as new nodes joining the cluster.</p>
    /// </summary>
    public class Cluster : IDisposable
    {
        private readonly Logger _logger = new Logger(typeof(Cluster));
        private readonly IEnumerable<IPAddress> _contactPoints;
        private readonly Configuration _configuration;
        private int _binaryProtocolVersion;

        private Cluster(IEnumerable<IPAddress> contactPoints, Configuration configuration)
        {
            this._contactPoints = contactPoints;
            this._configuration = configuration;
            this._metadata = new Metadata(configuration.Policies.ReconnectionPolicy);

            var controlpolicies = new Cassandra.Policies(
                //new ControlConnectionLoadBalancingPolicy(_configuration.Policies.LoadBalancingPolicy),
                _configuration.Policies.LoadBalancingPolicy,
                new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000),
                Cassandra.Policies.DefaultRetryPolicy);

            foreach (var ep in _contactPoints)
                Metadata.AddHost(ep);

            var poolingOptions = new PoolingOptions()
                .SetCoreConnectionsPerHost(HostDistance.Local, 0)
                .SetMaxConnectionsPerHost(HostDistance.Local, 1)
                .SetMinSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 0)
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 127)
                ;

            var controlConnection = new ControlConnection(this, new List<IPAddress>(), controlpolicies,
                                                        new ProtocolOptions(_configuration.ProtocolOptions.Port, configuration.ProtocolOptions.SslOptions),
                                                        poolingOptions, _configuration.SocketOptions,
                                                        new ClientOptions(
                                                            true,
                                                            _configuration.ClientOptions.QueryAbortTimeout, null),
                                                        _configuration.AuthProvider,
                                                        _configuration.AuthInfoProvider,
                                                        2//lets start from protocol version 2
                                                        );

            _metadata.SetupControllConnection(controlConnection);
            _binaryProtocolVersion = controlConnection.BinaryProtocolVersion;
            _logger.Info("Binary protocol version: [" + _binaryProtocolVersion.ToString()+"]");
        }

        /// <summary>
        ///  Build a new cluster based on the provided initializer. <p> Note that for
        ///  building a cluster programmatically, Cluster.NewBuilder provides a slightly less
        ///  verbose shortcut with <link>NewBuilder#Build</link>. </p><p> Also note that that all
        ///  the contact points provided by <code>* initializer</code> must share the same
        ///  port.</p>
        /// </summary>
        /// <param name="initializer"> the Cluster.Initializer to use </param>
        /// 
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
        ///  for <code>new Cluster.NewBuilder()</code></p>.
        /// </summary>
        /// 
        /// <returns>the new cluster builder.</returns>
        public static Builder Builder()
        {
            return new Builder();
        }

        /// <summary>
        ///  Creates a new session on this cluster.
        /// </summary>
        /// 
        /// <returns>a new session on this cluster set to no keyspace.</returns>
        public Session Connect()
        {
            return Connect(_configuration.ClientOptions.DefaultKeyspace);
        }

        /// <summary>
        ///  Creates a new session on this cluster and sets a keyspace to use.
        /// </summary>
        /// <param name="keyspace"> The name of the keyspace to use for the created <code>Session</code>. </param>
        /// <returns>a new session on this cluster set to keyspace: 
        ///  <code>keyspaceName</code>. </returns>
        public Session Connect(string keyspace)
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
        /// Name of default keyspace can be specified during creation of cluster object with <code>Cluster.Builder().WithDefaultKeyspace("keyspace_name")</code> method.
        /// </summary>
        /// <param name="replication">Replication property for this keyspace. To set it, refer to the <see cref="ReplicationStrategies"/> class methods. 
        /// It is a dictionary of replication property sub-options where key is a sub-option name and value is a value for that sub-option. 
        /// <p>Default value is <code>'SimpleStrategy'</code> with <code>'replication_factor' = 2</code></p></param>
        /// <param name="durable_writes">Whether to use the commit log for updates on this keyspace. Default is set to <code>true</code>.</param>
        /// <returns>a new session on this cluster set to default keyspace.</returns>
        public Session ConnectAndCreateDefaultKeyspaceIfNotExists(Dictionary<string, string> replication = null, bool durable_writes = true)
        {
            var session = Connect("");
            session.CreateKeyspaceIfNotExists(_configuration.ClientOptions.DefaultKeyspace, replication, durable_writes);
            session.ChangeKeyspace(_configuration.ClientOptions.DefaultKeyspace);
            return session;
        }

        /// <summary>
        ///  Gets the cluster configuration.
        /// </summary>
        public Configuration Configuration
        {
            get { return _configuration; }
        }

        private ConcurrentDictionary<Guid,Session> _connectedSessions = new ConcurrentDictionary<Guid,Session>();

        private Metadata _metadata = null;
        /// <summary>
        ///  Gets read-only metadata on the connected cluster. <p> This includes the
        ///  know nodes (with their status as seen by the driver) as well as the schema
        ///  definitions.</p>
        /// </summary>
        public Metadata Metadata
        {
            get { return _metadata; }
        }

        /// <summary>
        ///  Shutdown this cluster instance. This closes all connections from all the
        ///  sessions of this <code>* Cluster</code> instance and reclaim all resources
        ///  used by it. <p> This method has no effect if the cluster was already shutdown.</p>
        /// </summary>
        public void Shutdown(int timeoutMs = Timeout.Infinite)
        {
            foreach(var kv in _connectedSessions)
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

        public void Dispose()
        {
            Shutdown();
        }

        ~Cluster()
        {
            Shutdown();
        }

        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            return _metadata.RefreshSchema(keyspace, table);
        }

    }
}
