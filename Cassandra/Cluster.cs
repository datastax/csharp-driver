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
﻿using System;
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
                                                       _configuration.AuthInfoProvider);

            _metadata.SetupControllConnection(controlConnection);
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
            Session scs = new Session(this, _configuration.Policies,
                                      _configuration.ProtocolOptions,
                                      _configuration.PoolingOptions,
                                      _configuration.SocketOptions,
                                      _configuration.ClientOptions,
                                      _configuration.AuthProvider,
                                      _configuration.AuthInfoProvider, keyspace, true);
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
            try
            {
                session.ChangeKeyspace(_configuration.ClientOptions.DefaultKeyspace);
            }
            catch (InvalidQueryException)
            {
                session.CreateKeyspaceIfNotExists(_configuration.ClientOptions.DefaultKeyspace, replication, durable_writes);
                session.ChangeKeyspace(_configuration.ClientOptions.DefaultKeyspace);
            }
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

    /// <summary>
    ///  Initializer for <link>Cluster</link> instances. <p> If you want to create a
    ///  new <code>Cluster</code> instance programmatically, then it is advised to use
    ///  <link>Cluster.Builder</link> (obtained through the
    ///  <link>Cluster#builder</link> method).</p> <p> But it is also possible to
    ///  implement a custom <code>Initializer</code> that retrieve initialization from
    ///  a web-service or from a configuration file for instance.</p>
    /// </summary>
    public interface IInitializer
    {

        /// <summary>
        ///  Gets the initial Cassandra hosts to connect to.See
        ///  <link>Builder.AddContactPoint</link> for more details on contact
        /// </summary>
        ICollection<IPAddress> ContactPoints { get; }

        /// <summary>
        ///  The configuration to use for the new cluster. <p> Note that some
        ///  configuration can be modified after the cluster initialization but some other
        ///  cannot. In particular, the ones that cannot be change afterwards includes:
        ///  <ul> <li>the port use to connect to Cassandra nodes (see
        ///  <link>ProtocolOptions</link>).</li> <li>the policies used (see
        ///  <link>Policies</link>).</li> <li>the authentication info provided (see
        ///  <link>Configuration</link>).</li> <li>whether metrics are enabled (see
        ///  <link>Configuration</link>).</li> </ul></p>
        /// </summary>
        Configuration GetConfiguration();
    }

    /// <summary>
    ///  Helper class to build <link>Cluster</link> instances.
    /// </summary>
    public class Builder : IInitializer
    {

        private readonly List<IPAddress> _addresses = new List<IPAddress>();
        private int _port = ProtocolOptions.DefaultPort;
        private IAuthProvider _authProvider = NoneAuthProvider.Instance;
        private IAuthInfoProvider _authInfoProvider = null;
        private CompressionType _compression = CompressionType.NoCompression;
        private readonly PoolingOptions _poolingOptions = new PoolingOptions();
        private readonly SocketOptions _socketOptions = new SocketOptions();
        private SSLOptions _sslOptions = null;

        private ILoadBalancingPolicy _loadBalancingPolicy;
        private IReconnectionPolicy _reconnectionPolicy;
        private IRetryPolicy _retryPolicy;
        private bool _withoutRowSetBuffering = false;

        private string _defaultKeyspace = null;

        private int _queryAbortTimeout = Timeout.Infinite;

        public ICollection<IPAddress> ContactPoints
        {
            get { return _addresses; }
        }

        public int Port
        {
            get { return _port; }
        }

        /// <summary>
        ///  The port to use to connect to the Cassandra host. If not set through this
        ///  method, the default port (9042) will be used instead.
        /// </summary>
        /// <param name="port"> the port to set. </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithPort(int port)
        {
            this._port = port;
            return this;
        }


        /// <summary>
        ///  Sets the compression to use for the transport.
        /// </summary>
        /// <param name="compression"> the compression to set </param>
        /// 
        /// <returns>this Builder <see>ProtocolOptions.Compression</see></returns>
        public Builder WithCompression(CompressionType compression)
        {
            this._compression = compression;
            return this;
        }

        /// <summary>
        ///  The pooling options used by this builder.
        /// </summary>
        /// 
        /// <returns>the pooling options that will be used by this builder. You can use
        ///  the returned object to define the initial pooling options for the built
        ///  cluster.</returns>
        public PoolingOptions PoolingOptions
        {
            get { return _poolingOptions; }
        }

        /// <summary>
        ///  The socket options used by this builder.
        /// </summary>
        /// 
        /// <returns>the socket options that will be used by this builder. You can use
        ///  the returned object to define the initial socket options for the built
        ///  cluster.</returns>
        public SocketOptions SocketOptions
        {
            get { return _socketOptions; }
        }

        /// <summary>
        ///  Adds a contact point. Contact points are addresses of Cassandra nodes that
        ///  the driver uses to discover the cluster topology. Only one contact point is
        ///  required (the driver will retrieve the address of the other nodes
        ///  automatically), but it is usually a good idea to provide more than one
        ///  contact point, as if that unique contact point is not available, the driver
        ///  won't be able to initialize itself correctly.'
        /// </summary>
        /// <param name="address"> the address of the node to connect to </param>
        /// 
        /// <returns>this Builder </returns>
        public Builder AddContactPoint(string address)
        {
            this._addresses.AddRange(Utils.ResolveHostByName(address));
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder#addContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point
        ///  </param>
        /// 
        /// <returns>this Builder </returns>
        public Builder AddContactPoints(params string[] addresses)
        {
            foreach (string address in addresses)
                AddContactPoint(address);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder#addContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point
        ///  </param>
        /// 
        /// <returns>this Builder <see>Builder#addContactPoint</see></returns>
        public Builder AddContactPoints(params IPAddress[] addresses)
        {
            foreach (var address in addresses)
                this._addresses.Add(address);
            return this;
        }

        /// <summary>
        ///  Configure the load balancing policy to use for the new cluster. <p> If no
        ///  load balancing policy is set through this method,
        ///  <link>Policies#DefaultLoadBalancingPolicy</link> will be used instead.</p>
        /// </summary>
        /// <param name="policy"> the load balancing policy to use </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithLoadBalancingPolicy(ILoadBalancingPolicy policy)
        {
            this._loadBalancingPolicy = policy;
            return this;
        }

        /// <summary>
        ///  Configure the reconnection policy to use for the new cluster. <p> If no
        ///  reconnection policy is set through this method,
        ///  <link>Policies#DefaultReconnectionPolicy</link> will be used instead.</p>
        /// </summary>
        /// <param name="policy"> the reconnection policy to use </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithReconnectionPolicy(IReconnectionPolicy policy)
        {
            this._reconnectionPolicy = policy;
            return this;
        }

        /// <summary>
        ///  Configure the retry policy to use for the new cluster. <p> If no retry policy
        ///  is set through this method, <link>Policies#DefaultRetryPolicy</link> will
        ///  be used instead.</p>
        /// </summary>
        /// <param name="policy"> the retry policy to use </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithRetryPolicy(IRetryPolicy policy)
        {
            this._retryPolicy = policy;
            return this;
        }

        /// <summary>
        ///  Configure the cluster by applying settings from ConnectionString. 
        /// </summary>
        /// <param name="connectionString"> the ConnectionString to use </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithConnectionString(string connectionString)
        {
            var cnb = new CassandraConnectionStringBuilder(connectionString);
            return cnb.ApplyToBuilder(this);
        }

        /// <summary>
        ///  The configuration that will be used for the new cluster. <p> You <b>should
        ///  not</b> modify this object directly as change made to the returned object may
        ///  not be used by the cluster build. Instead, you should use the other methods
        ///  of this <code>Builder</code></p>.
        /// </summary>
        /// 
        /// <returns>the configuration to use for the new cluster.</returns>
        public Configuration GetConfiguration()
        {
            var policies = new Policies(
                _loadBalancingPolicy ?? Cassandra.Policies.DefaultLoadBalancingPolicy,
                _reconnectionPolicy ?? Cassandra.Policies.DefaultReconnectionPolicy,
                _retryPolicy ?? Cassandra.Policies.DefaultRetryPolicy
                );

            return new Configuration(policies,
                                     new ProtocolOptions(_port, _sslOptions).SetCompression(_compression),
                                     _poolingOptions,
                                     _socketOptions,
                                     new ClientOptions(_withoutRowSetBuffering, _queryAbortTimeout, _defaultKeyspace),
                                     _authProvider, _authInfoProvider
                );
        }

        /// <summary>
        ///  Uses the provided credentials when connecting to Cassandra hosts.
        ///  <p> This should be used if the Cassandra cluster has been configured to
        ///  use the {@code PasswordAuthenticator}. If the the default 
        ///  <code>AllowAllAuthenticator</code> is used instead, using this 
        ///  method has no effect.</p>
        /// </summary>
        /// <param name="username"> the username to use to login to Cassandra hosts.</param>
        /// <param name="password"> the password corresponding to <code>username</code>.</param>
        /// <returns>this Builder</returns>
        public Builder WithCredentials(String username, String password)
        {
            this._authInfoProvider = new SimpleAuthInfoProvider().Add("username", username).Add("password",password);
            return this;
        }

        /// <summary>
        ///  Use the specified AuthProvider when connecting to Cassandra hosts. <p> Use
        ///  this method when a custom authentication scheme is in place. You shouldn't
        ///  call both this method and {@code withCredentials}' on the same
        ///  <code>Builder</code> instance as one will supercede the other</p>
        /// </summary>
        /// <param name="authProvider"> the <link>AuthProvider"></link> to use to login to Cassandra hosts.</param>
        /// <returns>this Builder</returns>
        public Builder WithAuthProvider(IAuthProvider authProvider)
        {
            this._authProvider = authProvider;
            return this;
        }

        /// <summary>
        ///  Disables row set buffering for the created cluster (row set buffering is enabled by
        ///  default otherwise).
        /// </summary>
        /// 
        /// <returns>this builder</returns>
        public Builder WithoutRowSetBuffering()
        {
            this._withoutRowSetBuffering = true;
            return this;
        }


        /// <summary>
        ///  Sets the timeout for a single query within created cluster.
        ///  After the expiry of the timeout, query will be aborted.
        ///  Default timeout value is set to <code>Infinity</code>
        /// </summary>
        /// <param name="queryAbortTimeout">Timeout specified in milliseconds.</param>
        /// <returns>this builder</returns>
        public Builder WithQueryTimeout(int queryAbortTimeout)
        {
            this._queryAbortTimeout = queryAbortTimeout;
            return this;
        }

        /// <summary>
        ///  Sets default keyspace name for the created cluster.
        /// </summary>
        /// <param name="defaultKeyspace">Default keyspace name.</param>
        /// <returns>this builder</returns>
        public Builder WithDefaultKeyspace(string defaultKeyspace)
        {
            this._defaultKeyspace = defaultKeyspace;
            return this;
        }

        /// <summary>
        ///  Enables the use of SSL for the created Cluster. Calling this method will use default SSL options. 
        /// </summary>
        /// <remarks>
        /// If SSL is enabled, the driver will not connect to any
        /// Cassandra nodes that doesn't have SSL enabled and it is strongly
        /// advised to enable SSL on every Cassandra node if you plan on using
        /// SSL in the driver. Note that SSL certificate common name(CN) on Cassandra node must match Cassandra node hostname.
        /// </remarks>
        /// <param name="sslOptions">SSL options to use.</param>
        /// <returns>this builder</returns>        
        public Builder WithSSL()
        {
            this._sslOptions = new SSLOptions();
            return this;
        }

        /// <summary>
        ///  Enables the use of SSL for the created Cluster using the provided options. 
        /// </summary>
        /// <remarks>
        /// If SSL is enabled, the driver will not connect to any
        /// Cassandra nodes that doesn't have SSL enabled and it is strongly
        /// advised to enable SSL on every Cassandra node if you plan on using
        /// SSL in the driver. Note that SSL certificate common name(CN) on Cassandra node must match Cassandra node hostname.
        /// </remarks>
        /// <param name="sslOptions">SSL options to use.</param>
        /// <returns>this builder</returns>        
        public Builder WithSSL(SSLOptions sslOptions)
        {
            this._sslOptions = sslOptions;
            return this;
        }

        /// <summary>
        ///  Build the cluster with the configured set of initial contact points and
        ///  policies. This is a shorthand for <code>Cluster.buildFrom(this)</code>.
        /// </summary>
        /// 
        /// <returns>the newly build Cluster instance. </returns>
        public Cluster Build()
        {
            return Cluster.BuildFrom(this);
        }

    }
}
