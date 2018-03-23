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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    ///  Helper class to build <link>Cluster</link> instances.
    /// </summary>
    public class Builder : IInitializer
    {
        private readonly List<IPEndPoint> _addresses = new List<IPEndPoint>();
        private readonly IList<string> _hostNames = new List<string>();
        private const int DefaultQueryAbortTimeout = 20000;
        private PoolingOptions _poolingOptions;
        private SocketOptions _socketOptions = new SocketOptions();
        private IAuthInfoProvider _authInfoProvider;
        private IAuthProvider _authProvider = NoneAuthProvider.Instance;
        private CompressionType _compression = CompressionType.NoCompression;
        private IFrameCompressor _customCompressor;
        private string _defaultKeyspace;

        private ILoadBalancingPolicy _loadBalancingPolicy;
        private ITimestampGenerator _timestampGenerator;
        private int _port = ProtocolOptions.DefaultPort;
        private int _queryAbortTimeout = DefaultQueryAbortTimeout;
        private QueryOptions _queryOptions = new QueryOptions();
        private IReconnectionPolicy _reconnectionPolicy;
        private IRetryPolicy _retryPolicy;
        private SSLOptions _sslOptions;
        private bool _withoutRowSetBuffering;
        private IAddressTranslator _addressTranslator = new DefaultAddressTranslator();
        private ISpeculativeExecutionPolicy _speculativeExecutionPolicy;
        private ProtocolVersion _maxProtocolVersion = ProtocolVersion.MaxSupported;
        private TypeSerializerDefinitions _typeSerializerDefinitions;
        private bool _noCompact;

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
        /// Gets the contact points that were added as <c>IPEndPoint"</c> instances.
        /// <para>
        /// Note that only contact points that were added using <see cref="AddContactPoint(IPEndPoint)"/> and
        /// <see cref="AddContactPoints(IPEndPoint[])"/> are returned by this property, as IP addresses and host names must be resolved and assigned
        /// the port number, which is performed on <see cref="Build()"/>.
        /// </para>
        /// </summary>
        public ICollection<IPEndPoint> ContactPoints
        {
            get { return _addresses; }
        }

        /// <summary>
        ///  The configuration that will be used for the new cluster. <p> You <b>should
        ///  not</b> modify this object directly as change made to the returned object may
        ///  not be used by the cluster build. Instead, you should use the other methods
        ///  of this <c>Builder</c></p>.
        /// </summary>
        /// 
        /// <returns>the configuration to use for the new cluster.</returns>
        public Configuration GetConfiguration()
        {
            var policies = new Policies(
                _loadBalancingPolicy,
                _reconnectionPolicy,
                _retryPolicy,
                _speculativeExecutionPolicy,
                _timestampGenerator);
            var config = new Configuration(policies,
                new ProtocolOptions(_port, _sslOptions).SetCompression(_compression)
                                                       .SetCustomCompressor(_customCompressor)
                                                       .SetMaxProtocolVersion(_maxProtocolVersion)
                                                       .SetNoCompact(_noCompact),
                _poolingOptions,
                _socketOptions,
                new ClientOptions(_withoutRowSetBuffering, _queryAbortTimeout, _defaultKeyspace),
                _authProvider,
                _authInfoProvider,
                _queryOptions,
                _addressTranslator);
            if (_typeSerializerDefinitions != null)
            {
                config.TypeSerializers = _typeSerializerDefinitions.Definitions;
            }
            return config;
        }

        /// <summary>
        ///  The port to use to connect to all Cassandra hosts. If not set through this
        ///  method, the default port (9042) will be used instead.
        /// </summary>
        /// <param name="port"> the port to set. </param>
        /// <returns>this Builder</returns>
        public Builder WithPort(int port)
        {
            _port = port;
            foreach (var addr in _addresses)
            {
                addr.Port = port;
            }
            return this;
        }


        /// <summary>
        /// Sets the QueryOptions to use for the newly created Cluster.
        /// 
        /// If no query options are set through this method, default query
        /// options will be used.
        /// </summary>
        /// <param name="options">the QueryOptions to use.</param>
        /// <returns>this Builder.</returns>
        public Builder WithQueryOptions(QueryOptions options)
        {
            _queryOptions = options;
            return this;
        }

        /// <summary>
        ///  Sets the compression to use for the transport.
        /// </summary>
        /// <param name="compression"> the compression to set </param>
        /// <returns>this Builder <see>ProtocolOptions.Compression</see></returns>
        public Builder WithCompression(CompressionType compression)
        {
            _compression = compression;
            return this;
        }

        /// <summary>
        /// Sets a custom compressor to be used for the compression type.
        /// If specified, the compression type is mandatory.
        /// If not specified the driver default compressor will be use for the compression type.
        /// </summary>
        /// <param name="compressor">Implementation of IFrameCompressor</param>
        public Builder WithCustomCompressor(IFrameCompressor compressor)
        {
            _customCompressor = compressor;
            return this;
        }

        /// <summary>
        ///  Adds a contact point. Contact points are addresses of Cassandra nodes that
        ///  the driver uses to discover the cluster topology. Only one contact point is
        ///  required (the driver will retrieve the address of the other nodes
        ///  automatically), but it is usually a good idea to provide more than one
        ///  contact point, as if that unique contact point is not available, the driver
        ///  won't be able to initialize itself correctly.
        /// </summary>
        /// <remarks>
        ///  However, this can be useful if the Cassandra nodes are behind a router and 
        ///  are not accessed directly. Note that if you are in this situation 
        ///  (Cassandra nodes are behind a router, not directly accessible), you almost 
        ///  surely want to provide a specific <c>IAddressTranslator</c> 
        ///  (through <link>Builder.WithAddressTranslater</link>) to translate actual 
        ///  Cassandra node addresses to the addresses the driver should use, otherwise 
        ///  the driver will not be able to auto-detect new nodes (and will generally not 
        ///  function optimally).
        /// </remarks>
        /// <param name="address">the address of the node to connect to</param> 
        /// <returns>this Builder</returns>
        public Builder AddContactPoint(string address)
        {
            _hostNames.Add(address ?? throw new ArgumentNullException(nameof(address)));
            return this;
        }

        /// <summary>
        ///  Add contact point. See <see cref="Builder.AddContactPoint(string)"/> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="address"> address of the node to add as contact point</param> 
        /// <returns>this Builder</returns>
        public Builder AddContactPoint(IPAddress address)
        {
            // Avoid creating IPEndPoint entries using the current port,
            // as the user might provide a different one by calling WithPort() after this call
            AddContactPoint(address.ToString());
            return this;
        }

        /// <summary>
        ///  Add contact point. See <see cref="Builder.AddContactPoint(string)"/> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="address"> address of the node to add as contact point</param> 
        /// <returns>this Builder</returns>
        public Builder AddContactPoint(IPEndPoint address)
        {
            _addresses.Add(address);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <see cref="Builder.AddContactPoint(string)"/> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param> 
        /// <returns>this Builder </returns>
        public Builder AddContactPoints(params string[] addresses)
        {
            AddContactPoints(addresses.AsEnumerable());
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public Builder AddContactPoints(IEnumerable<string> addresses)
        {
            foreach (var address in addresses)
            {
                AddContactPoint(address);
            }
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public Builder AddContactPoints(params IPAddress[] addresses)
        {
            AddContactPoints(addresses.AsEnumerable());
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public Builder AddContactPoints(IEnumerable<IPAddress> addresses)
        {
            foreach (var address in addresses)
            {
                AddContactPoint(address);
            }
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point
        ///  </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder AddContactPoints(params IPEndPoint[] addresses)
        {
            AddContactPoints(addresses.AsEnumerable());
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point
        ///  </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder AddContactPoints(IEnumerable<IPEndPoint> addresses)
        {
            _addresses.AddRange(addresses);
            return this;
        }

        /// <summary>
        ///  Configure the load balancing policy to use for the new cluster. <p> If no
        ///  load balancing policy is set through this method,
        ///  <link>Policies.DefaultLoadBalancingPolicy</link> will be used instead.</p>
        /// </summary>
        /// <param name="policy"> the load balancing policy to use </param>
        /// <returns>this Builder</returns>
        public Builder WithLoadBalancingPolicy(ILoadBalancingPolicy policy)
        {
            _loadBalancingPolicy = policy;
            return this;
        }

        /// <summary>
        ///  Configure the reconnection policy to use for the new cluster. <p> If no
        ///  reconnection policy is set through this method,
        ///  <link>Policies.DefaultReconnectionPolicy</link> will be used instead.</p>
        /// </summary>
        /// <param name="policy"> the reconnection policy to use </param>
        /// 
        /// <returns>this Builder</returns>
        public Builder WithReconnectionPolicy(IReconnectionPolicy policy)
        {
            _reconnectionPolicy = policy;
            return this;
        }

        /// <summary>
        /// Configure the retry policy to be used for the new cluster.
        /// <para>
        /// When the retry policy is not set with this method, the <see cref="Policies.DefaultRetryPolicy" />
        /// will be used instead.
        /// </para>
        /// <para>
        /// Use a <see cref="IExtendedRetryPolicy"/> implementation to cover all error scenarios.
        /// </para>
        /// </summary>
        /// <param name="policy"> the retry policy to use </param>
        /// <returns>this Builder</returns>
        public Builder WithRetryPolicy(IRetryPolicy policy)
        {
            _retryPolicy = policy;
            return this;
        }

        /// <summary>
        ///  Configure the speculative execution to use for the new cluster. 
        /// <para> 
        /// If no speculative execution policy is set through this method, <see cref="Policies.DefaultSpeculativeExecutionPolicy"/> will be used instead.
        /// </para>
        /// </summary>
        /// <param name="policy"> the speculative execution policy to use </param>
        /// <returns>this Builder</returns>
        public Builder WithSpeculativeExecutionPolicy(ISpeculativeExecutionPolicy policy)
        {
            _speculativeExecutionPolicy = policy;
            return this;
        }

        /// <summary>
        /// Configures the generator that will produce the client-side timestamp sent with each query.
        /// <para>
        /// This feature is only available with protocol version 3 or above of the native protocol. 
        /// With earlier versions, timestamps are always generated server-side, and setting a generator
        /// through this method will have no effect.
        /// </para>
        /// <para>
        /// If no generator is set through this method, the driver will default to client-side timestamps
        /// by using <see cref="AtomicMonotonicTimestampGenerator"/>.
        /// </para>
        /// </summary>
        /// <param name="generator">The generator to use.</param>
        /// <returns>This builder instance</returns>
        public Builder WithTimestampGenerator(ITimestampGenerator generator)
        {
            _timestampGenerator = generator;
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
        ///  Uses the provided credentials when connecting to Cassandra hosts. <p> This
        ///  should be used if the Cassandra cluster has been configured to use the
        ///  <c>PasswordAuthenticator</c>. If the the default <c>*
        ///  AllowAllAuthenticator</c> is used instead, using this method has no effect.</p>
        /// </summary>
        /// <param name="username"> the user name to use to login to Cassandra hosts.</param>
        /// <param name="password"> the password corresponding to </param>
        /// <returns>this Builder</returns>
        public Builder WithCredentials(String username, String password)
        {
            _authInfoProvider = new SimpleAuthInfoProvider().Add("username", username).Add("password", password);
            _authProvider = new PlainTextAuthProvider(username, password);
            return this;
        }


        /// <summary>
        ///  Use the specified AuthProvider when connecting to Cassandra hosts. <p> Use
        ///  this method when a custom authentication scheme is in place. You shouldn't
        ///  call both this method and {@code withCredentials}' on the same
        ///  <c>Builder</c> instance as one will supersede the other</p>
        /// </summary>
        /// <param name="authProvider"> the <link>AuthProvider"></link> to use to login to Cassandra hosts.</param>
        /// <returns>this Builder</returns>
        public Builder WithAuthProvider(IAuthProvider authProvider)
        {
            _authProvider = authProvider;
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
            _withoutRowSetBuffering = true;
            return this;
        }

        /// <summary>
        /// Specifies the number of milliseconds that the driver should wait for the response before the query times out in a synchronous operation.
        /// <para>
        /// This will cause that synchronous operations like <see cref="ISession.Execute(string)"/> to throw a <see cref="System.TimeoutException"/> 
        /// after the specified number of milliseconds.
        /// </para>
        /// Default timeout value is set to <code>20,000</code> (20 seconds).
        /// </summary>
        /// <remarks>
        /// If you want to define a read timeout at a lower level, you can use <see cref="Cassandra.SocketOptions.SetReadTimeoutMillis(int)"/>.
        /// </remarks>
        /// <param name="queryAbortTimeout">Timeout specified in milliseconds.</param>
        /// <returns>this builder</returns>
        public Builder WithQueryTimeout(int queryAbortTimeout)
        {
            _queryAbortTimeout = queryAbortTimeout;
            return this;
        }

        /// <summary>
        ///  Sets default keyspace name for the created cluster.
        /// </summary>
        /// <param name="defaultKeyspace">Default keyspace name.</param>
        /// <returns>this builder</returns>
        public Builder WithDefaultKeyspace(string defaultKeyspace)
        {
            _defaultKeyspace = defaultKeyspace;
            return this;
        }

        /// <summary>
        /// Configures the socket options that are going to be used to create the connections to the hosts.
        /// </summary>
        public Builder WithSocketOptions(SocketOptions value)
        {
            _socketOptions = value;
            return this;
        }

        public Builder WithPoolingOptions(PoolingOptions value)
        {
            _poolingOptions = value;
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
        /// <returns>this builder</returns>
        public Builder WithSSL()
        {
            _sslOptions = new SSLOptions();
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
            _sslOptions = sslOptions;
            return this;
        }

        /// <summary>
        ///  Configures the address translater to use for the new cluster.
        /// </summary>
        /// <remarks>
        /// See <c>IAddressTranslater</c> for more detail on address translation,
        /// but the default tanslater, <c>DefaultAddressTranslator</c>, should be
        /// correct in most cases. If unsure, stick to the default.
        /// </remarks>
        /// <param name="addressTranslator">the translater to use.</param>
        /// <returns>this Builder</returns>
        public Builder WithAddressTranslator(IAddressTranslator addressTranslator)
        {
            _addressTranslator = addressTranslator;
            return this;
        }

        /// <summary>
        /// <para>Limits the maximum protocol version used to connect to the nodes</para>
        /// <para>
        /// When it is not set, the protocol version used is negotiated between the driver and the Cassandra
        /// cluster upon establishing the first connection.
        /// </para>
        /// <para>
        /// Useful when connecting to a cluster that contains nodes with different major/minor versions 
        /// of Cassandra. For example, preparing for a rolling upgrade of the Cluster.
        /// </para>
        /// </summary>
        /// <param name="version">
        /// <para>The native protocol version.</para>
        /// <para>Different Cassandra versions support a range of protocol versions, for example: </para>
        /// <para>- Cassandra 2.0 (DSE 4.0 - 4.6): Supports protocol versions 1 and 2.</para>
        /// <para>- Cassandra 2.1 (DSE 4.7 - 4.8): Supports protocol versions 1, 2 and 3.</para>
        /// <para>- Cassandra 2.2: Supports protocol versions 1, 2, 3 and 4.</para>
        /// <para>- Cassandra 3.0: Supports protocol versions 3 and 4.</para>
        /// </param>
        /// <remarks>Some Cassandra features are only available with a specific protocol version.</remarks>
        /// <returns>this instance</returns>
        public Builder WithMaxProtocolVersion(byte version)
        {
            return WithMaxProtocolVersion((ProtocolVersion)version);
        }

        /// <summary>
        /// <para>Limits the maximum protocol version used to connect to the nodes</para>
        /// <para>
        /// When it is not set, the protocol version used is negotiated between the driver and the Cassandra
        /// cluster upon establishing the first connection.
        /// </para>
        /// <para>
        /// Useful when connecting to a cluster that contains nodes with different major/minor versions 
        /// of Cassandra. For example, preparing for a rolling upgrade of the Cluster.
        /// </para>
        /// </summary>
        /// <remarks>Some Cassandra features are only available with a specific protocol version.</remarks>
        /// <returns>this instance</returns>
        public Builder WithMaxProtocolVersion(ProtocolVersion version)
        {
            if (version == 0)
            {
                throw new ArgumentException("Protocol version 0 does not exist.");
            }
            _maxProtocolVersion = version;
            return this;
        }

        /// <summary>
        /// Enables the NO_COMPACT startup option.
        /// <para>
        /// When this option is set, <c>SELECT</c>, <c>UPDATE</c>, <c>DELETE</c>, and <c>BATCH</c> statements
        /// on <c>COMPACT STORAGE</c> tables function in "compatibility" mode which allows seeing these tables
        /// as if they were "regular" CQL tables.
        /// </para>
        /// <para>
        /// This option only affects interactions with tables using <c>COMPACT STORAGE</c> and it is only
        /// supported by C* 3.0.16+, 3.11.2+, 4.0+ and DSE 6.0+.
        /// </para>
        /// </summary>
        public Builder WithNoCompact()
        {
            _noCompact = true;
            return this;
        }

        /// <summary>
        /// Sets the <see cref="TypeSerializer{T}"/> to be used, replacing the default ones.
        /// </summary>
        /// <param name="definitions"></param>
        /// <returns>this instance</returns>
        public Builder WithTypeSerializers(TypeSerializerDefinitions definitions)
        {
            if (definitions == null)
            {
                throw new ArgumentNullException("definitions");
            }
            if (_typeSerializerDefinitions != null)
            {
                const string message = "TypeSerializers definitions were already set." +
                    "Use a single TypeSerializerDefinitions instance and call Define() multiple times";
                throw new InvalidOperationException(message);
            }
            _typeSerializerDefinitions = definitions;
            return this;
        }

        /// <summary>
        ///  Build the cluster with the configured set of initial contact points and policies.
        /// </summary>
        /// <exception cref="NoHostAvailableException">Throws a NoHostAvailableException when no host could be resolved.</exception>
        /// <exception cref="ArgumentException">Throws an ArgumentException when no contact point was provided.</exception>
        /// <returns>the newly build Cluster instance. </returns>
        public Cluster Build()
        {
            return Cluster.BuildFrom(this, _hostNames);
        }
    }
}