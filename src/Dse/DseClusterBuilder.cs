using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using Cassandra;
using Cassandra.Serialization;
using Dse.Geometry;
using Dse.Graph;
using Dse.Policies;

namespace Dse
{
    /// <summary>
    /// Helper class to build <see cref="DseCluster"/> instances.
    /// </summary>
    public class DseClusterBuilder : Builder
    {
        private static readonly Logger Logger = new Logger(typeof(DseClusterBuilder));
        private TypeSerializerDefinitions _typeSerializerDefinitions;
        private IAddressTranslator _addressTranslator = new IdentityAddressTranslator();
        private ILoadBalancingPolicy _loadBalancingPolicy;
        /// <summary>
        /// Gets the DSE Graph options.
        /// </summary>
        public GraphOptions GraphOptions { get; private set; }

        /// <summary>
        /// Sets the DSE Graph options.
        /// </summary>
        /// <returns>this instance</returns>
        public DseClusterBuilder WithGraphOptions(GraphOptions options)
        {
            GraphOptions = options;
            return this;
        }


        /// <summary>
        ///  The port to use to connect to all Cassandra hosts. If not set through this
        ///  method, the default port (9042) will be used instead.
        /// </summary>
        /// <param name="port"> the port to set. </param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder WithPort(int port)
        {
            base.WithPort(port);
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
        public new DseClusterBuilder WithQueryOptions(QueryOptions options)
        {
            base.WithQueryOptions(options);
            return this;
        }

        /// <summary>
        ///  Sets the compression to use for the transport.
        /// </summary>
        /// <param name="compression"> the compression to set </param>
        /// <returns>this Builder <see>ProtocolOptions.Compression</see></returns>
        public new DseClusterBuilder WithCompression(CompressionType compression)
        {
            base.WithCompression(compression);
            return this;
        }

        /// <summary>
        /// Sets a custom compressor to be used for the compression type.
        /// If specified, the compression type is mandatory.
        /// If not specified the driver default compressor will be use for the compression type.
        /// </summary>
        /// <param name="compressor">Implementation of IFrameCompressor</param>
        public new DseClusterBuilder WithCustomCompressor(IFrameCompressor compressor)
        {
            base.WithCustomCompressor(compressor);
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
        public new DseClusterBuilder AddContactPoint(string address)
        {
            base.AddContactPoint(address);
            return this;
        }

        /// <summary>
        ///  Add contact point. See <see cref="Builder.AddContactPoint(string)"/> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="address"> address of the node to add as contact point</param> 
        /// <returns>this Builder</returns>
        public new DseClusterBuilder AddContactPoint(IPAddress address)
        {
            base.AddContactPoint(address);
            return this;
        }

        /// <summary>
        ///  Add contact point. See <see cref="Builder.AddContactPoint(string)"/> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="address"> address of the node to add as contact point</param> 
        /// <returns>this Builder</returns>
        public new DseClusterBuilder AddContactPoint(IPEndPoint address)
        {
            base.AddContactPoint(address);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <see cref="Builder.AddContactPoint(string)"/> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param> 
        /// <returns>this Builder </returns>
        public new DseClusterBuilder AddContactPoints(params string[] addresses)
        {
            base.AddContactPoints(addresses);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder AddContactPoints(IEnumerable<string> addresses)
        {
            base.AddContactPoints(addresses);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder AddContactPoints(params IPAddress[] addresses)
        {
            base.AddContactPoints(addresses);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder AddContactPoints(IEnumerable<IPAddress> addresses)
        {
            base.AddContactPoints(addresses);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point</param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder AddContactPoints(params IPEndPoint[] addresses)
        {
            base.AddContactPoints(addresses);
            return this;
        }

        /// <summary>
        ///  Add contact points. See <link>Builder.AddContactPoint</link> for more details
        ///  on contact points.
        /// </summary>
        /// <param name="addresses"> addresses of the nodes to add as contact point
        /// </param>
        /// <returns>this instance</returns>
        public new DseClusterBuilder AddContactPoints(IEnumerable<IPEndPoint> addresses)
        {
            base.AddContactPoints(addresses);
            return this;
        }

        /// <summary>
        /// Configures the load balancing policy to use for the new cluster.
        /// <para> 
        /// If no load balancing policy is set through this method, <see cref="DseLoadBalancingPolicy"/>
        /// will be used instead.
        /// </para>
        /// </summary>
        /// <param name="policy"> the load balancing policy to use </param>
        /// <returns>this instance</returns>
        public new DseClusterBuilder WithLoadBalancingPolicy(ILoadBalancingPolicy policy)
        {
            _loadBalancingPolicy = policy;
            base.WithLoadBalancingPolicy(policy);
            return this;
        }

        /// <summary>
        ///  Configure the reconnection policy to use for the new cluster. <p> If no
        ///  reconnection policy is set through this method,
        ///  <link>Policies.DefaultReconnectionPolicy</link> will be used instead.</p>
        /// </summary>
        /// <param name="policy"> the reconnection policy to use </param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder WithReconnectionPolicy(IReconnectionPolicy policy)
        {
            base.WithReconnectionPolicy(policy);
            return this;
        }

        /// <summary>
        ///  Configure the retry policy to use for the new cluster. <p> If no retry policy
        ///  is set through this method, <link>Policies.DefaultRetryPolicy</link> will
        ///  be used instead.</p>
        /// </summary>
        /// <param name="policy"> the retry policy to use </param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder WithRetryPolicy(IRetryPolicy policy)
        {
            base.WithRetryPolicy(policy);
            return this;
        }

        /// <summary>
        ///  Configure the speculative execution to use for the new cluster. 
        /// <para> 
        /// If no speculative execution policy is set through this method, <see cref="Cassandra.Policies.DefaultSpeculativeExecutionPolicy"/> will be used instead.
        /// </para>
        /// </summary>
        /// <param name="policy"> the speculative execution policy to use </param>
        /// <returns>this Builder</returns>
        public new DseClusterBuilder WithSpeculativeExecutionPolicy(ISpeculativeExecutionPolicy policy)
        {
            base.WithSpeculativeExecutionPolicy(policy);
            return this;
        }

        /// <summary>
        ///  Configure the cluster by applying settings from ConnectionString. 
        /// </summary>
        /// <param name="connectionString"> the ConnectionString to use </param>
        /// 
        /// <returns>this Builder</returns>
        public new DseClusterBuilder WithConnectionString(string connectionString)
        {
            base.WithConnectionString(connectionString);
            return this;
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
        public new DseClusterBuilder WithCredentials(String username, String password)
        {
            base.WithCredentials(username, password);
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
        public new DseClusterBuilder WithAuthProvider(IAuthProvider authProvider)
        {
            base.WithAuthProvider(authProvider);
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
        public new DseClusterBuilder WithQueryTimeout(int queryAbortTimeout)
        {
            base.WithQueryTimeout(queryAbortTimeout);
            return this;
        }

        /// <summary>
        ///  Sets default keyspace name for the created cluster.
        /// </summary>
        /// <param name="defaultKeyspace">Default keyspace name.</param>
        /// <returns>this builder</returns>
        public new DseClusterBuilder WithDefaultKeyspace(string defaultKeyspace)
        {
            base.WithDefaultKeyspace(defaultKeyspace);
            return this;
        }

        /// <summary>
        /// Configures the socket options that are going to be used to create the connections to the hosts.
        /// </summary>
        public new DseClusterBuilder WithSocketOptions(SocketOptions value)
        {
            base.WithSocketOptions(value);
            return this;
        }

        /// <summary>
        /// Sets the pooling options for the cluster.
        /// </summary>
        /// <returns>this instance</returns>
        public new DseClusterBuilder WithPoolingOptions(PoolingOptions value)
        {
            base.WithPoolingOptions(value);
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
        // ReSharper disable once InconsistentNaming
        public new DseClusterBuilder WithSSL()
        {
            base.WithSSL();
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
        // ReSharper disable once InconsistentNaming
        public new DseClusterBuilder WithSSL(SSLOptions sslOptions)
        {
            base.WithSSL(sslOptions);
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
        public new DseClusterBuilder WithAddressTranslator(IAddressTranslator addressTranslator)
        {
            _addressTranslator = addressTranslator;
            base.WithAddressTranslator(addressTranslator);
            return this;
        }

        /// <summary>
        /// <para>Limits the maximum protocol version used to connect to the nodes, when it is not set
        /// protocol version used between the driver and the Cassandra cluster is negotiated upon establishing 
        /// the first connection.</para>
        /// <para>Useful for using the driver against a cluster that contains nodes with different major/minor versions 
        /// of Cassandra. For example, preparing for a rolling upgrade of the Cluster.</para>
        /// </summary>
        /// <param name="version">
        /// <para>The native protocol version.</para>
        /// <para>Different Cassandra versions support a range of protocol versions, for example: </para>
        /// <para>- Cassandra 2.0 (DSE 4.0 – 4.6): Supports protocol versions 1 and 2.</para>
        /// <para>- Cassandra 2.1 (DSE 4.7 – 4.8): Supports protocol versions 1, 2 and 3.</para>
        /// <para>- Cassandra 2.2: Supports protocol versions 1, 2, 3 and 4.</para>
        /// <para>- Cassandra 3.0: Supports protocol versions 3 and 4.</para>
        /// </param>
        /// <remarks>Some Cassandra features are only available with a specific protocol version.</remarks>
        /// <returns>this instance</returns>
        public new DseClusterBuilder WithMaxProtocolVersion(byte version)
        {
            base.WithMaxProtocolVersion(version);
            return this;
        }

        /// <summary>
        /// Sets the <see cref="TypeSerializer{T}"/> to be used, replacing the default ones.
        /// </summary>
        /// <returns>this instance</returns>
        public new DseClusterBuilder WithTypeSerializers(TypeSerializerDefinitions definitions)
        {
            //Store the definitions
            //If the definitions for GeoTypes or other have already been defined those will be considered.
            _typeSerializerDefinitions = definitions;
            return this;
        }

        /// <summary>
        /// Builds the cluster with the configured set of initial contact points and policies.
        /// </summary>
        /// <returns>
        /// A new <see cref="DseCluster"/> instance.
        /// </returns>
        public new DseCluster Build()
        {
            var dseAssembly = Assembly.GetExecutingAssembly();
            var cassandraAssembly = typeof(ISession).Assembly;
            Logger.Info("Using DataStax C# DSE driver v{0} (core driver v{1})", 
                FileVersionInfo.GetVersionInfo(dseAssembly.Location).FileVersion,
                FileVersionInfo.GetVersionInfo(cassandraAssembly.Location).FileVersion);
            var typeSerializerDefinitions = _typeSerializerDefinitions ?? new TypeSerializerDefinitions();
            typeSerializerDefinitions
                .Define(new LineStringSerializer())
                .Define(new PointSerializer())
                .Define(new PolygonSerializer());
            base.WithTypeSerializers(typeSerializerDefinitions);
            if (_loadBalancingPolicy == null)
            {
                base.WithLoadBalancingPolicy(DseLoadBalancingPolicy.CreateDefault());
            }
            var coreCluster = base.Build();
            var config = new DseConfiguration(coreCluster.Configuration, GraphOptions ?? new GraphOptions());
            //To be replace after CSHARP-444.
            config.AddressTranslator = _addressTranslator;
            return new DseCluster(
                coreCluster, 
                config);
        }
    }
}
