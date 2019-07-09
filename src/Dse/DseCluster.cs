//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Dse.Cloud;
using Dse.Connections;
using Dse.Requests;
using Dse.Serialization;
using Dse.SessionManagement;
using Dse.Tasks;

namespace Dse
{
    /// <summary>
    /// Information and known state of a DSE cluster.
    /// <para>
    /// This is the main entry point of the DSE driver. It extends the CQL driver's ICluster instance with DSE-specific
    /// features.
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// IDseCluster cluster = DseCluster.Builder().AddContactPoint("192.168.0.1").Build();
    /// IDseSession session = cluster.Connect("ks1");
    /// foreach (Row row in session.Execute(query))
    /// {
    ///     //do something...
    /// }
    /// </code>
    /// </example>
    public class DseCluster : IInternalDseCluster
    {
        private readonly IInternalCluster _coreCluster;
        private readonly ISessionFactory<IInternalDseSession> _dseSessionFactory;

        /// <summary>
        /// Represents an event that is triggered when a new host is added to the cluster.
        /// </summary>
        public event Action<Host> HostAdded;

        /// <summary>
        /// Represents an event that is triggered when a new host is decommissioned from the cluster.
        /// </summary>
        public event Action<Host> HostRemoved;

        /// <inheritdoc/>
        public Metadata Metadata => _coreCluster.Metadata;

        /// <inheritdoc/>
        public DseConfiguration Configuration { get; }

        /// <inheritdoc/>
        Configuration ICluster.Configuration => Configuration.CassandraConfiguration;

        /// <summary>
        /// Creates a new <see cref="DseClusterBuilder"/> instance.
        /// </summary>
        public static DseClusterBuilder Builder()
        {
            return new DseClusterBuilder();
        }

        internal DseCluster(IInitializer initializer, IReadOnlyList<string> hostnames, DseConfiguration config, IDseCoreClusterFactory dseCoreClusterFactory)
        {
            Configuration = config;
            _coreCluster = dseCoreClusterFactory.Create(this, initializer, hostnames, config.CassandraConfiguration);
            _coreCluster.HostAdded += OnCoreHostAdded;
            _coreCluster.HostRemoved += OnCoreHostRemoved;
            _dseSessionFactory = config.DseSessionFactoryBuilder.BuildWithCluster(this);
        }

        //TODO
        internal static async Task<IDseCluster> ForClusterConfigAsync(string url, string certFile)
        {
            var metadata = new CloudMetadataService();
            var clusterMetadata = await metadata.GetClusterMetadataAsync(url, certFile).ConfigureAwait(false);

            var proxyAddress = clusterMetadata.ContactInfo.SniProxyAddress;
            var separatorIndex = proxyAddress.IndexOf(':');

            if (separatorIndex == -1)
            {
                throw new DriverInternalError($"The SNI endpoint address should contain ip/name and port. Address received: {proxyAddress}");
            }

            var ipOrName = proxyAddress.Substring(0, separatorIndex);
            var port = int.Parse(proxyAddress.Substring(separatorIndex + 1));
            var isIp = IPAddress.TryParse(ipOrName, out var address);
            var sniOptions = new SniOptions(address, port, isIp ? null : ipOrName);

            var builder = 
                DseCluster.Builder()
                          .AddContactPoints(clusterMetadata.ContactInfo.ContactPoints);

            var sslOptions = new SSLOptions(SslProtocols.Tls12 | SslProtocols.Tls, false, (sender, certificate, chain, errors) => true); // TODO REMOVE CERT VALIDATION CALLBACK

            if (certFile != null)
            {
                sslOptions = sslOptions.SetCertificateCollection(new X509Certificate2Collection(new[]
                    { new X509Certificate2(certFile) }));
            }
               
            return builder
                      .WithSSL(sslOptions)
                      .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(clusterMetadata.ContactInfo.LocalDc)))
                      .WithEndPointResolver(new SniEndPointResolver(new DnsResolver(), sniOptions))
                      .Build();
        }

        /// <summary>
        /// Calls <see cref="Shutdown(int)"/> with an infinite timeout.
        /// </summary>
        public void Dispose()
        {
            _coreCluster.Dispose();
        }

        /// <summary>
        /// Returns all known hosts of this cluster.
        /// </summary>
        /// <returns></returns>
        public ICollection<Host> AllHosts()
        {
            return _coreCluster.AllHosts();
        }

        ISession ICluster.Connect()
        {
            return Connect();
        }

        ISession ICluster.Connect(string keyspace)
        {
            return Connect(keyspace);
        }

        Task<ISession> ICluster.ConnectAsync()
        {
            return _coreCluster.ConnectAsync();
        }

        Task<ISession> ICluster.ConnectAsync(string keyspace)
        {
            return _coreCluster.ConnectAsync(keyspace);
        }

        /// <summary>
        /// Creates a new <see cref="IDseSession"/> for this cluster.
        /// </summary>
        public IDseSession Connect()
        {
            return Connect(Configuration.CassandraConfiguration.ClientOptions.DefaultKeyspace);
        }

        /// <summary>
        /// Creates a new <see cref="IDseSession"/> for this cluster to a specific keyspaces.
        /// </summary>
        public IDseSession Connect(string keyspace)
        {
            return TaskHelper.WaitToComplete(ConnectAsync(keyspace));
        }

        /// <summary>
        /// Asynchronously creates a new session on this cluster.
        /// </summary>
        public Task<IDseSession> ConnectAsync()
        {
            return ConnectAsync(Configuration.CassandraConfiguration.ClientOptions.DefaultKeyspace);
        }

        /// <summary>
        /// Asynchronously creates a new session on this cluster and using a keyspace an existing keyspace.
        /// </summary>
        /// <param name="keyspace">Case-sensitive keyspace name to use</param>
        public async Task<IDseSession> ConnectAsync(string keyspace)
        {
            return await _coreCluster.ConnectAsync(_dseSessionFactory, keyspace).ConfigureAwait(false);
        }

        /// <summary>
        /// Get a host instance for a given endpoint.
        /// </summary>
        public Host GetHost(IPEndPoint address)
        {
            return _coreCluster.GetHost(address);
        }

        ICollection<Host> ICluster.GetReplicas(byte[] partitionKey)
        {
            return _coreCluster.GetReplicas(partitionKey);
        }

        /// <summary>
        /// Gets a collection of replicas for a given partitionKey on a given keyspace.
        /// </summary>
        /// <param name="keyspace">The keyspace name.</param>
        /// <param name="partitionKey">Byte array representing the partition key.</param>
        /// <returns></returns>
        public ICollection<Host> GetReplicas(string keyspace, byte[] partitionKey)
        {
            return _coreCluster.GetReplicas(keyspace, partitionKey);
        }

        private void OnCoreHostRemoved(Host h)
        {
            if (HostRemoved != null)
            {
                HostRemoved(h);
            }
        }

        private void OnCoreHostAdded(Host h)
        {
            if (HostAdded != null)
            {
                HostAdded(h);
            }
        }

        /// <inheritdoc />
        public void Shutdown(int timeoutMs = -1)
        {
            _coreCluster.Shutdown(timeoutMs);
        }

        /// <inheritdoc />
        public bool AnyOpenConnections(Host host)
        {
            return _coreCluster.AnyOpenConnections(host);
        }

        /// <inheritdoc />
        IControlConnection IInternalCluster.GetControlConnection()
        {
            return _coreCluster.GetControlConnection();
        }

        /// <inheritdoc />
        ConcurrentDictionary<byte[], PreparedStatement> IInternalCluster.PreparedQueries => _coreCluster.PreparedQueries;

        /// <inheritdoc />
        Task<PreparedStatement> IInternalCluster.Prepare(IInternalSession session, Serializer serializer, InternalPrepareRequest request)
        {
            return _coreCluster.Prepare(session, serializer, request);
        }

        Task<TSession> IInternalCluster.ConnectAsync<TSession>(ISessionFactory<TSession> sessionFactory, string keyspace)
        {
            return _coreCluster.ConnectAsync(sessionFactory, keyspace);
        }

        public Task<bool> OnInitializeAsync()
        {
            return _coreCluster.OnInitializeAsync();
        }

        public Task<bool> OnShutdownAsync(int timeoutMs = Timeout.Infinite)
        {
            return _coreCluster.OnShutdownAsync(timeoutMs);
        }

        public IReadOnlyDictionary<string, IEnumerable<IPEndPoint>> GetResolvedEndpoints()
        {
            return _coreCluster.GetResolvedEndpoints();
        }

        /// <inheritdoc />
        public Task ShutdownAsync(int timeout = Timeout.Infinite)
        {
            return _coreCluster.ShutdownAsync(timeout);
        }

        /// <inheritdoc />
        public Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)
        {
            return _coreCluster.RefreshSchemaAsync(keyspace, table);
        }

        /// <inheritdoc />
        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            return _coreCluster.RefreshSchema(keyspace, table);
        }
    }
}