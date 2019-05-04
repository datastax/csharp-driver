//
//  Copyright (C) 2016-2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

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
        private readonly Cluster _coreCluster;
        private readonly DseConfiguration _config;
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
        public Metadata Metadata
        {
            get { return _coreCluster.Metadata; }
        }

        /// <inheritdoc/>
        public DseConfiguration Configuration
        {
            get { return _config; }
        }

        /// <inheritdoc/>
        Configuration ICluster.Configuration
        {
            get { return _config.CassandraConfiguration; }
        }

        /// <summary>
        /// Creates a new <see cref="DseClusterBuilder"/> instance.
        /// </summary>
        public static DseClusterBuilder Builder()
        {
            return new DseClusterBuilder();
        }

        internal DseCluster(Cluster coreCluster, DseConfiguration config)
        {
            _coreCluster = coreCluster;
            _config = config;
            _coreCluster.HostAdded += OnCoreHostAdded;
            _coreCluster.HostRemoved += OnCoreHostRemoved;
            _dseSessionFactory = config.DseSessionFactoryBuilder.BuildWithCluster(this);
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

        /// <summary>
        /// Creates a new <see cref="IDseSession"/> for this cluster.
        /// </summary>
        public IDseSession Connect()
        {
            return Connect(_config.CassandraConfiguration.ClientOptions.DefaultKeyspace);
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
            return ConnectAsync(_config.CassandraConfiguration.ClientOptions.DefaultKeyspace);
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

        /// <summary>
        /// Shutdown this cluster instance. This closes all connections from all the sessions of this instance and
        /// reclaim all resources used by it.
        /// <para>This method has no effect if the cluster has already been shutdown.</para>
        /// </summary>
        public void Shutdown(int timeoutMs = -1)
        {
            _coreCluster.Shutdown(timeoutMs);
        }

        public bool AnyOpenConnections(Host host)
        {
            return _coreCluster.InternalRef.AnyOpenConnections(host);
        }

        /// <inheritdoc />
        IControlConnection IInternalCluster.GetControlConnection()
        {
            return _coreCluster.InternalRef.GetControlConnection();
        }

        /// <inheritdoc />
        ConcurrentDictionary<byte[], PreparedStatement> IInternalCluster.PreparedQueries => _coreCluster.InternalRef.PreparedQueries;

        /// <inheritdoc />
        Task<PreparedStatement> IInternalCluster.Prepare(IInternalSession session, Serializer serializer, InternalPrepareRequest request)
        {
            return _coreCluster.InternalRef.Prepare(session, serializer, request);
        }

        /// <summary>
        /// Shutdown this cluster instance. This closes all connections from all the sessions of this instance and
        /// reclaim all resources used by it.
        /// <para>This method has no effect if the cluster has already been shutdown.</para>
        /// </summary>
        public Task ShutdownAsync(int timeout = Timeout.Infinite)
        {
            return _coreCluster.ShutdownAsync(timeout);
        }
    }
}