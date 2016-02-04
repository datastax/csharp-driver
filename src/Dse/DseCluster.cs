using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Cassandra;
using Dse.Graph;

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
    /// IDseCluster cluster = DseCluster.Builder().AddContactPoint("192.168.0.1").Build();
    /// IDseSession session = cluster.Connect("ks1");
    /// foreach (Row row in session.Execute(query))
    /// {
    ///     //do something...
    /// }
    /// </example>
    public class DseCluster : IDseCluster
    {
        private readonly ICluster _coreCluster;
        private readonly DseConfiguration _config;
        public event Action<Host> HostAdded;
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

        /// <inheritdoc/>
        public static DseClusterBuilder Builder()
        {
            return new DseClusterBuilder();
        }

        internal DseCluster(ICluster coreCluster, DseConfiguration config)
        {
            _coreCluster = coreCluster;
            _config = config;
            _coreCluster.HostAdded += OnCoreHostAdded;
            _coreCluster.HostRemoved += OnCoreHostRemoved;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _coreCluster.Dispose();
        }

        /// <inheritdoc/>
        public ICollection<Host> AllHosts()
        {
            return _coreCluster.AllHosts();
        }

        /// <inheritdoc/>
        ISession ICluster.Connect()
        {
            return Connect();
        }

        /// <inheritdoc/>
        ISession ICluster.Connect(string keyspace)
        {
            return Connect(keyspace);
        }

        /// <inheritdoc/>
        public IDseSession Connect()
        {
            return new DseSession(_coreCluster.Connect());
        }

        /// <inheritdoc/>
        public IDseSession Connect(string keyspace)
        {
            return new DseSession(_coreCluster.Connect(keyspace));
        }

        /// <inheritdoc/>
        public Host GetHost(IPEndPoint address)
        {
            return _coreCluster.GetHost(address);
        }

        /// <inheritdoc/>
        public ICollection<Host> GetReplicas(byte[] partitionKey)
        {
            return _coreCluster.GetReplicas(partitionKey);
        }

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public void Shutdown(int timeoutMs = -1)
        {
            _coreCluster.Shutdown(timeoutMs);
        }
    }
}
