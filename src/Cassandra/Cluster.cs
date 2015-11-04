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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using Cassandra.Tasks;
using Microsoft.IO;

namespace Cassandra
{
    /// <summary>
    /// Implementation of <see cref="ICluster"/>
    /// </summary>
    /// <inheritdoc />
    public class Cluster : ICluster
    {
        private static int _maxProtocolVersion = 4;
        // ReSharper disable once InconsistentNaming
        private static readonly Logger _logger = new Logger(typeof(Cluster));
        private byte _protocolVersion;
        private readonly ConcurrentBag<Session> _connectedSessions = new ConcurrentBag<Session>();
        private ControlConnection _controlConnection;
        private volatile bool _initialized;
        private volatile Exception _initException;
        private readonly object _initLock = new Object();
        private readonly Metadata _metadata;
        /// <inheritdoc />
        public event Action<Host> HostAdded;
        /// <inheritdoc />
        public event Action<Host> HostRemoved;

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
            if (initializer.ContactPoints.Count == 0)
            {
                throw new ArgumentException("Cannot build a cluster without contact points");
            }

            return new Cluster(initializer.ContactPoints, initializer.GetConfiguration());
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

        /// <summary>
        /// Gets or sets the maximum protocol version used by this driver.
        /// <para>
        /// While property value is maintained for backward-compatibility, 
        /// use <see cref="ProtocolOptions.SetMaxProtocolVersion(byte)"/> to set the maximum protocol version used by the driver.
        /// </para>
        /// </summary>
        public static int MaxProtocolVersion
        {
            get { return _maxProtocolVersion; }
            set { _maxProtocolVersion = value; }
        }

        /// <summary>
        ///  Gets the cluster configuration.
        /// </summary>
        public Configuration Configuration { get; private set; }

        /// <inheritdoc />
        public Metadata Metadata
        {
            get
            {
                Init();
                return _metadata;
            }
        }

        private Cluster(IEnumerable<IPEndPoint> contactPoints, Configuration configuration)
        {
            Configuration = configuration;
            _metadata = new Metadata(configuration);
            foreach (var ep in contactPoints)
            {
                _metadata.AddHost(ep);
            }
        }

        /// <summary>
        /// Initializes once (Thread-safe) the control connection and metadata associated with the Cluster instance
        /// </summary>
        private void Init()
        {
            if (_initialized)
            {
                //It was already initialized
                return;
            }
            lock (_initLock)
            {
                if (_initialized)
                {
                    //It was initialized when waiting on the lock
                    return;
                }
                if (_initException != null)
                {
                    //There was an exception that is not possible to recover from
                    throw _initException;
                }
                _protocolVersion = (byte) MaxProtocolVersion;
                if (Configuration.ProtocolOptions.MaxProtocolVersion != null &&
                    Configuration.ProtocolOptions.MaxProtocolVersion < MaxProtocolVersion
                    )
                {
                    _protocolVersion = Configuration.ProtocolOptions.MaxProtocolVersion.Value;
                }
                //create the buffer pool with 16KB for small buffers and 256Kb for large buffers.
                Configuration.BufferPool = new RecyclableMemoryStreamManager(16 * 1024, 256 * 1024, ProtocolOptions.MaximumFrameLength);
                _controlConnection = new ControlConnection(_protocolVersion, Configuration, _metadata);
                _metadata.ControlConnection = _controlConnection;
                try
                {
                    _controlConnection.Init();
                    _protocolVersion = _controlConnection.ProtocolVersion;
                    //Initialize policies
                    Configuration.Policies.LoadBalancingPolicy.Initialize(this);
                    Configuration.Policies.SpeculativeExecutionPolicy.Initialize(this);
                }
                catch (NoHostAvailableException)
                {
                    //No host available now, maybe later it can recover from
                    throw;
                }
                catch (Exception ex)
                {
                    //There was an error that the driver is not able to recover from
                    //Store the exception for the following times
                    _initException = ex;
                    //Throw the actual exception for the first time
                    throw;
                }
                Configuration.Timer = new HashedWheelTimer();
                _logger.Info("Cluster Connected using binary protocol version: [" + _protocolVersion + "]");
                _initialized = true;
                _metadata.Hosts.Added += OnHostAdded;
                _metadata.Hosts.Removed += OnHostRemoved;
            }
        }

        /// <inheritdoc />
        public ICollection<Host> AllHosts()
        {
            //Do not connect at first
            return _metadata.AllHosts();
        }

        /// <summary>
        /// Creates a new session on this cluster.
        /// </summary>
        public ISession Connect()
        {
            return Connect(Configuration.ClientOptions.DefaultKeyspace);
        }

        /// <summary>
        /// Creates a new session on this cluster and using a keyspace an existing keyspace.
        /// </summary>
        /// <param name="keyspace">Case-sensitive keyspace name to use</param>
        public ISession Connect(string keyspace)
        {
            Init();
            var session = new Session(this, Configuration, keyspace, _protocolVersion);
            session.Init();
            _connectedSessions.Add(session);
            _logger.Info("Session connected ({0})", session.GetHashCode());
            return session;
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
            var session = Connect(null);
            session.CreateKeyspaceIfNotExists(Configuration.ClientOptions.DefaultKeyspace, replication, durableWrites);
            session.ChangeKeyspace(Configuration.ClientOptions.DefaultKeyspace);
            return session;
        }

        public void Dispose()
        {
            Shutdown();
        }

        /// <inheritdoc />
        public Host GetHost(IPEndPoint address)
        {
            return Metadata.GetHost(address);
        }

        /// <inheritdoc />
        public ICollection<Host> GetReplicas(byte[] partitionKey)
        {
            return Metadata.GetReplicas(partitionKey);
        }

        /// <inheritdoc />
        public ICollection<Host> GetReplicas(string keyspace, byte[] partitionKey)
        {
            return Metadata.GetReplicas(keyspace, partitionKey);
        }

        private void OnHostRemoved(Host h)
        {
            if (HostRemoved != null)
            {
                HostRemoved(h);
            }
        }

        private void OnHostAdded(Host h)
        {
            if (HostAdded != null)
            {
                HostAdded(h);
            }
        }

        /// <summary>
        /// Updates cluster metadata for a given keyspace or keyspace table
        /// </summary>
        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            return Metadata.RefreshSchema(keyspace, table);
        }

        /// <inheritdoc />
        public void Shutdown(int timeoutMs = Timeout.Infinite)
        {
            if (!_initialized)
            {
                return;
            }
            Session session;
            while (_connectedSessions.TryTake(out session))
            {
                session.WaitForAllPendingActions(timeoutMs);
                session.Dispose();
            }
            _metadata.ShutDown(timeoutMs);
            _controlConnection.Dispose();
            Configuration.Timer.Dispose();
            Configuration.Policies.SpeculativeExecutionPolicy.Dispose();
            _logger.Info("Cluster [" + _metadata.ClusterName + "] has been shut down.");
        }
    }
}
