//
//      Copyright (C) 2012-2016 DataStax Inc.
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
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Collections;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Implementation of <see cref="ICluster"/>
    /// </summary>
    /// <inheritdoc />
    public class Cluster : ICluster
    {
        private static ProtocolVersion _maxProtocolVersion = ProtocolVersion.MaxSupported;
        // ReSharper disable once InconsistentNaming
        private static readonly Logger _logger = new Logger(typeof(Cluster));
        private readonly CopyOnWriteList<Session> _connectedSessions = new CopyOnWriteList<Session>();
        private readonly ControlConnection _controlConnection;
        private volatile bool _initialized;
        private volatile Exception _initException;
        private readonly SemaphoreSlim _initLock = new SemaphoreSlim(1, 1);

        private readonly Metadata _metadata;
        private readonly Serializer _serializer;

        /// <inheritdoc />
        public event Action<Host> HostAdded;
        /// <inheritdoc />
        public event Action<Host> HostRemoved;

        /// <summary>
        /// Gets the control connection used by the cluster
        /// </summary>
        internal ControlConnection GetControlConnection()
        {
            return _controlConnection;
        }
        
        /// <summary>
        /// Gets the the prepared statements cache
        /// </summary>
        internal ConcurrentDictionary<byte[], PreparedStatement> PreparedQueries { get; } 
            = new ConcurrentDictionary<byte[], PreparedStatement>(new ByteArrayComparer());

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
            return BuildFrom(initializer, null);
        }

        internal static Cluster BuildFrom(IInitializer initializer, ICollection<string> hostNames)
        {
            hostNames = hostNames ?? new string[0];
            if (initializer.ContactPoints.Count == 0 && hostNames.Count == 0)
            {
                throw new ArgumentException("Cannot build a cluster without contact points");
            }

            return new Cluster(initializer.ContactPoints.Cast<object>().Concat(hostNames), initializer.GetConfiguration());
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
        /// use <see cref="ProtocolOptions.SetMaxProtocolVersion(ProtocolVersion)"/> to set the maximum protocol version used by the driver.
        /// </para>
        /// <para>
        /// Protocol version used can not be higher than <see cref="ProtocolVersion.MaxSupported"/>.
        /// </para>
        /// </summary>
        public static int MaxProtocolVersion
        {
            get { return (int)_maxProtocolVersion; }
            set
            {
                if (value > (int)ProtocolVersion.MaxSupported)
                {
                    // Ignore
                    return;
                }
                _maxProtocolVersion = (ProtocolVersion) value;
            }
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
                TaskHelper.WaitToComplete(Init());
                return _metadata;
            }
        }

        private Cluster(IEnumerable<object> contactPoints, Configuration configuration)
        {
            Configuration = configuration;
            _metadata = new Metadata(configuration);
            TaskHelper.WaitToComplete(AddHosts(contactPoints));
            var protocolVersion = _maxProtocolVersion;
            if (Configuration.ProtocolOptions.MaxProtocolVersionValue != null &&
                Configuration.ProtocolOptions.MaxProtocolVersionValue.Value.IsSupported())
            {
                protocolVersion = Configuration.ProtocolOptions.MaxProtocolVersionValue.Value;
            }
            _controlConnection = new ControlConnection(protocolVersion, Configuration, _metadata);
            _metadata.ControlConnection = _controlConnection;
            _serializer = _controlConnection.Serializer;
        }

        /// <summary>
        /// Adds contact points as hosts and resolving host names if necessary.
        /// </summary>
        /// <exception cref="NoHostAvailableException">When no host can be resolved and no other contact point is an address</exception>
        private async Task AddHosts(IEnumerable<object> contactPoints)
        {
            var hostNames = new List<string>();
            foreach (var contactPoint in contactPoints)
            {
                if (contactPoint is IPEndPoint endpoint)
                {
                    _metadata.AddHost(endpoint);
                    continue;
                }

                if (!(contactPoint is string contactPointText))
                {
                    throw new InvalidOperationException("Contact points should be either string or IPEndPoint instances");
                }

                if (IPAddress.TryParse(contactPointText, out var ipAddress))
                {
                    _metadata.AddHost(new IPEndPoint(ipAddress, Configuration.ProtocolOptions.Port));
                    continue;
                }

                hostNames.Add(contactPointText);
                IPHostEntry hostEntry = null;
                try
                {
                    hostEntry = await Dns.GetHostEntryAsync(contactPointText).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    _logger.Warning($"Host '{contactPointText}' could not be resolved");
                }

                if (hostEntry != null)
                {
                    foreach (var resolvedAddress in hostEntry.AddressList)
                    {
                        _metadata.AddHost(new IPEndPoint(resolvedAddress, Configuration.ProtocolOptions.Port));
                    }                    
                }
            }

            if (_metadata.Hosts.Count == 0)
            {
                throw new NoHostAvailableException($"No host name could be resolved, attempted: {string.Join(", ", hostNames)}");                
            }
        }

        /// <summary>
        /// Initializes once (Thread-safe) the control connection and metadata associated with the Cluster instance
        /// </summary>
        private async Task Init()
        {
            if (_initialized)
            {
                //It was already initialized
                return;
            }
            await _initLock.WaitAsync().ConfigureAwait(false);
            try
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
                try
                {
                    // Only abort the async operations when at least twice the time for ConnectTimeout per host passed
                    var initialAbortTimeout = Configuration.SocketOptions.ConnectTimeoutMillis * 2 *
                                              _metadata.Hosts.Count;
                    initialAbortTimeout = Math.Max(initialAbortTimeout, ControlConnection.MetadataAbortTimeout);
                    await _controlConnection.Init().WaitToCompleteAsync(initialAbortTimeout).ConfigureAwait(false);

                    // Initialize policies
                    Configuration.Policies.LoadBalancingPolicy.Initialize(this);
                    Configuration.Policies.SpeculativeExecutionPolicy.Initialize(this);
                    Configuration.Policies.InitializeRetryPolicy(this);
                }
                catch (NoHostAvailableException)
                {
                    //No host available now, maybe later it can recover from
                    throw;
                }
                catch (TimeoutException ex)
                {
                    _initException = ex;
                    throw new TimeoutException(
                        "Cluster initialization was aborted after timing out. This mechanism is put in place to" +
                        " avoid blocking the calling thread forever. This usually caused by a networking issue" +
                        " between the client driver instance and the cluster.", ex);
                }
                catch (Exception ex)
                {
                    //There was an error that the driver is not able to recover from
                    //Store the exception for the following times
                    _initException = ex;
                    //Throw the actual exception for the first time
                    throw;
                }
                _logger.Info("Cluster Connected using binary protocol version: [" + _serializer.ProtocolVersion + "]");
                _initialized = true;
                _metadata.Hosts.Added += OnHostAdded;
                _metadata.Hosts.Removed += OnHostRemoved;
                _metadata.Hosts.Up += OnHostUp;
            }
            finally
            {
                _initLock.Release();
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
        /// Creates a new session on this cluster.
        /// </summary>
        public Task<ISession> ConnectAsync()
        {
            return ConnectAsync(Configuration.ClientOptions.DefaultKeyspace);
        }

        /// <summary>
        /// Creates a new session on this cluster and using a keyspace an existing keyspace.
        /// </summary>
        /// <param name="keyspace">Case-sensitive keyspace name to use</param>
        public ISession Connect(string keyspace)
        {
            return TaskHelper.WaitToComplete(ConnectAsync(keyspace));
        }

        /// <summary>
        /// Creates a new session on this cluster and using a keyspace an existing keyspace.
        /// </summary>
        /// <param name="keyspace">Case-sensitive keyspace name to use</param>
        public async Task<ISession> ConnectAsync(string keyspace)
        {
            await Init().ConfigureAwait(false);
            var session = new Session(this, Configuration, keyspace, _serializer);
            await session.Init().ConfigureAwait(false);
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

        internal bool AnyOpenConnections(Host host)
        {
            return _connectedSessions.Any(session => session.HasConnections(host));
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

        private void OnHostUp(Host h)
        {
            if (!Configuration.QueryOptions.IsReprepareOnUp())
            {
                return;
            }
            // We should prepare all current queries on the host
            PrepareHandler.PrepareAllQueries(h, PreparedQueries.Values, _connectedSessions).Forget();
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
            ShutdownAsync(timeoutMs).Wait();
        }

        /// <inheritdoc />
        public async Task ShutdownAsync(int timeoutMs = Timeout.Infinite)
        {
            if (!_initialized)
            {
                return;
            }
            var sessions = _connectedSessions.ClearAndGet();
            try
            {
                var task = Task.Run(() =>
                {
                    foreach (var s in sessions)
                    {
                        s.Dispose();
                    }
                }).WaitToCompleteAsync(timeoutMs);
                await task.ConfigureAwait(false);
            }
            catch (AggregateException ex)
            {
                if (ex.InnerExceptions.Count == 1)
                {
                    throw ex.InnerExceptions[0];
                }
                throw;
            }
            _metadata.ShutDown(timeoutMs);
            _controlConnection.Dispose();
            Configuration.Timer.Dispose();
            Configuration.Policies.SpeculativeExecutionPolicy.Dispose();
            _logger.Info("Cluster [" + _metadata.ClusterName + "] has been shut down.");
        }

        /// <summary>
        /// Helper method to retrieve the distance from LoadBalancingPolicy and set it at Host level.
        /// Once ProfileManager is implemented, this logic will be part of it.
        /// </summary>
        internal static HostDistance RetrieveDistance(Host host, ILoadBalancingPolicy lbp)
        {
            var distance = lbp.Distance(host);
            host.SetDistance(distance);
            return distance;
        }
    }
}
