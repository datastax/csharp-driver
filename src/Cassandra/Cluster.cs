//
//      Copyright (C) DataStax Inc.
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
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Collections;
using Cassandra.Connections;
using Cassandra.ExecutionProfiles;
using Cassandra.Helpers;
using Cassandra.ProtocolEvents;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Implementation of <see cref="ICluster"/>
    /// </summary>
    /// <inheritdoc />
    public class Cluster : IInternalCluster
    {
        private static ProtocolVersion _maxProtocolVersion = ProtocolVersion.MaxSupported;
        // ReSharper disable once InconsistentNaming
        private static readonly Logger _logger = new Logger(typeof(Cluster));
        private readonly CopyOnWriteList<IInternalSession> _connectedSessions = new CopyOnWriteList<IInternalSession>();
        private readonly IControlConnection _controlConnection;
        private volatile bool _initialized;
        private volatile Exception _initException;
        private readonly SemaphoreSlim _initLock = new SemaphoreSlim(1, 1);

        private readonly Metadata _metadata;
        private readonly Serializer _serializer;
        private readonly ISessionFactory<IInternalSession> _sessionFactory;
        private readonly IClusterLifecycleManager _lifecycleManager;
        private readonly IProtocolEventDebouncer _protocolEventDebouncer;

        /// <inheritdoc />
        public event Action<Host> HostAdded;
        /// <inheritdoc />
        public event Action<Host> HostRemoved;
        
        internal IInternalCluster InternalRef => this;

        /// <inheritdoc />
        IControlConnection IInternalCluster.GetControlConnection()
        {
            return _controlConnection;
        }

        /// <inheritdoc />
        ConcurrentDictionary<byte[], PreparedStatement> IInternalCluster.PreparedQueries { get; } 
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
            return BuildFrom(initializer, null, null, null);
        }
        
        internal static Cluster BuildFrom(IInitializer initializer, IReadOnlyList<string> hostNames)
        {
            return BuildFrom(initializer, hostNames, null, null);
        }

        internal static Cluster BuildFrom(IInitializer initializer, IReadOnlyList<string> hostNames, Configuration config, IClusterLifecycleManager manager)
        {
            hostNames = hostNames ?? new string[0];
            if (initializer.ContactPoints.Count == 0 && hostNames.Count == 0)
            {
                throw new ArgumentException("Cannot build a cluster without contact points");
            }

            return new Cluster(
                initializer.ContactPoints.Cast<object>().Concat(hostNames), 
                config ?? initializer.GetConfiguration(), 
                manager);
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

        private Cluster(IEnumerable<object> contactPoints, Configuration configuration, IClusterLifecycleManager lifecycleManager)
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

            _protocolEventDebouncer = new ProtocolEventDebouncer(
                configuration.TimerFactory,
                TimeSpan.FromMilliseconds(configuration.MetadataSyncOptions.RefreshSchemaDelayIncrement),
                TimeSpan.FromMilliseconds(configuration.MetadataSyncOptions.MaxTotalRefreshSchemaDelay));
            _controlConnection = configuration.ControlConnectionFactory.Create(_protocolEventDebouncer, protocolVersion, Configuration, _metadata);
            _metadata.ControlConnection = _controlConnection;
            _serializer = _controlConnection.Serializer;
            _sessionFactory = configuration.SessionFactoryBuilder.BuildWithCluster(this);
            _lifecycleManager = lifecycleManager ?? new ClusterLifecycleManager(this);
        }

        /// <summary>
        /// Adds contact points as hosts and resolving host names if necessary.
        /// </summary>
        /// <exception cref="NoHostAvailableException">When no host can be resolved and no other contact point is an address</exception>
        private async Task AddHosts(IEnumerable<object> contactPoints)
        {
            var resolvedContactPoints = new Dictionary<string, ICollection<IPEndPoint>>();
            var hostNames = new List<string>();
            foreach (var contactPoint in contactPoints)
            {
                if (contactPoint is IPEndPoint endpoint)
                {
                    resolvedContactPoints.CreateOrAdd(endpoint.ToString(), endpoint);
                    _metadata.AddHost(endpoint);
                    continue;
                }

                if (!(contactPoint is string contactPointText))
                {
                    throw new InvalidOperationException("Contact points should be either string or IPEndPoint instances");
                }

                if (IPAddress.TryParse(contactPointText, out var ipAddress))
                {
                    var ipEndpoint = new IPEndPoint(ipAddress, Configuration.ProtocolOptions.Port);
                    resolvedContactPoints.CreateOrAdd(contactPointText, ipEndpoint);
                    _metadata.AddHost(ipEndpoint);
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

                if (hostEntry != null && hostEntry.AddressList.Length > 0)
                {
                    foreach (var resolvedAddress in hostEntry.AddressList)
                    {
                        var ipEndpoint = new IPEndPoint(resolvedAddress, Configuration.ProtocolOptions.Port);
                        _metadata.AddHost(ipEndpoint);
                        resolvedContactPoints.CreateOrAdd(contactPointText, ipEndpoint);
                    }                    
                }
                else
                {
                    resolvedContactPoints.CreateIfDoesNotExist(contactPointText);
                }
            }

            _metadata.SetResolvedContactPoints(resolvedContactPoints.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.AsEnumerable()));

            if (_metadata.Hosts.Count == 0)
            {
                throw new NoHostAvailableException($"No host name could be resolved, attempted: {string.Join(", ", hostNames)}");                
            }
        }

        /// <summary>
        /// Initializes once (Thread-safe) the control connection and metadata associated with the Cluster instance
        /// </summary>
        private Task Init()
        {
            return _lifecycleManager.InitializeAsync();
        }

        private static string GetAssemblyInfo()
        {
            var assembly = typeof(ISession).GetTypeInfo().Assembly;
            var info = FileVersionInfo.GetVersionInfo(assembly.Location);
            return $"{info.ProductName} v{info.FileVersion}";
        }

        internal IReadOnlyDictionary<string, IEnumerable<IPEndPoint>> GetResolvedEndpoints()
        {
            return _metadata.ResolvedContactPoints;
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
            return await InternalRef.ConnectAsync(_sessionFactory, keyspace).ConfigureAwait(false);
        }

        async Task<TSession> IInternalCluster.ConnectAsync<TSession>(ISessionFactory<TSession> sessionFactory, string keyspace)
        {
            await Init().ConfigureAwait(false);
            var session = await sessionFactory.CreateSessionAsync(keyspace, _serializer).ConfigureAwait(false);
            await session.Init().ConfigureAwait(false);
            _connectedSessions.Add(session);
            _logger.Info("Session connected ({0})", session.GetHashCode());
            return session;
        }

        /// <inheritdoc />
        async Task<bool> IInternalCluster.OnInitializeAsync()
        {
            if (_initialized)
            {
                //It was already initialized
                return false;
            }
            await _initLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_initialized)
                {
                    //It was initialized when waiting on the lock
                    return false;
                }
                if (_initException != null)
                {
                    //There was an exception that is not possible to recover from
                    throw _initException;
                }
                _logger.Info("Connecting to cluster using {0}", GetAssemblyInfo());
                try
                {
                    // Only abort the async operations when at least twice the time for ConnectTimeout per host passed
                    var initialAbortTimeout = Configuration.SocketOptions.ConnectTimeoutMillis * 2 * _metadata.Hosts.Count;
                    initialAbortTimeout = Math.Max(initialAbortTimeout, ControlConnection.MetadataAbortTimeout);
                    await _controlConnection.Init().WaitToCompleteAsync(initialAbortTimeout).ConfigureAwait(false);
                    
                    // Initialize policies
                    var loadBalancingPolicies = new HashSet<ILoadBalancingPolicy>(new ReferenceEqualityComparer<ILoadBalancingPolicy>());
                    var speculativeExecutionPolicies = new HashSet<ISpeculativeExecutionPolicy>(new ReferenceEqualityComparer<ISpeculativeExecutionPolicy>());
                    foreach (var options in Configuration.RequestOptions.Values)
                    {
                        loadBalancingPolicies.Add(options.LoadBalancingPolicy);
                        speculativeExecutionPolicies.Add(options.SpeculativeExecutionPolicy);
                    }

                    loadBalancingPolicies.Add(Configuration.Policies.LoadBalancingPolicy);
                    speculativeExecutionPolicies.Add(Configuration.Policies.SpeculativeExecutionPolicy);

                    foreach (var lbp in loadBalancingPolicies)
                    {
                        lbp.Initialize(this);
                    }

                    foreach (var sep in speculativeExecutionPolicies)
                    {
                        sep.Initialize(this);
                    }
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

            return true;
        }

        /// <inheritdoc />
        async Task<bool> IInternalCluster.OnShutdownAsync(int timeoutMs)
        {
            if (!_initialized)
            {
                return false;
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
            _protocolEventDebouncer.Dispose();
            _controlConnection.Dispose();
            Configuration.Timer.Dispose();
            
            // Dispose policies
            var speculativeExecutionPolicies = new HashSet<ISpeculativeExecutionPolicy>(new ReferenceEqualityComparer<ISpeculativeExecutionPolicy>());
            foreach (var options in Configuration.RequestOptions.Values)
            {
                speculativeExecutionPolicies.Add(options.SpeculativeExecutionPolicy);
            }

            speculativeExecutionPolicies.Add(Configuration.Policies.SpeculativeExecutionPolicy);
            foreach (var sep in speculativeExecutionPolicies)
            {
                sep.Dispose();
            }

            return true;
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

        bool IInternalCluster.AnyOpenConnections(Host host)
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

        private async void OnHostUp(Host h)
        {
            try
            {
                if (!Configuration.QueryOptions.IsReprepareOnUp())
                {
                    return;
                }

                // We should prepare all current queries on the host
                await PrepareAllQueries(h).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Cluster._logger.Error(
                    "An exception was thrown when preparing all queries on a host ({0}) " +
                    "that came UP:" + Environment.NewLine + "{1}", h?.Address?.ToString(), ex.ToString());
            }
        }

        /// <inheritdoc />
        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            return Metadata.RefreshSchema(keyspace, table);
        }

        /// <inheritdoc />
        public Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)
        {
            return Metadata.RefreshSchemaAsync(keyspace, table);
        }

        /// <inheritdoc />
        public void Shutdown(int timeoutMs = Timeout.Infinite)
        {
            ShutdownAsync(timeoutMs).Wait();
        }

        /// <inheritdoc />
        public Task ShutdownAsync(int timeoutMs = Timeout.Infinite)
        {
            return _lifecycleManager.ShutdownAsync(timeoutMs);
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

        /// <inheritdoc />
        async Task<PreparedStatement> IInternalCluster.Prepare(
            IInternalSession session, Serializer serializer, InternalPrepareRequest request)
        {
            var lbp = session.Cluster.Configuration.DefaultRequestOptions.LoadBalancingPolicy;
            var handler = InternalRef.Configuration.PrepareHandlerFactory.Create(serializer);
            var ps = await handler.Prepare(request, session, lbp.NewQueryPlan(session.Keyspace, null).GetEnumerator()).ConfigureAwait(false);
            var psAdded = InternalRef.PreparedQueries.GetOrAdd(ps.Id, ps);
            if (ps != psAdded)
            {
                PrepareHandler.Logger.Warning("Re-preparing already prepared query is generally an anti-pattern and will likely " +
                               "affect performance. Consider preparing the statement only once. Query='{0}'", ps.Cql);
                ps = psAdded;
            }

            return ps;
        }
        
        private async Task PrepareAllQueries(Host host)
        {
            ICollection<PreparedStatement> preparedQueries = InternalRef.PreparedQueries.Values;
            IEnumerable<IInternalSession> sessions = _connectedSessions;

            if (preparedQueries.Count == 0)
            {
                return;
            }
            // Get the first connection for that host, in any of the existings connection pool
            var connection = sessions.SelectMany(s => s.GetExistingPool(host.Address)?.ConnectionsSnapshot)
                                     .FirstOrDefault();
            if (connection == null)
            {
                PrepareHandler.Logger.Info($"Could not re-prepare queries on {host.Address} as there wasn't an open connection to" +
                            " the node");
                return;
            }
            PrepareHandler.Logger.Info($"Re-preparing {preparedQueries.Count} queries on {host.Address}");
            var tasks = new List<Task>(preparedQueries.Count);
            using (var semaphore = new SemaphoreSlim(64, 64))
            {
                foreach (var query in preparedQueries.Select(ps => ps.Cql))
                {
                    var request = new InternalPrepareRequest(query);
                    await semaphore.WaitAsync().ConfigureAwait(false);

                    async Task SendSingle()
                    {
                        try
                        {
                            await connection.Send(request).ConfigureAwait(false);
                        }
                        finally
                        {
                            // ReSharper disable once AccessToDisposedClosure
                            // There is no risk of being disposed as the list of tasks is awaited upon below
                            semaphore.Release();
                        }
                    }

                    tasks.Add(Task.Run(SendSingle));
                }
                try
                {
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    PrepareHandler.Logger.Error($"There was an error when re-preparing queries on {host.Address}", ex);
                }
            }
        }
    }
}
