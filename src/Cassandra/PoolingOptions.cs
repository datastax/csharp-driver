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

namespace Cassandra
{
    /// <summary>
    /// <para>Represents the options related to connection pooling.</para>
    /// <para>
    /// For each host selected by the load balancing policy, the driver keeps a core amount of 
    /// connections open at all times 
    /// (<see cref="PoolingOptions.GetCoreConnectionsPerHost(HostDistance)"/>).
    /// If the use of those connections reaches a configurable threshold 
    /// (<see cref="PoolingOptions.GetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance)"/>), 
    /// more connections are created up to the configurable maximum number of connections 
    /// (<see cref="PoolingOptions.GetMaxConnectionPerHost(HostDistance)"/>).
    /// </para>
    /// <para>
    /// The driver uses connections in an asynchronous manner and multiple requests can be
    /// submitted on the same connection at the same time without waiting for a response.
    /// This means that the driver only needs to maintain a relatively small number of connections
    /// to each Cassandra host. The <see cref="PoolingOptions"/> allows you to to control how many
    /// connections are kept per host.
    /// </para>
    /// <para>
    /// Each of these parameters can be separately set for <see cref="HostDistance.Local"/> and
    /// <see cref="HostDistance.Remote"/> hosts. For <see cref="HostDistance.Ignored"/> hosts,
    /// the default for all those settings is 0 and cannot be changed.
    /// </para>
    /// <para>
    /// The default amount of connections depend on the Cassandra version of the Cluster, as newer
    /// versions of Cassandra (2.1 and above) support a higher number of in-flight requests.
    /// </para>
    /// <para>For Cassandra 2.1 and above, the default amount of connections per host are:</para>
    /// <list type="bullet">
    /// <item>Local datacenter: 1 core connection per host, with 2 connections as maximum when the simultaneous
    /// requests threshold is reached.</item>
    /// <item>Remote datacenter: 1 core connection per host (being 1 also max).</item>
    /// </list>
    /// <para>For older Cassandra versions (1.2 and 2.0), the default amount of connections per host are:</para>
    /// <list type="bullet">
    /// <item>Local datacenter: 2 core connection per host, with 8 connections as maximum when the simultaneous
    /// requests threshold is reached.</item>
    /// <item>Remote datacenter: 1 core connection per host (being 2 the maximum).</item>
    /// </list>
    /// </summary>
    public class PoolingOptions
    {
        //the defaults target small number concurrent requests (protocol 1 and 2) and multiple connections to a host
        private const int DefaultMinRequests = 25;
        private const int DefaultMaxRequestsThreshold = 128;
        private const int DefaultCorePoolLocal = 2;
        private const int DefaultCorePoolRemote = 1;
        private const int DefaultMaxPoolLocal = 8;
        private const int DefaultMaxPoolRemote = 2;

        /// <summary>
        /// Default maximum amount of requests that can be in-flight on a single connection at the same time after
        /// which the connection will start rejecting requests: 2048.
        /// </summary>
        public const int DefaultMaxRequestsPerConnection = 2048;

        /// <summary>
        /// The default heartbeat interval in milliseconds: 30000.
        /// </summary>
        public const int DefaultHeartBeatInterval = 30000;

        private int _coreConnectionsForLocal = DefaultCorePoolLocal;
        private int _coreConnectionsForRemote = DefaultCorePoolRemote;

        private int _maxConnectionsForLocal = DefaultMaxPoolLocal;
        private int _maxConnectionsForRemote = DefaultMaxPoolRemote;
        private int _maxSimultaneousRequestsForLocal = DefaultMaxRequestsThreshold;
        private int _maxSimultaneousRequestsForRemote = DefaultMaxRequestsThreshold;
        private int _minSimultaneousRequestsForLocal = DefaultMinRequests;
        private int _minSimultaneousRequestsForRemote = DefaultMinRequests;
        private int _heartBeatInterval = DefaultHeartBeatInterval;
        private int _maxRequestsPerConnection = DefaultMaxRequestsPerConnection;
        private bool _warmup = true;

        /// <summary>
        /// DEPRECATED: It will be removed in future versions. Use <see cref="PoolingOptions.Create"/> instead.
        /// <para>
        /// Creates a new instance of <see cref="PoolingOptions"/> using defaults suitable for old server versions
        /// (Apache Cassandra 2.0 and below) for compatibility reasons. It's recommended that you
        /// use <see cref="PoolingOptions.Create"/> providing the server protocol version.
        /// </para>
        /// </summary>
        public PoolingOptions()
        {

        }

        /// <summary>
        ///  Number of simultaneous requests on a connection below which connections in
        ///  excess are reclaimed. <p> If an opened connection to an host at distance
        ///  <c>distance</c> handles less than this number of simultaneous requests
        ///  and there is more than <link>#GetCoreConnectionsPerHost</link> connections
        ///  open to this host, the connection is closed. </p><p> The default value for this
        ///  option is 25 for <c>Local</c> and <c>Remote</c> hosts.</p>
        /// </summary>
        /// <param name="distance"> the <c>HostDistance</c> for which to return this threshold.</param>
        /// <returns>the configured threshold, or the default one if none have been set.</returns>
        public int GetMinSimultaneousRequestsPerConnectionTreshold(HostDistance distance)
        {
            switch (distance)
            {
                case HostDistance.Local:
                    return _minSimultaneousRequestsForLocal;
                case HostDistance.Remote:
                    return _minSimultaneousRequestsForRemote;
                default:
                    return 0;
            }
        }

        /// <summary>
        ///  Sets the number of simultaneous requests on a connection below which
        ///  connections in excess are reclaimed.
        /// </summary>
        /// <param name="distance"> the <see cref="HostDistance"/> for which to configure this
        ///  threshold. </param>
        /// <param name="minSimultaneousRequests"> the value to set. </param>
        /// 
        /// <returns>this <c>PoolingOptions</c>. </returns>
        public PoolingOptions SetMinSimultaneousRequestsPerConnectionTreshold(HostDistance distance, int minSimultaneousRequests)
        {
            switch (distance)
            {
                case HostDistance.Local:
                    _minSimultaneousRequestsForLocal = minSimultaneousRequests;
                    break;
                case HostDistance.Remote:
                    _minSimultaneousRequestsForRemote = minSimultaneousRequests;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Cannot set min streams per connection threshold for " + distance + " hosts");
            }
            return this;
        }

        /// <summary>
        /// <para>
        /// Number of simultaneous requests on each connections to a host after which more
        /// connections are created.
        /// </para>
        /// <para>
        /// If all the connections opened to a host are handling more than this number of simultaneous requests
        /// and there is less than <see cref="GetMaxConnectionPerHost"/> connections open to this host,
        /// a new connection is open.
        /// </para>
        /// </summary>
        /// <param name="distance"> the <see cref="HostDistance"/> for which to return this threshold.</param>
        /// <returns>the configured threshold, or the default one if none have been set.</returns>
        public int GetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance distance)
        {
            switch (distance)
            {
                case HostDistance.Local:
                    return _maxSimultaneousRequestsForLocal;
                case HostDistance.Remote:
                    return _maxSimultaneousRequestsForRemote;
                default:
                    return 0;
            }
        }

        /// <summary>
        /// Sets number of simultaneous requests on all connections to an host after
        /// which more connections are created.
        /// </summary>
        /// <param name="distance">The <see cref="HostDistance"/> for which to configure this
        ///  threshold. </param>
        /// <param name="maxSimultaneousRequests"> the value to set. </param>
        /// <returns>this <c>PoolingOptions</c>. </returns>
        /// <throws name="IllegalArgumentException"> if <c>distance == HostDistance.Ignore</c>.</throws>
        public PoolingOptions SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance distance, int maxSimultaneousRequests)
        {
            switch (distance)
            {
                case HostDistance.Local:
                    _maxSimultaneousRequestsForLocal = maxSimultaneousRequests;
                    break;
                case HostDistance.Remote:
                    _maxSimultaneousRequestsForRemote = maxSimultaneousRequests;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Cannot set max streams per connection threshold for " + distance + " hosts");
            }
            return this;
        }

        /// <summary>
        /// <para>
        /// The core number of connections per host.
        /// </para>
        /// <para>
        /// For the provided <see cref="HostDistance"/>, this correspond to the number of
        /// connections initially created and kept open to each host of that distance.
        /// </para>
        /// </summary>
        /// <param name="distance">The <see cref="HostDistance"/> for which to return this threshold.</param>
        /// <returns>the core number of connections per host at distance <see cref="HostDistance"/>.</returns>
        public int GetCoreConnectionsPerHost(HostDistance distance)
        {
            switch (distance)
            {
                case HostDistance.Local:
                    return _coreConnectionsForLocal;
                case HostDistance.Remote:
                    return _coreConnectionsForRemote;
                default:
                    return 0;
            }
        }

        /// <summary>
        /// Gets whether all connections to hosts in the local datacenter must be opened on connect. Default: true.
        /// </summary>
        public bool GetWarmup()
        {
            return _warmup;
        }

        /// <summary>
        ///  Sets the core number of connections per host.
        /// </summary>
        /// <param name="distance"> the <see cref="HostDistance"/> for which to set this threshold.</param>
        /// <param name="coreConnections"> the value to set </param>
        /// <returns>this <c>PoolingOptions</c>. </returns>
        /// <throws name="IllegalArgumentException"> if <c>distance == HostDistance.Ignored</c>.</throws>
        public PoolingOptions SetCoreConnectionsPerHost(HostDistance distance, int coreConnections)
        {
            switch (distance)
            {
                case HostDistance.Local:
                    _coreConnectionsForLocal = coreConnections;
                    break;
                case HostDistance.Remote:
                    _coreConnectionsForRemote = coreConnections;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Cannot set core connections per host for " + distance + " hosts");
            }
            return this;
        }

        /// <summary>
        ///  The maximum number of connections per host. <p> For the provided
        ///  <c>distance</c>, this correspond to the maximum number of connections
        ///  that can be created per host at that distance.</p>
        /// </summary>
        /// <param name="distance"> the <c>HostDistance</c> for which to return this threshold.
        ///  </param>
        /// 
        /// <returns>the maximum number of connections per host at distance
        ///  <c>distance</c>.</returns>
        public int GetMaxConnectionPerHost(HostDistance distance)
        {
            switch (distance)
            {
                case HostDistance.Local:
                    return _maxConnectionsForLocal;
                case HostDistance.Remote:
                    return _maxConnectionsForRemote;
                default:
                    return 0;
            }
        }

        /// <summary>
        ///  Sets the maximum number of connections per host.
        /// </summary>
        /// <param name="distance"> the <c>HostDistance</c> for which to set this threshold.
        ///  </param>
        /// <param name="maxConnections"> the value to set </param>
        /// 
        /// <returns>this <c>PoolingOptions</c>. </returns>
        public PoolingOptions SetMaxConnectionsPerHost(HostDistance distance, int maxConnections)
        {
            switch (distance)
            {
                case HostDistance.Local:
                    _maxConnectionsForLocal = maxConnections;
                    break;
                case HostDistance.Remote:
                    _maxConnectionsForRemote = maxConnections;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Cannot set max connections per host for " + distance + " hosts");
            }
            return this;
        }

        /// <summary>
        /// Gets the amount of idle time in milliseconds that has to pass
        /// before the driver issues a request on an active connection to avoid
        /// idle time disconnections.
        /// <remarks>
        /// A value of <c>0</c> or <c>null</c> means that the heartbeat
        /// functionality at connection level is disabled.
        /// </remarks>
        /// </summary>
        public int? GetHeartBeatInterval()
        {
            return _heartBeatInterval;
        }

        /// <summary>
        /// Sets the amount of idle time in milliseconds that has to pass
        /// before the driver issues a request on an active connection to avoid
        /// idle time disconnections.
        /// <remarks>
        /// When set to <c>0</c> the heartbeat functionality at connection
        /// level is disabled.
        /// </remarks>
        /// </summary>
        public PoolingOptions SetHeartBeatInterval(int value)
        {
            _heartBeatInterval = value;
            return this;
        }

        /// <summary>
        /// Gets the maximum amount of requests that can be in-flight on a single connection at the same time.
        /// <para>
        /// This setting acts as a fixed maximum, once this value is reached for a host the pool will start
        /// rejecting requests, throwing <see cref="BusyPoolException"/> instances.
        /// </para>
        /// <para>
        /// This setting should not be mistaken with <see cref="GetMaxSimultaneousRequestsPerConnectionTreshold"/>.
        /// </para>
        /// </summary>
        public int GetMaxRequestsPerConnection()
        {
            return _maxRequestsPerConnection;
        }

        /// <summary>
        /// Sets the maximum amount of requests that can be in-flight on a single connection at the same time.
        /// <para>
        /// This setting acts as a fixed maximum, once this value is reached for a host the pool will start
        /// rejecting requests, throwing <see cref="BusyPoolException"/> instances.
        /// </para>
        /// <para>
        /// This setting should not be mistaken with <see cref="SetMaxSimultaneousRequestsPerConnectionTreshold"/>.
        /// </para>
        /// </summary>
        public PoolingOptions SetMaxRequestsPerConnection(int value)
        {
            _maxRequestsPerConnection = value;
            return this;
        }

        /// <summary>
        /// Sets whether all connections to hosts in the local datacenter must be opened on connect. Default: true.
        /// </summary>
        public PoolingOptions SetWarmup(bool doWarmup)
        {
            _warmup = doWarmup;
            return this;
        }

        /// <summary>
        /// Creates a new instance of <see cref="PoolingOptions"/> using the default amount of connections
        /// and settings based on the protocol version.
        /// <para>
        /// For modern server versions (Apache Cassandra 2.1+) the amount of core connections are set to 1,
        /// setting 2 for max local connections.
        /// </para>
        /// </summary>
        /// <returns>A new instance of <see cref="PoolingOptions"/></returns>
        /// <seealso cref="ProtocolVersion"/>
        public static PoolingOptions Create(ProtocolVersion protocolVersion = ProtocolVersion.MaxSupported)
        {
            if (!protocolVersion.Uses2BytesStreamIds())
            {
                //New instance of pooling options with default values
                return new PoolingOptions();
            }
            //New instance of pooling options with default values for high number of concurrent requests
            return new PoolingOptions()
                .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                .SetMaxConnectionsPerHost(HostDistance.Local, 2)
                .SetCoreConnectionsPerHost(HostDistance.Remote, 1)
                .SetMaxConnectionsPerHost(HostDistance.Remote, 1)
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 1500)
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Remote, 1500);
        }
    }
}
