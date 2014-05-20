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

using System;

namespace Cassandra
{
    /// <summary>
    ///  Options related to connection pooling. <p> The driver uses connections in an
    ///  asynchronous way. Meaning that multiple requests can be submitted on the same
    ///  connection at the same time. This means that the driver only needs to
    ///  maintain a relatively small number of connections to each Cassandra host.
    ///  These options allow to control how many connections are kept exactly. </p><p> For
    ///  each host, the driver keeps a core amount of connections open at all time
    ///  (<link>PoolingOptions#getCoreConnectionsPerHost</link>). If the utilisation
    ///  of those connections reaches a configurable threshold
    ///  (<link>PoolingOptions#getMaxSimultaneousRequestsPerConnectionTreshold</link>),
    ///  more connections are created up to a configurable maximum number of
    ///  connections (<link>PoolingOptions#getMaxConnectionPerHost</link>). Once more
    ///  than core connections have been created, connections in excess are reclaimed
    ///  if the utilisation of opened connections drops below the configured threshold
    ///  (<link>PoolingOptions#getMinSimultaneousRequestsPerConnectionTreshold</link>).
    ///  </p><p> Each of these parameters can be separately set for <c>Local</c> and
    ///  <c>Remote</c> hosts (<link>HostDistance</link>). For
    ///  <c>Ignored</c> hosts, the default for all those settings is 0 and
    ///  cannot be changed.</p>
    /// </summary>
    public class PoolingOptions
    {
        // Note: we could use an enumMap or similar, but synchronization would
        // be more costly so let's stick to volatile in for now.
        private const int DefaultMinRequests = 25;
        private const int DefaultMaxRequests = 100;

        private const int DefaultCorePoolLocal = 2;
        private const int DefaultCorePoolRemote = 1;

        private const int DefaultMaxPoolLocal = 8;
        private const int DefaultMaxPoolRemote = 2;

        private int _coreConnectionsForLocal = DefaultCorePoolLocal;
        private int _coreConnectionsForRemote = DefaultCorePoolRemote;

        private int _maxConnectionsForLocal = DefaultMaxPoolLocal;
        private int _maxConnectionsForRemote = DefaultMaxPoolRemote;
        private int _maxSimultaneousRequestsForLocal = DefaultMaxRequests;
        private int _maxSimultaneousRequestsForRemote = DefaultMaxRequests;
        private int _minSimultaneousRequestsForLocal = DefaultMinRequests;
        private int _minSimultaneousRequestsForRemote = DefaultMinRequests;

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
        /// <param name="distance"> the <c>HostDistance</c> for which to configure this
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
        ///  Number of simultaneous requests on all connections to an host after which
        ///  more connections are created. <p> If all the connections opened to an host at
        ///  distance <c>* distance</c> connection are handling more than this
        ///  number of simultaneous requests and there is less than
        ///  <link>#getMaxConnectionPerHost</link> connections open to this host, a new
        ///  connection is open. </p><p> Note that a given connection cannot handle more than
        ///  128 simultaneous requests (protocol limitation). </p><p> The default value for
        ///  this option is 100 for <c>Local</c> and <c>Remote</c> hosts.</p>
        /// </summary>
        /// <param name="distance"> the <c>HostDistance</c> for which to return this threshold.</param>
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
        ///  Sets number of simultaneous requests on all connections to an host after
        ///  which more connections are created.
        /// </summary>
        /// <param name="distance"> the <c>HostDistance</c> for which to configure this
        ///  threshold. </param>
        /// <param name="maxSimultaneousRequests"> the value to set. </param>
        /// 
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
        ///  The core number of connections per host. <p> For the provided
        ///  <c>distance</c>, this correspond to the number of connections initially
        ///  created and kept open to each host of that distance.</p>
        /// </summary>
        /// <param name="distance"> the <c>HostDistance</c> for which to return this threshold.
        ///  </param>
        /// 
        /// <returns>the core number of connections per host at distance
        ///  <c>distance</c>.</returns>
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
        ///  Sets the core number of connections per host.
        /// </summary>
        /// <param name="distance"> the <c>HostDistance</c> for which to set this threshold.
        ///  </param>
        /// <param name="coreConnections"> the value to set </param>
        /// 
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
    }
}