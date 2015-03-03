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

﻿using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Net;
using System.Threading;

namespace Cassandra
{
    /// <summary>
    /// Represents a pool of connections to a host
    /// </summary>
    internal class HostConnectionPool : IDisposable
    {
        private readonly static Logger _logger = new Logger(typeof(HostConnectionPool));
        private ConcurrentBag<Connection> _connections;
        private readonly object _poolCreationLock = new object();
        private int _creating = 0;

        private Configuration Configuration { get; set; }

        /// <summary>
        /// Gets a list of connections already opened to the host
        /// </summary>
        public IEnumerable<Connection> OpenConnections 
        { 
            get
            {
                if (_connections == null)
                {
                    return new Connection[] { };
                }
                return _connections;
            }
        }

        private Host Host { get; set; }

        private HostDistance HostDistance { get; set; }

        public byte ProtocolVersion { get; set; }

        public HostConnectionPool(Host host, HostDistance hostDistance, Configuration configuration)
        {
            Host = host;
            Host.Down += OnHostDown;
            HostDistance = hostDistance;
            Configuration = configuration;
        }

        /// <summary>
        /// Gets an open connection from the host pool (creating if necessary).
        /// It returns null if the load balancing policy didn't allow connections to this host.
        /// </summary>
        public Connection BorrowConnection()
        {
            MaybeCreateCorePool();
            if (_connections.Count == 0)
            {
                //The load balancing policy stated no connections for this host
                return null;
            }
            var connection = _connections.OrderBy(c => c.InFlight).First();
            MaybeSpawnNewConnection(connection.InFlight);
            return connection;
        }

        /// <exception cref="System.Net.Sockets.SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        /// <exception cref="AuthenticationException" />
        /// <exception cref="UnsupportedProtocolVersionException"></exception>
        private Connection CreateConnection()
        {
            _logger.Info("Creating a new connection to the host " + Host.Address.ToString());
            var endpoint = new IPEndPoint(Host.Address, Configuration.ProtocolOptions.Port);
            var c = new Connection(ProtocolVersion, endpoint, Configuration);
            c.Init();
            if (Configuration.PoolingOptions.GetHeartBeatInterval() != null)
            {
                //Heartbeat is enabled, subscribe for possible exceptions
                c.OnIdleRequestException += OnIdleRequestException;   
            }
            return c;
        }

        /// <summary>
        /// Handler that gets invoked when if there is a socket exception when making a heartbeat/idle request
        /// </summary>
        private void OnIdleRequestException(Exception ex)
        {
            Host.SetDown();
        }

        private void OnHostDown(Host h, DateTimeOffset nextTimeUp)
        {
            //Dispose all current connections
            //Connection allows multiple calls to Dispose
            var currentPool = _connections;
            if (currentPool != null)
            {
                foreach (var c in currentPool)
                {
                    c.Dispose();
                }
            }
        }

        /// <summary>
        /// Create the min amount of connections, if the pool is empty
        /// </summary>
        private void MaybeCreateCorePool()
        {
            var coreConnections = Configuration.GetPoolingOptions(ProtocolVersion).GetCoreConnectionsPerHost(HostDistance);
            if (_connections == null || _connections.All(c => c.IsClosed))
            {
                lock(_poolCreationLock)
                {
                    if (!Host.IsConsiderablyUp || (_connections != null && !_connections.All(c => c.IsClosed)))
                    {
                        //While waiting for the lock
                        //The connections have been created
                        //Or the host was set as down again
                        return;
                    }
                    _connections = new ConcurrentBag<Connection>();
                    while (_connections.Count < coreConnections)
                    {
                        try
                        {
                            _connections.Add(CreateConnection());
                        }
                        catch
                        {
                            _logger.Warning(String.Format("Could not create connections to host {0}", Host.Address));
                            if (_connections.Count == 0)
                            {
                                //Leave the pool to its previous state
                                _connections = null;
                            }
                            throw;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Creates a new connection, if the conditions apply
        /// </summary>
        private void MaybeSpawnNewConnection(int inFlight)
        {
            var maxInFlight = Configuration.GetPoolingOptions(ProtocolVersion).GetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance);
            var maxConnections = Configuration.GetPoolingOptions(ProtocolVersion).GetMaxConnectionPerHost(HostDistance);
            if (inFlight > maxInFlight)
            {
                if (_connections.Count >= maxConnections)
                {
                    _logger.Warning("Max amount of connections and max amount of in-flight operations reached");
                    return;
                }
                //Only one creation at a time
                if (Interlocked.Increment(ref _creating) == 1)
                {
                    _connections.Add(CreateConnection());
                }
                Interlocked.Decrement(ref _creating);
            }
        }

        public void Dispose()
        {
            Host.Down -= OnHostDown;
            var connections = _connections;
            if (connections == null)
            {
                return;
            }
            _logger.Info(String.Format("Disposing connection pool to {0}, closing {1} connections.", Host.Address, connections.Count));
            foreach (var c in connections)
            {
                c.Dispose();
            }
            _connections = null;
        }
    }
}
