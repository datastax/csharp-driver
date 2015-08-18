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
﻿using System.Collections;
﻿using System.Collections.Generic;
using System.Collections.Concurrent;
﻿using System.Diagnostics;
﻿using System.Linq;
using System.Threading;
﻿using System.Threading.Tasks;
﻿using Cassandra.Collections;
﻿using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents a pool of connections to a host
    /// </summary>
    internal class HostConnectionPool : IDisposable
    {
        private readonly static Logger Logger = new Logger(typeof(HostConnectionPool));
        private readonly List<Connection> _connections = new List<Connection>();
        private volatile Task<Connection>[] _openingConnections;
        private readonly SemaphoreSlim _poolModificationSemaphore = new SemaphoreSlim(1);

        private Configuration Configuration { get; set; }

        /// <summary>
        /// Gets a list of connections already opened to the host
        /// </summary>
        public IEnumerable<Connection> OpenConnections 
        { 
            get
            {
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
        public Task<Connection> BorrowConnection()
        {
            return MaybeCreateCorePool().ContinueSync(poolConnections =>
            {
                if (poolConnections.Length == 0)
                {
                    //The load balancing policy stated no connections for this host
                    return null;
                }
                var connection = MinInFlight(poolConnections);
                MaybeSpawnNewConnection(connection.InFlight);
                return connection;
            });
        }

        /// <summary> 
        /// Gets the connection with the minimum number of InFlight requests
        /// </summary>
        public static Connection MinInFlight(IEnumerable<Connection> connections)
        {
            var lastValue = int.MaxValue;
            Connection result = null;
            foreach (var c in connections)
            {
                if (c.InFlight >= lastValue)
                {
                    continue;
                }
                result = c;
                lastValue = c.InFlight;
            }
            return result;
        }

        /// <exception cref="System.Net.Sockets.SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        /// <exception cref="AuthenticationException" />
        /// <exception cref="UnsupportedProtocolVersionException"></exception>
        internal virtual Task<Connection> CreateConnection()
        {
            Logger.Info("Creating a new connection to the host " + Host.Address);
            var c = new Connection(ProtocolVersion, Host.Address, Configuration);
            return c.Open().ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion)
                {
                    if (Configuration.PoolingOptions.GetHeartBeatInterval() != null)
                    {
                        //Heartbeat is enabled, subscribe for possible exceptions
                        c.OnIdleRequestException += OnIdleRequestException;
                    }
                    return c;
                }
                Logger.Info("The connection to {0} could not be opened", Host.Address);
                c.Dispose();
                if (t.Exception != null)
                {
                    Logger.Error(t.Exception.InnerException);
                    throw t.Exception.InnerException;
                }
                throw new TaskCanceledException("The connection creation task was cancelled");
            }, TaskContinuationOptions.ExecuteSynchronously);
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
            foreach (var c in _connections)
            {
                c.Dispose();
            }
        }

        /// <summary>
        /// Create the min amount of connections, if the pool is empty
        /// </summary>
        internal Task<Connection[]> MaybeCreateCorePool()
        {
            var coreConnections = Configuration.GetPoolingOptions(ProtocolVersion).GetCoreConnectionsPerHost(HostDistance);
            if (!_connections.Any(c => c.IsClosed) && _connections.Count >= coreConnections)
            {
                //Pool has the appropriate size
                return TaskHelper.ToTask(_connections.ToArray());
            }
            if (!_poolModificationSemaphore.Wait(0))
            {
                //Couldn't enter semaphore, check if there is a connection available to yield
                var opened = _connections.Where(c => !c.IsClosed).ToArray();
                if (opened.Length > 0)
                {
                    return TaskHelper.ToTask(opened);
                }
                var alreadyOpening = _openingConnections;
                if (alreadyOpening != null && alreadyOpening.Length > 0)
                {
                    return Task.Factory.ContinueWhenAny(alreadyOpening, t =>
                    {
                        if (t.Status == TaskStatus.RanToCompletion)
                        {
                            return new[] { t.Result };
                        }
                        if (t.Exception != null)
                        {
                            throw t.Exception.InnerException;
                        }
                        throw new TaskCanceledException("Could not get an opened connection because the Task was cancelled");
                    }, TaskContinuationOptions.ExecuteSynchronously);
                }
                //There isn't a connection available yet, enter semaphore
                _poolModificationSemaphore.Wait();
            }
            //Semaphore entered
            //Remove closed connections from the pool
            foreach (var c in _connections.Where(c => c.IsClosed))
            {
                _connections.Remove(c);
            }
            var opening = new List<Task<Connection>>();
            if (_openingConnections != null)
            {
                opening.AddRange(_openingConnections);
            }
            while (_connections.Count + opening.Count < coreConnections)
            {
                opening.Add(CreateConnection());
            }
            if (opening.Count == 0)
            {
                if (_connections.Count == 0)
                {
                    return TaskHelper.FromException<Connection[]>(new DriverInternalError("Could not create a connection and no connections found in pool"));
                }
                _poolModificationSemaphore.Release();
                return TaskHelper.ToTask(_connections.ToArray());
            }
            var openingArray = opening.ToArray();
            _openingConnections = openingArray;
            //Clean up when all open task finished
            var allCompleted = Task.Factory.ContinueWhenAll(openingArray, tasks =>
            {
                _connections.AddRange(tasks.Where(t => t.Status == TaskStatus.RanToCompletion).Select(t => t.Result).ToArray());
                if (_connections.Count == coreConnections)
                {
                    Logger.Info("{0} connection(s) to host {1} were created successfully", coreConnections, Host.Address);
                }
                _openingConnections = null;
                var connectionsArray = _connections.ToArray();
                _poolModificationSemaphore.Release();
                if (connectionsArray.Length == 0 && tasks.All(t => t.Status != TaskStatus.RanToCompletion))
                {
                    //Pool could not be created
                    throw new AggregateException(string.Format("Connection pool to host {0} could not be created", Host.Address), 
                        tasks.Where(t => t.Exception != null).Select(t => t.Exception.InnerException));
                }
                return connectionsArray;
            }, TaskContinuationOptions.ExecuteSynchronously);

            //yield the first connection available
            return Task.Factory.ContinueWhenAny(openingArray, t =>
            {
                if (t.Status == TaskStatus.RanToCompletion)
                {
                    return new[] { t.Result };
                }
                if (t.Exception != null)
                {
                    throw t.Exception.InnerException;
                }
                throw new TaskCanceledException("Could not get an opened connection because the Task was cancelled");
            }, TaskContinuationOptions.ExecuteSynchronously)
            .ContinueWith(t =>
            {
                if (t.Status != TaskStatus.RanToCompletion)
                {
                    //The first connection failed
                    //Wait for all to complete
                    return allCompleted;
                }
                return TaskHelper.ToTask(t.Result);
            }).Unwrap();
        }

        /// <summary>
        /// Creates a new connection, if the conditions apply.
        /// Only creates a new connection if there isn't a thread already creating one
        /// </summary>
        internal bool MaybeSpawnNewConnection(int inFlight)
        {
            var maxInFlight = Configuration.GetPoolingOptions(ProtocolVersion).GetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance);
            var maxConnections = Configuration.GetPoolingOptions(ProtocolVersion).GetMaxConnectionPerHost(HostDistance);
            if (inFlight <= maxInFlight)
            {
                return false;
            }
            if (_connections.Count >= maxConnections)
            {
                Logger.Warning("Max amount of connections and max amount of in-flight operations reached");
                return false;
            }
            if (!_poolModificationSemaphore.Wait(0))
            {
                //Other thread is already modifying the pool
                //Don't mind, it will be tried in the following attempts
                return false;
            }
            Logger.Info("Maximum requests per connection threshold reached, creating a new connection");
            //Semaphore entered
            CreateConnection().ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion)
                {
                    _connections.Add(t.Result);   
                }
                _poolModificationSemaphore.Release();
            }, TaskContinuationOptions.ExecuteSynchronously);
            return true;
        }

        public void CheckHealth(Connection c)
        {
            if (c.TimedOutOperations < Configuration.SocketOptions.DefunctReadTimeoutThreshold)
            {
                return;
            }
            //We are in the default thread-pool (non-io thread)
            //Defunct: close it and remove it from the pool
            _poolModificationSemaphore.Wait();
            _connections.Remove(c);
            _poolModificationSemaphore.Release();
            c.Dispose();
        }

        public void Dispose()
        {
            Host.Down -= OnHostDown;
            if (_connections.Count == 0)
            {
                return;
            }
            Logger.Info(String.Format("Disposing connection pool to {0}, closing {1} connections.", Host.Address, _connections.Count));
            foreach (var c in _connections)
            {
                c.Dispose();
            }
        }
    }
}
