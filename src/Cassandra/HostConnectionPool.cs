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
        private const int ConnectionIndexOverflow = int.MaxValue - 100000;
        private readonly static Logger Logger = new Logger(typeof(HostConnectionPool));
        //Safe iteration of connections
        private readonly CopyOnWriteList<Connection> _connections = new CopyOnWriteList<Connection>();
        private volatile Task<Connection>[] _openingConnections;
        private readonly SemaphoreSlim _poolModificationSemaphore = new SemaphoreSlim(1);
        private readonly Host _host;
        private readonly HostDistance _distance;
        private readonly Configuration _config;
        private readonly HashedWheelTimer _timer;
        private int _connectionIndex;
        private int _hostDownFlag;
        private volatile HashedWheelTimer.ITimeout _timeout;

        /// <summary>
        /// Gets a list of connections already opened to the host
        /// </summary>
        public IEnumerable<Connection> OpenConnections 
        { 
            get { return _connections; }
        }

        public byte ProtocolVersion { get; set; }

        public HostConnectionPool(Host host, HostDistance distance, Configuration config)
        {
            _host = host;
            _host.Down += OnHostDown;
            _host.Up += OnHostUp;
            _distance = distance;
            _config = config;
            _timer = config.Timer;
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
                var connection = MinInFlight(poolConnections, ref _connectionIndex);
                MaybeSpawnNewConnection(connection.InFlight);
                return connection;
            });
        }

        /// <summary>
        /// Gets the connection with the minimum number of InFlight requests.
        /// Only checks for index + 1 and index, to avoid a loop of all connections.
        /// </summary>
        public static Connection MinInFlight(Connection[] connections, ref int connectionIndex)
        {
            if (connections.Length == 1)
            {
                return connections[0];
            }
            //It is very likely that the amount of InFlight requests per connection is the same
            //Do round robin between connections, skipping connections that have more in flight requests
            var index = Interlocked.Increment(ref connectionIndex);
            if (index > ConnectionIndexOverflow)
            {
                //Overflow protection, not exactly thread-safe but we can live with it
                Interlocked.Exchange(ref connectionIndex, 0);
            }
            var currentConnection = connections[index % connections.Length];
            var previousConnection = connections[(index - 1)%connections.Length];
            if (previousConnection.InFlight < currentConnection.InFlight)
            {
                return previousConnection;
            }
            return currentConnection;
        }

        /// <exception cref="System.Net.Sockets.SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        /// <exception cref="AuthenticationException" />
        /// <exception cref="UnsupportedProtocolVersionException"></exception>
        internal virtual Task<Connection> CreateConnection()
        {
            return CreateConnection(CancellationToken.None);
        }

        /// <exception cref="System.Net.Sockets.SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        /// <exception cref="AuthenticationException" />
        /// <exception cref="UnsupportedProtocolVersionException"></exception>
        internal virtual async Task<Connection> CreateConnection(CancellationToken cancellationToken)
        {
            Logger.Info("Creating a new connection to the host " + _host.Address);
            var c = new Connection(ProtocolVersion, _host.Address, _config);
            try
            {
                await c.Open(cancellationToken);
                if (_config.PoolingOptions.GetHeartBeatInterval() > 0)
                {
                    //Heartbeat is enabled, subscribe for possible exceptions
                    c.OnIdleRequestException += OnIdleRequestException;
                }
                return c;
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
                throw;
            }
        }

        /// <summary>
        /// Handler that gets invoked when if there is a socket exception when making a heartbeat/idle request
        /// </summary>
        private void OnIdleRequestException(Exception ex)
        {
            _host.SetDown();
        }

        private void OnHostDown(Host h, long delay)
        {
            if (Interlocked.CompareExchange(ref _hostDownFlag, 1, 0) != 0)
            {
                //A reconnection attempt is being scheduled concurrently
                return;
            }
            var currentTimeout = _timeout;
            //De-schedule the current reconnection attempt
            if (currentTimeout != null)
            {
                currentTimeout.Cancel();
            }
            //Schedule next reconnection attempt (without using the timer thread)
            _timeout = _timer.NewTimeout(() => Task.Factory.StartNew(AttemptReconnection), delay);
            //Dispose all current connections
            foreach (var c in _connections)
            {
                //Connection class allows multiple calls to Dispose
                c.Dispose();
            }
            Interlocked.Exchange(ref _hostDownFlag, 0);
        }

        /// <summary>
        /// Handles the reconnection attempts.
        /// If it succeeds, it marks the host as UP.
        /// If not, it marks the host as DOWN
        /// </summary>
        internal async Task AttemptReconnection()
        {
            _poolModificationSemaphore.Wait();

            var toRemove = _connections.Where(c => c.IsClosed).ToArray();
            foreach (var c in toRemove)
            {
                _connections.Remove(c);
            }
            if (_connections.Count > 0)
            {
                //there is already an open connection
                _poolModificationSemaphore.Release();
                return;
            }
            Logger.Info("Attempting reconnection to host {0}", _host.Address);
            try
            {
                var connection = await CreateConnection();
                _connections.Add(connection);
                //Release as soon as possible
                _poolModificationSemaphore.Release();
                Logger.Info("Reconnection attempt to host {0} succeeded", _host.Address);
                _host.BringUpIfDown();
            }
            catch (Exception)
            {
                _poolModificationSemaphore.Release();
                Logger.Info("Reconnection attempt to host {0} failed", _host.Address);
                _host.SetDown();
            }
        }

        private void OnHostUp(Host host)
        {
            var timeout = _timeout;
            if (timeout != null)
            {
                timeout.Cancel();
            }
            _timeout = null;
            //The host is back up, we can start creating the pool (if applies)
            MaybeCreateCorePool();
        }

        /// <summary>
        /// Create the min amount of connections, if the pool is empty
        /// </summary>
        /// <exception cref="System.Net.Sockets.SocketException" />
        internal Task<Connection[]> MaybeCreateCorePool()
        {
            var coreConnections = _config.GetPoolingOptions(ProtocolVersion).GetCoreConnectionsPerHost(_distance);
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
            var toRemove = _connections.Where(c => c.IsClosed).ToArray();
            foreach (var c in toRemove)
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
                    Logger.Info("{0} connection(s) to host {1} {2} created successfully", coreConnections, _host.Address, _connections.Count < 2 ? "was" : "were");
                    _host.BringUpIfDown();
                }
                _openingConnections = null;
                var connectionsArray = _connections.ToArray();
                _poolModificationSemaphore.Release();
                if (connectionsArray.Length == 0 && tasks.All(t => t.Status != TaskStatus.RanToCompletion))
                {
                    //Pool could not be created
                    Logger.Info("Connection pool to host {0} could not be created", _host.Address);
                    //There are multiple problems, but we only care about one
                    // ReSharper disable once PossibleNullReferenceException
                    throw tasks.First().Exception.InnerException;
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
                    if (t.Exception != null)
                    {
                        t.Exception.Handle(e => true);
                    }
                    //The first connection failed
                    //Wait for all to complete
                    return allCompleted;
                }
                return TaskHelper.ToTask(t.Result);
            }, TaskContinuationOptions.ExecuteSynchronously).Unwrap();
        }

        /// <summary>
        /// Creates a new connection, if the conditions apply.
        /// Only creates a new connection if there isn't a thread already creating one
        /// </summary>
        internal bool MaybeSpawnNewConnection(int inFlight)
        {
            var maxInFlight = _config.GetPoolingOptions(ProtocolVersion).GetMaxSimultaneousRequestsPerConnectionTreshold(_distance);
            var maxConnections = _config.GetPoolingOptions(ProtocolVersion).GetMaxConnectionPerHost(_distance);
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
                if (t.Exception != null)
                {
                    Logger.Error("Error during new connection attempt", t.Exception);
                }
            }, TaskContinuationOptions.ExecuteSynchronously);
            return true;
        }

        public void CheckHealth(Connection c)
        {
            if (c.TimedOutOperations < _config.SocketOptions.DefunctReadTimeoutThreshold)
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
            _host.Down -= OnHostDown;
            _host.Up -= OnHostUp;
            if (_connections.Count == 0)
            {
                return;
            }
            Logger.Info(String.Format("Disposing connection pool to {0}, closing {1} connections.", _host.Address, _connections.Count));
            _poolModificationSemaphore.Wait();
            foreach (var c in _connections)
            {
                c.Dispose();
            }
            _connections.Clear();
            _poolModificationSemaphore.Release();
            _poolModificationSemaphore.Dispose();
        }
    }
}
