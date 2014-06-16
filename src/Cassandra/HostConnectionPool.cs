using System;
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
    internal class HostConnectionPool
    {
        public static Logger _logger = new Logger(typeof(HostConnectionPool));
        private ConcurrentBag<Connection> _connections;
        private object _poolCreationLock = new object();
        private int _creating = 0;

        public Configuration Configuration { get; set; }

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

        public Host Host { get; set; }

        public HostDistance HostDistance { get; set; }

        public byte ProtocolVersion { get; set; }

        public HostConnectionPool(Host host, HostDistance hostDistance, byte protocolVersion, Configuration configuration)
        {
            this.Host = host;
            this.HostDistance = hostDistance;
            this.ProtocolVersion = protocolVersion;
            this.Configuration = configuration;
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

        public Connection CreateConnection()
        {
            _logger.Info("Creating a new connection to the host " + Host.Address.ToString());
            var endpoint = new IPEndPoint(Host.Address, Configuration.ProtocolOptions.Port);
            var c = new Connection(this.ProtocolVersion, endpoint, Configuration);
            c.Init();
            return c;
        }

        /// <summary>
        /// Create the min amount of connections, if the pool is empty
        /// </summary>
        public void MaybeCreateCorePool()
        {
            var coreConnections = Configuration.PoolingOptions.GetCoreConnectionsPerHost(HostDistance);
            if (_connections == null || _connections.All(c => c.IsClosed))
            {
                lock(_poolCreationLock)
                {
                    if (_connections != null && !_connections.All(c => c.IsClosed))
                    {
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
        public void MaybeSpawnNewConnection(int inFlight)
        {
            int maxInFlight = Configuration.PoolingOptions.GetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance);
            int maxConnections = Configuration.PoolingOptions.GetMaxConnectionPerHost(HostDistance);
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
    }
}
