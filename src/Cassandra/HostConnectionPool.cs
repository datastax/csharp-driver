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
        public ConcurrentBag<Connection> _connections;
        public object _poolCreationLock = new object();
        private int _creating = 0;

        public Host Host { get; set; }

        public HostDistance HostDistance { get; set; }

        public Configuration Configuration { get; set; }

        public byte ProtocolVersion { get; set; }

        public HostConnectionPool(Host host, HostDistance hostDistance, byte protocolVersion, Configuration configuration)
        {
            this.Host = host;
            this.HostDistance = hostDistance;
            this.ProtocolVersion = protocolVersion;
            this.Configuration = configuration;
        }

        public Connection BorrowConnection(string keyspace)
        {
            MaybeCreateCorePool();
            var connection = _connections.OrderBy(c => c.InFlight).First();
            MaybeSpawnNewConnection(connection.InFlight);
            connection.Keyspace = keyspace;
            return connection;
        }

        public Connection CreateConnection()
        {
            _logger.Info("Creating a new connection to the host " + Host.Address.ToString());
            var endpoint = new IPEndPoint(Host.Address, Configuration.ProtocolOptions.Port);
            var c = new Connection(this.ProtocolVersion, endpoint, Configuration.ProtocolOptions, Configuration.SocketOptions);
            c.Init();
            return c;
        }

        /// <summary>
        /// Create the min amount of connections, if the pool is empty
        /// </summary>
        public void MaybeCreateCorePool()
        {
            var coreConnections = Configuration.PoolingOptions.GetCoreConnectionsPerHost(HostDistance);
            if (_connections == null)
            {
                lock(_poolCreationLock)
                {
                    if (_connections != null)
                    {
                        return;
                    }
                    _connections = new ConcurrentBag<Connection>();
                    while (_connections.Count < coreConnections)
                    {
                        _connections.Add(CreateConnection());
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
