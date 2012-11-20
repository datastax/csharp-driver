using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using Cassandra.Native;
using System.Threading;
using System.Data.Common;
namespace Cassandra
{

    public enum CqlConsistencyLevel
    {
        ANY = 0x0000,
        ONE = 0x0001,
        TWO = 0x0002,
        THREE = 0x0003,
        QUORUM = 0x0004,
        ALL = 0x0005,
        LOCAL_QUORUM = 0x0006,
        EACH_QUORUM = 0x0007,
        DEFAULT = QUORUM,
        IGNORE = ANY
    }

    public class CqlConnectionStringBuilder : DbConnectionStringBuilder
    {
        public string Keyspace { get; set; }
        public IEnumerable<IPEndPoint> ClusterEndpoints { get; private set; }
        public string Username { get; private set; }
        public string Password { get; private set; }
        public CassandraCompressionType CompressionType { get; private set; }
        public CqlConsistencyLevel ReadCqlConsistencyLevel { get; private set; }
        public CqlConsistencyLevel WriteCqlConsistencyLevel { get; private set; }

        public string PoolId { get; private set; }
        public int ConnectionTimeout { get; private set; }
        public int MaxPoolSize { get; private set; }

        public CqlConnectionStringBuilder(
            string Keyspace,
            IEnumerable<IPEndPoint> ClusterEndpoints,
            string Username = null,
            string Password = null,
            CassandraCompressionType CompressionType = CassandraCompressionType.NoCompression,
            CqlConsistencyLevel ReadCqlConsistencyLevel = CqlConsistencyLevel.QUORUM,
            CqlConsistencyLevel WriteCqlConsistencyLevel = CqlConsistencyLevel.QUORUM,
            int ConnectionTimeout = Timeout.Infinite,
            int MaxPoolSize = int.MaxValue,
            string PoolId = ""
        )
        {
            this.Keyspace = Keyspace;
            this.ClusterEndpoints = ClusterEndpoints;
            this.Username = Username;
            this.Password = Password;
            this.CompressionType = CompressionType;
            this.ReadCqlConsistencyLevel = ReadCqlConsistencyLevel;
            this.WriteCqlConsistencyLevel = WriteCqlConsistencyLevel;
            this.ConnectionTimeout = ConnectionTimeout;
            this.MaxPoolSize = MaxPoolSize;
            this.PoolId = PoolId;
        }

        public CqlConnectionStringBuilder() 
        { 
        }

        public CqlConnectionStringBuilder(string connectionString)
        {
            InitializeConnectionString(connectionString);
        }

        static int DefaultPort = 8000;

        private void InitializeConnectionString(string connectionString)
        {
            string[] connParts = connectionString.Split(';');
            IDictionary<string, string> pairs = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);

            foreach (string part in connParts)
            {
                string[] nameValue = part.Split(new[] { '=' }, 2);

                if (nameValue.Length != 2)
                    continue;

                pairs.Add(nameValue[0], nameValue[1]);
            }

            if (pairs.ContainsKey("Keyspace"))
                Keyspace = pairs["Keyspace"];

            if (!pairs.ContainsKey("Max Pool Size"))
                MaxPoolSize = int.MaxValue;
            else
            {
                int maxPoolSize;

                if (!Int32.TryParse(pairs["Max Pool Size"], out maxPoolSize))
                    throw new Exception("Max Pool Size is not valid.");

                if (maxPoolSize < 0)
                    throw new Exception("Max Pool Size is not valid.");

                MaxPoolSize = maxPoolSize;
            }

            if (!pairs.ContainsKey("Connection Timeout"))
                ConnectionTimeout = Timeout.Infinite;
            else
            {
                int connectionTimeout;

                if (!Int32.TryParse(pairs["Connection Timeout"], out connectionTimeout))
                    throw new Exception("Connection Timeout is not valid.");

                if (connectionTimeout < 0)
                    throw new Exception("Connection Timeout is not valid.");

                ConnectionTimeout = connectionTimeout * 1000;
            }

            if (!pairs.ContainsKey("Read"))
                ReadCqlConsistencyLevel = CqlConsistencyLevel.QUORUM;
            else
                ReadCqlConsistencyLevel = (CqlConsistencyLevel)Enum.Parse(typeof(CqlConsistencyLevel), pairs["Read"]);

            if (!pairs.ContainsKey("Write"))
                WriteCqlConsistencyLevel = CqlConsistencyLevel.QUORUM;
            else
                WriteCqlConsistencyLevel = (CqlConsistencyLevel)Enum.Parse(typeof(CqlConsistencyLevel), pairs["Write"]);

            if (!pairs.ContainsKey("Compression Type"))
                CompressionType = CassandraCompressionType.NoCompression;
            else
                CompressionType = (CassandraCompressionType)Enum.Parse(typeof(CassandraCompressionType), pairs["Compression Type"]);

            if (pairs.ContainsKey("Username"))
                Username = pairs["Username"];

            if (pairs.ContainsKey("Password"))
                Password = pairs["Password"];

            if (pairs.ContainsKey("PoolId"))
                PoolId = pairs["PoolId"];
            else
                PoolId = "";

            if (!pairs.ContainsKey("Servers"))
            {
                throw new Exception("There must be specified at least one Cluster Server");
            }
            else
            {
                var ce = new List<IPEndPoint>();
                string[] servers = pairs["Servers"].Split(',');
                foreach (var server in servers)
                    ce.Add(ParseEndPoint(server));
                ClusterEndpoints = ce;
            }            
        }

        public static IPEndPoint ParseEndPoint(string server)
        {
            string[] serverParts = server.Split(':');
            string host = serverParts[0];

            if (serverParts.Length == 2)
            {
                int port;
                if (Int32.TryParse(serverParts[1], out port))
                    return new IPEndPoint(IPAddress.Parse(host), port);
                else
                    throw new Exception("Endpoint port is not valid.");
            }
            else
                return new IPEndPoint(IPAddress.Parse(host), DefaultPort);
        }

        public string ClusterEndpointsString()
        {
            List<string> servers = new List<string>();
            foreach (var n in ClusterEndpoints)
                servers.Add(n.Address.ToString() + ":" + n.Port.ToString());
            return  String.Join(",", servers.ToArray());
        }

        public string GetConnectionString()
        {
            StringBuilder b = new StringBuilder();
            string format = "{0}={1};";

            b.AppendFormat(format, "Keyspace", Keyspace);

            if(MaxPoolSize != int.MaxValue)
                b.AppendFormat(format, "Max Pool Size", MaxPoolSize);
    
            if(ConnectionTimeout!=Timeout.Infinite)
                b.AppendFormat(format, "Connection Timeout", Convert.ToInt32(ConnectionTimeout / 1000));
    
            if(ReadCqlConsistencyLevel != CqlConsistencyLevel.QUORUM)
                b.AppendFormat(format, "Read", ReadCqlConsistencyLevel);
    
            if(WriteCqlConsistencyLevel != CqlConsistencyLevel.QUORUM)
                b.AppendFormat(format, "Write", WriteCqlConsistencyLevel);

            if( CompressionType != CassandraCompressionType.NoCompression)
                b.AppendFormat(format, "Compression Type", CompressionType);

            if(Username!=null)
                b.AppendFormat(format, "Username", Username);
    
            if(Password!=null)
                b.AppendFormat(format, "Password", Password);
    
            if(!string.IsNullOrEmpty(PoolId))
                b.AppendFormat(format, "PoolId", PoolId);

            b.AppendFormat(format, "Servers", ClusterEndpointsString());

            return b.ToString();
        }

    }
}