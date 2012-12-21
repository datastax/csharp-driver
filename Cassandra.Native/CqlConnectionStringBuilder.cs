using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using Cassandra.Native;
using System.Threading;
using System.Data.Common;
namespace Cassandra
{

    public enum ConsistencyLevel
    {
        ANY = 0x0000,
        ONE = 0x0001,
        TWO = 0x0002,
        THREE = 0x0003,
        QUORUM = 0x0004,
        ALL = 0x0005,
        LOCAL_QUORUM = 0x0006,
        EACH_QUORUM = 0x0007,
        DEFAULT = ONE,
        IGNORE = ANY
    }

    public class ConnectionStringBuilder : DbConnectionStringBuilder
    {
        public string Keyspace { get; set; }
        public int Port { get; private set; }
        public IEnumerable<IPAddress> ContactPoints { get; private set; }
        //public string Username { get; private set; }
        //public string Password { get; private set; }
        public CompressionType CompressionType { get; private set; }
        //public ConsistencyLevel ReadCqlConsistencyLevel { get; private set; }
        //public ConsistencyLevel WriteCqlConsistencyLevel { get; private set; }

        //public int ConnectionTimeout { get; private set; }
        //public int MaxPoolSize { get; private set; }

        public ConnectionStringBuilder(
            string Keyspace,
            IEnumerable<IPAddress> ContactPoints,
            int Port = Cluster.DEFAULT_PORT,
            //string Username = null,
            //string Password = null,
            CompressionType CompressionType = CompressionType.NoCompression
            //ConsistencyLevel ReadCqlConsistencyLevel = ConsistencyLevel.QUORUM,
            //ConsistencyLevel WriteCqlConsistencyLevel = ConsistencyLevel.QUORUM,
            //int ConnectionTimeout = Timeout.Infinite,
            //int MaxPoolSize = int.MaxValue
        )
        {
            this.Keyspace = Keyspace;
            this.Port = Port;
            this.ContactPoints = ContactPoints;
            //this.Username = Username;
            //this.Password = Password;
            this.CompressionType = CompressionType;
            //this.ReadCqlConsistencyLevel = ReadCqlConsistencyLevel;
            //this.WriteCqlConsistencyLevel = WriteCqlConsistencyLevel;
            //this.ConnectionTimeout = ConnectionTimeout;
            //this.MaxPoolSize = MaxPoolSize;
        }

        public ConnectionStringBuilder() 
        { 
        }

        public ConnectionStringBuilder(string connectionString)
        {
            InitializeConnectionString(connectionString);
        }

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

            //if (!pairs.ContainsKey("Max Pool Size"))
            //    MaxPoolSize = int.MaxValue;
            //else
            //{
            //    int maxPoolSize;

            //    if (!Int32.TryParse(pairs["Max Pool Size"], out maxPoolSize))
            //        throw new Exception("Max Pool Size is not valid.");

            //    if (maxPoolSize < 0)
            //        throw new Exception("Max Pool Size is not valid.");

            //    MaxPoolSize = maxPoolSize;
            //}

            if (!pairs.ContainsKey("Port"))
                Port = Cluster.DEFAULT_PORT;
            else
            {
                int port;
                if (!Int32.TryParse(pairs["Port"], out port))
                    throw new Exception("Port is not valid.");
                Port = port;
            }

            //if (!pairs.ContainsKey("Connection Timeout"))
            //    ConnectionTimeout = Timeout.Infinite;
            //else
            //{
            //    int connectionTimeout;

            //    if (!Int32.TryParse(pairs["Connection Timeout"], out connectionTimeout))
            //        throw new Exception("Connection Timeout is not valid.");

            //    if (connectionTimeout < 0)
            //        throw new Exception("Connection Timeout is not valid.");

            //    ConnectionTimeout = connectionTimeout * 1000;
            //}

            //if (!pairs.ContainsKey("Read"))
            //    ReadCqlConsistencyLevel = ConsistencyLevel.QUORUM;
            //else
            //    ReadCqlConsistencyLevel = (ConsistencyLevel)Enum.Parse(typeof(ConsistencyLevel), pairs["Read"]);

            //if (!pairs.ContainsKey("Write"))
            //    WriteCqlConsistencyLevel = ConsistencyLevel.QUORUM;
            //else
            //    WriteCqlConsistencyLevel = (ConsistencyLevel)Enum.Parse(typeof(ConsistencyLevel), pairs["Write"]);

            if (!pairs.ContainsKey("Compression Type"))
                CompressionType = CompressionType.NoCompression;
            else
                CompressionType = (CompressionType)Enum.Parse(typeof(CompressionType), pairs["Compression Type"]);

            //if (pairs.ContainsKey("Username"))
            //    Username = pairs["Username"];

            //if (pairs.ContainsKey("Password"))
            //    Password = pairs["Password"];

            if (!pairs.ContainsKey("Servers"))
            {
                throw new Exception("There must be specified at least one Cluster Server");
            }
            else
            {
                var ce = new List<IPAddress>();
                string[] servers = pairs["Servers"].Split(',');
                foreach (var server in servers)
                    ce.Add(IPAddress.Parse(server));
                ContactPoints = ce;
            }            
        }

        public string ClusterEndpointsString()
        {
            List<string> servers = new List<string>();
            foreach (var n in ContactPoints)
                servers.Add(n.ToString());
            return string.Join(",", servers.ToArray());
        }

        public string GetConnectionString()
        {
            StringBuilder b = new StringBuilder();
            string format = "{0}={1};";

            b.AppendFormat(format, "Keyspace", Keyspace);

            //if(MaxPoolSize != int.MaxValue)
            //    b.AppendFormat(format, "Max Pool Size", MaxPoolSize);
    
            //if(ConnectionTimeout!=Timeout.Infinite)
            //    b.AppendFormat(format, "Connection Timeout", Convert.ToInt32(ConnectionTimeout / 1000));
    
            //if(ReadCqlConsistencyLevel != ConsistencyLevel.QUORUM)
            //    b.AppendFormat(format, "Read", ReadCqlConsistencyLevel);
    
            //if(WriteCqlConsistencyLevel != ConsistencyLevel.QUORUM)
            //    b.AppendFormat(format, "Write", WriteCqlConsistencyLevel);

            if( CompressionType != CompressionType.NoCompression)
                b.AppendFormat(format, "Compression Type", CompressionType);

            //if(Username!=null)
            //    b.AppendFormat(format, "Username", Username);
    
            //if(Password!=null)
            //    b.AppendFormat(format, "Password", Password);

            if (Port != Cluster.DEFAULT_PORT)
                b.AppendFormat(format, "Port", Port);
    
            b.AppendFormat(format, "Servers", ClusterEndpointsString());

            return b.ToString();
        }

    }
}