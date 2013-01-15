using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Data.Common;

namespace Cassandra
{

    public enum ConsistencyLevel
    {
        Any = 0x0000,
        One = 0x0001,
        Two = 0x0002,
        Three = 0x0003,
        Quorum = 0x0004,
        All = 0x0005,
        LocalQuorum = 0x0006,
        EachQuorum = 0x0007,
        Default = One,
        Ignore = Any
    }

    public class ConnectionStringBuilder : DbConnectionStringBuilder
    {
        public string Keyspace { get; private set; }
        public int Port { get; private set; }
        public IEnumerable<string> ContactPoints { get; private set; }
        //public string Username { get; private set; }
        //public string Password { get; private set; }
        public CompressionType CompressionType { get; private set; }
        //public Consistency ReadCqlConsistencyLevel { get; private set; }
        //public Consistency WriteCqlConsistencyLevel { get; private set; }

        //public int ConnectionTimeout { get; private set; }
        //public int MaxPoolSize { get; private set; }

        public ConnectionStringBuilder(
            string keyspace,
            IEnumerable<string> contactPoints,
            int port = ProtocolOptions.DefaultPort,
            //string Username = null,
            //string Password = null,
            CompressionType compressionType = CompressionType.NoCompression
            //Consistency ReadCqlConsistencyLevel = Consistency.QUORUM,
            //Consistency WriteCqlConsistencyLevel = Consistency.QUORUM,
            //int ConnectionTimeout = Timeout.Infinite,
            //int MaxPoolSize = int.MaxValue
        )
        {
            this.Keyspace = keyspace;
            this.Port = port;
            this.ContactPoints = contactPoints;
            //this.Username = Username;
            //this.Password = Password;
            this.CompressionType = compressionType;
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

            if (pairs.ContainsKey("DefaultKeyspace"))
                Keyspace = pairs["DefaultKeyspace"];

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
                Port = ProtocolOptions.DefaultPort;
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
            //    ReadCqlConsistencyLevel = Consistency.QUORUM;
            //else
            //    ReadCqlConsistencyLevel = (Consistency)Enum.Parse(typeof(Consistency), pairs["Read"]);

            //if (!pairs.ContainsKey("Write"))
            //    WriteCqlConsistencyLevel = Consistency.QUORUM;
            //else
            //    WriteCqlConsistencyLevel = (Consistency)Enum.Parse(typeof(Consistency), pairs["Write"]);

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
                ContactPoints = pairs["Servers"].Split(',');
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
            var b = new StringBuilder();
            string format = "{0}={1};";

            b.AppendFormat(format, "DefaultKeyspace", Keyspace);

            //if(MaxPoolSize != int.MaxValue)
            //    b.AppendFormat(format, "Max Pool Size", MaxPoolSize);
    
            //if(ConnectionTimeout!=Timeout.Infinite)
            //    b.AppendFormat(format, "Connection Timeout", Convert.ToInt32(ConnectionTimeout / 1000));
    
            //if(ReadCqlConsistencyLevel != Consistency.QUORUM)
            //    b.AppendFormat(format, "Read", ReadCqlConsistencyLevel);
    
            //if(WriteCqlConsistencyLevel != Consistency.QUORUM)
            //    b.AppendFormat(format, "Write", WriteCqlConsistencyLevel);

            if( CompressionType != CompressionType.NoCompression)
                b.AppendFormat(format, "Compression Type", CompressionType);

            //if(Username!=null)
            //    b.AppendFormat(format, "Username", Username);
    
            //if(Password!=null)
            //    b.AppendFormat(format, "Password", Password);

            if (Port != ProtocolOptions.DefaultPort)
                b.AppendFormat(format, "Port", Port);
    
            b.AppendFormat(format, "Servers", ClusterEndpointsString());

            return b.ToString();
        }

    }
}