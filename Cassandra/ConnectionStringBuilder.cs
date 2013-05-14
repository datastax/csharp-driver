using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Data.Common;

namespace Cassandra
{
    public class CassandraConnectionStringBuilder : DbConnectionStringBuilder
    {

        public CassandraConnectionStringBuilder() : base(false) { }
        public CassandraConnectionStringBuilder(string connectionString) : base(false) { ConnectionString = connectionString; }

        public Builder ApplyToBuilder(Builder builder)
        {
            return builder.AddContactPoints(ContactPoints).WithPort(Port).WithDefaultKeyspace(DefaultKeyspace).WithAuthInfoProvider(new SimpleAuthInfoProvider().Add("username", Username).Add("password",Password));
        }

        public Builder MakeClusterBuilder()
        {
            return ApplyToBuilder(Cluster.Builder());
        }

        private T DefaultIfNotExists<T>(string name, T def)
        {
            if (!base.ContainsKey(name))
                return def;
            return (T)Convert.ChangeType(base[name], typeof(T));
        }

        private T ThrowIfNotExists<T>(string name)
        {
            if (!base.ContainsKey(name))
                throw new FormatException(name + " value are missing in connection string");
            return (T)Convert.ChangeType(base[name], typeof(T));
        }

        public string ClusterName
        {
            get { return DefaultIfNotExists<string>("Cluster Name", "Cassandra Cluster"); }
            set { base["Cluster Name"] = value; }
        }

        public string DefaultKeyspace
        {
            get { return DefaultIfNotExists<string>("Default Keyspace", null); }
            set { base["Default Keyspace"] = value; }
        }

        public int Port
        {
            get { return DefaultIfNotExists<int>("Port", ProtocolOptions.DefaultPort); }
            set { base["Port"] = value; }
        }

        public string[] ContactPoints
        {
            get { return ThrowIfNotExists<string>("Contact Points").Split(','); }
            set { base["Contact Points"] = string.Join(",", value); }
        }

        public string Username
        {
            get { return DefaultIfNotExists<string>("Username", null); }
            set { base["Username"] = value; }
        }

        public string Password
        {
            get { return DefaultIfNotExists<string>("Password", null); }
            set { base["Password"] = value; }
        }
    }
}