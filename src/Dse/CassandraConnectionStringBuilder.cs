//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Data.Common;

namespace Dse
{
    public class CassandraConnectionStringBuilder : DbConnectionStringBuilder
    {
        public string ClusterName
        {
            get { return DefaultIfNotExists("Cluster Name", "Cassandra Cluster"); }
            set { base["Cluster Name"] = value; }
        }

        public string DefaultKeyspace
        {
            get { return DefaultIfNotExists<string>("Default Keyspace", null); }
            set { base["Default Keyspace"] = value; }
        }

        public int Port
        {
            get { return DefaultIfNotExists("Port", ProtocolOptions.DefaultPort); }
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

        public CassandraConnectionStringBuilder()
        {
        }

        public CassandraConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public Builder ApplyToBuilder(Builder builder)
        {
            builder.AddContactPoints(ContactPoints).WithPort(Port).WithDefaultKeyspace(DefaultKeyspace);
            if (!string.IsNullOrEmpty(Username) && !string.IsNullOrEmpty(Password))
            {
                //Make sure the credentials are not null
                builder.WithCredentials(Username, Password);
            }
            return builder;
        }

        public Builder MakeClusterBuilder()
        {
            return ApplyToBuilder(Cluster.Builder());
        }

        private T DefaultIfNotExists<T>(string name, T def)
        {
            if (!base.ContainsKey(name))
                return def;
            return (T) Convert.ChangeType(base[name], typeof (T));
        }

        private T ThrowIfNotExists<T>(string name)
        {
            if (!base.ContainsKey(name))
                throw new FormatException(name + " value are missing in connection string");
            return (T) Convert.ChangeType(base[name], typeof (T));
        }
    }
}
