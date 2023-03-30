//
//      Copyright (C) DataStax Inc.
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

using System;
using System.Data.Common;

namespace Cassandra
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
