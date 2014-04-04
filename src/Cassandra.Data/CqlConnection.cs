//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Cassandra.Data
{
    public class CqlConnection : DbConnection, ICloneable
    {
        private static readonly Dictionary<string, Cluster> _clusters = new Dictionary<string, Cluster>();
        internal Session ManagedConnection = null;
        private ConnectionState _connectionState = ConnectionState.Closed;
        private CassandraConnectionStringBuilder _connectionStringBuilder;
        private CqlBatchTransaction _currentTransaction;
        private Cluster _managedCluster;

        public override string ConnectionString
        {
            get { return _connectionStringBuilder == null ? null : _connectionStringBuilder.ConnectionString; }
            set { _connectionStringBuilder = new CassandraConnectionStringBuilder(value); }
        }

        public override string DataSource
        {
            get { return _connectionStringBuilder.ClusterName; }
        }

        public override string Database
        {
            get { return ManagedConnection == null ? null : ManagedConnection.Keyspace; }
        }

        protected override DbProviderFactory DbProviderFactory
        {
            get { return CqlProviderFactory.Instance; }
        }

        public override string ServerVersion
        {
            get { return "2.0"; }
        }

        public override ConnectionState State
        {
            get { return _connectionState; }
        }

        public CqlConnection()
        {
            _connectionStringBuilder = new CassandraConnectionStringBuilder();
        }

        public CqlConnection(string connectionString)
        {
            _connectionStringBuilder = new CassandraConnectionStringBuilder(connectionString);
        }

        public object Clone()
        {
            var conn = new CqlConnection(_connectionStringBuilder.ConnectionString);
            if (State != ConnectionState.Closed && State != ConnectionState.Broken)
                conn.Open();
            return conn;
        }

        private Dictionary<string, string> getCredentials(string auth)
        {
            return null;
        }

        internal void ClearDbTransaction()
        {
            _currentTransaction = null;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if (_currentTransaction != null)
                throw new InvalidOperationException();

            _currentTransaction = new CqlBatchTransaction(this);
            return _currentTransaction;
        }

        public override void ChangeDatabase(string databaseName)
        {
            if (ManagedConnection != null)
                ManagedConnection.ChangeKeyspace(databaseName);
        }

        protected override void Dispose(bool disposing)
        {
            if (_connectionState == ConnectionState.Open)
                Close();
            base.Dispose(disposing);
        }

        public override void Close()
        {
            _connectionState = ConnectionState.Closed;
            if (ManagedConnection != null)
                ManagedConnection.Dispose();
        }

        protected override DbCommand CreateDbCommand()
        {
            var cmd = new CqlCommand {CqlConnection = this};
            if (_currentTransaction != null)
                _currentTransaction.Append(cmd);
            return cmd;
        }

        public override void Open()
        {
            _connectionState = ConnectionState.Connecting;

            lock (_clusters)
            {
                if (!_clusters.ContainsKey(_connectionStringBuilder.ClusterName))
                {
                    Builder builder = _connectionStringBuilder.MakeClusterBuilder();
                    OnBuildingCluster(builder);
                    _managedCluster = builder.Build();
                    _clusters.Add(_connectionStringBuilder.ClusterName, _managedCluster);
                }
                else
                    _managedCluster = _clusters[_connectionStringBuilder.ClusterName];
            }

            if (string.IsNullOrEmpty(_connectionStringBuilder.DefaultKeyspace))
                ManagedConnection = _managedCluster.Connect("");
            else
                ManagedConnection = _managedCluster.Connect(_connectionStringBuilder.DefaultKeyspace);

            _connectionState = ConnectionState.Open;
        }

        /// <summary>
        /// To be overriden in child classes to change the default <see cref="Builder"/> settings
        /// for building a <see cref="Cluster"/>.
        /// 
        /// For example, some clients might want to specify the <see cref="DCAwareRoundRobinPolicy"/>
        /// when building the <see cref="Cluster"/> so that the clients could talk to only the hosts
        /// in specified datacenter for better performance.
        /// </summary>
        /// <param name="builder">The <see cref="Builder"/> for building a <see cref="Cluster"/>.</param>
        protected virtual void OnBuildingCluster(Builder builder)
        {
        }
    }
}