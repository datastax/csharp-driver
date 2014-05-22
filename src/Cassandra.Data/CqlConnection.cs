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
﻿using System;
﻿using System.Collections.Concurrent;
﻿using System.Data;
using System.Data.Common;

namespace Cassandra.Data
{
    /// <summary>
    /// Represents a CQL connection.
    /// </summary>
    /// <inheritdoc />
    public class CqlConnection : DbConnection, ICloneable
    {
        private CassandraConnectionStringBuilder _connectionStringBuilder;
        private readonly static ConcurrentDictionary<string, Cluster> _clusters = new ConcurrentDictionary<string, Cluster>();
        private Cluster _managedCluster;
        private ConnectionState _connectionState = ConnectionState.Closed;
        private CqlBatchTransaction _currentTransaction;
        internal protected ISession ManagedConnection;

        /// <summary>
        /// Initializes a <see cref="CqlConnection"/>.
        /// </summary>
        public CqlConnection()
        {
            _connectionStringBuilder = new CassandraConnectionStringBuilder();
        }

        /// <summary>
        /// Initializes a <see cref="CqlConnection"/>.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public CqlConnection(string connectionString)
        {
            _connectionStringBuilder = new CassandraConnectionStringBuilder(connectionString);
        }

        internal void ClearDbTransaction()
        {
            _currentTransaction = null;
        }

        /// <inheritdoc />
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if (_currentTransaction != null)
                throw new InvalidOperationException();

            _currentTransaction = new CqlBatchTransaction(this);
            return _currentTransaction;
        }

        /// <inheritdoc />
        public override void ChangeDatabase(string databaseName)
        {
            if (ManagedConnection != null)
                ManagedConnection.ChangeKeyspace(databaseName);
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (_connectionState == ConnectionState.Open)
                Close();
            base.Dispose(disposing);
        }

        /// <inheritdoc />
        public override void Close()
        {
            _connectionState = System.Data.ConnectionState.Closed;
            if (ManagedConnection != null)
            {
                ManagedConnection.Dispose();
                ManagedConnection = null;
            }
            _managedCluster = null;
        }

        /// <inheritdoc />
        public override string ConnectionString
        {
            get
            {
                return _connectionStringBuilder == null ? null : _connectionStringBuilder.ConnectionString;
            }
            set
            {
                _connectionStringBuilder = new CassandraConnectionStringBuilder(value);
            }
        }

        /// <inheritdoc />
        protected override DbCommand CreateDbCommand()
        {
            var cmd = new CqlCommand() { CqlConnection = this };
            if (_currentTransaction != null)
                _currentTransaction.Append(cmd);
            return cmd;
        }

        /// <inheritdoc />
        public override string DataSource
        {
            get { return _connectionStringBuilder.ClusterName; }
        }

        /// <summary>
        /// Returns the Keyspace
        /// </summary>
        public override string Database
        {
            get { return ManagedConnection == null ? null : ManagedConnection.Keyspace; }
        }

        protected override DbProviderFactory DbProviderFactory { get { return CqlProviderFactory.Instance; } }

        /// <inheritdoc />
        public override void Open()
        {
            _connectionState = System.Data.ConnectionState.Connecting;
            _managedCluster = CreateCluster(_connectionStringBuilder);
            ManagedConnection = CreatedSession(_connectionStringBuilder.DefaultKeyspace);
            _connectionState = System.Data.ConnectionState.Open;
        }

        /// <summary>
        /// To be overridden in child classes to change the default <see cref="Builder"/> settings
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

        /// <summary>
        /// Creates a <see cref="Cluster"/>. By default <see cref="Cluster"/>s are created and cached
        /// by cluster name specified in connection string.
        /// 
        /// To be overridden in child classes to change the default creation and caching behavior.
        /// </summary>
        /// <param name="connectionStringBuilder">The <see cref="CassandraConnectionStringBuilder"/>.</param>
        /// <returns></returns>
        protected virtual Cluster CreateCluster(CassandraConnectionStringBuilder connectionStringBuilder)
        {
            Cluster cluster;
            if (!_clusters.TryGetValue(_connectionStringBuilder.ClusterName, out cluster))
            {
                var builder = _connectionStringBuilder.MakeClusterBuilder();
                OnBuildingCluster(builder);
                cluster = builder.Build();
                _clusters.TryAdd(_connectionStringBuilder.ClusterName, cluster);
            }

            return cluster;
        }

        /// <summary>
        /// Creates a <see cref="ISession"/>.
        /// 
        /// To be overridden in child classes if want to cache the <see cref="ISession"/> created.
        /// </summary>
        /// <param name="keyspace">The keyspace.</param>
        /// <returns>Returns the created <see cref="ISession"/>.</returns>
        protected virtual ISession CreatedSession(string keyspace)
        {
            if (_managedCluster == null)
            {
                return null;
            }

            return _managedCluster.Connect(keyspace ?? string.Empty);
        }

        /// <summary>
        /// To be called by CqlCommand to creates a <see cref="PreparedStatement"/>
        /// from <see cref="ManagedConnection"/>.
        /// 
        /// To be overridden in child classes if want to cache the <see cref="PreparedStatement"/> created.
        /// </summary>
        /// <param name="cqlQuery">The CQL query string.</param>
        /// <returns>Returns the created <see cref="PreparedStatement"/>.</returns>
        internal protected virtual PreparedStatement CreatePreparedStatement(string cqlQuery)
        {
            if (ManagedConnection == null)
            {
                return null;
            }
            return ManagedConnection.Prepare(cqlQuery);
        }

        /// <inheritdoc />
        public override string ServerVersion
        {
            get { return "2.0"; }
        }

        /// <inheritdoc />
        public override ConnectionState State
        {
            get { return _connectionState; }
        }

        public object Clone()
        {
            var conn = new CqlConnection(_connectionStringBuilder.ConnectionString);
            if (State != System.Data.ConnectionState.Closed && State != System.Data.ConnectionState.Broken)
                conn.Open();
            return conn;
        }
    }
}
