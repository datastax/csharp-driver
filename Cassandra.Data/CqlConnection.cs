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
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using Cassandra;

namespace Cassandra.Data
{
    public class CqlConnection : DbConnection, ICloneable
    {
        CassandraConnectionStringBuilder _connectionStringBuilder = null;
        static Dictionary<string, Cluster> _clusters = new Dictionary<string, Cluster>();
        Cluster _managedCluster = null;
        ConnectionState _connectionState = ConnectionState.Closed;
        CqlBatchTransaction _currentTransaction = null;

        internal Session ManagedConnection = null;

        public CqlConnection()
        {
            _connectionStringBuilder = new CassandraConnectionStringBuilder();
        }

        public CqlConnection(string connectionString)
        {
            _connectionStringBuilder = new CassandraConnectionStringBuilder(connectionString);
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
            _connectionState = System.Data.ConnectionState.Closed;
            if (ManagedConnection != null)
                ManagedConnection.Dispose();
        }

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

        protected override DbCommand CreateDbCommand()
        {
            var cmd = new CqlCommand() { CqlConnection = this };
            if (_currentTransaction != null)
                _currentTransaction.Append(cmd);
            return cmd;
        }

        public override string DataSource
        {
            get { return _connectionStringBuilder.ClusterName; }
        }

        public override string Database
        {
            get { return ManagedConnection == null ? null : ManagedConnection.Keyspace; }
        }

        protected override DbProviderFactory DbProviderFactory { get { return CqlProviderFactory.Instance; } }

        public override void Open()
        {
            _connectionState = System.Data.ConnectionState.Connecting;

            lock (_clusters)
            {
                if (!_clusters.ContainsKey(_connectionStringBuilder.ClusterName))
                {
                    _managedCluster = _connectionStringBuilder.MakeClusterBuilder().Build();
                    _clusters.Add(_connectionStringBuilder.ClusterName, _managedCluster);
                }
                else
                    _managedCluster = _clusters[_connectionStringBuilder.ClusterName];
            }

            if (string.IsNullOrEmpty(_connectionStringBuilder.DefaultKeyspace))
                ManagedConnection = _managedCluster.Connect("");
            else
                ManagedConnection = _managedCluster.Connect(_connectionStringBuilder.DefaultKeyspace);

            _connectionState = System.Data.ConnectionState.Open;
        }

        public override string ServerVersion
        {
            get { return "2.0"; }
        }

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
