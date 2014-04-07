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
using System.Threading.Tasks;
namespace Cassandra
{
    /// <summary>
    /// A session holds connections to a Cassandra cluster, allowing it to be queried.
    /// 
    /// Each session maintains multiple connections to the cluster nodes,
    /// provides policies to choose which node to use for each query (round-robin on
    /// all nodes of the cluster by default), and handles retries for failed query (when
    /// it makes sense), etc...
    /// 
    /// Session instances are thread-safe and usually a single instance is enough
    /// per application. However, a given session can only be set to one keyspace
    /// at a time, so one instance per keyspace is necessary.
    /// </summary>
    interface ISession: IDisposable
    {
        /// <summary>
        /// Gets name of currently used keyspace. 
        /// </summary>
        string Keyspace { get; }
        /// <summary>
        /// Gets the cluster information and state
        /// </summary>
        Cluster Cluster { get; }
        /// <summary>
        /// Gets the Cassandra native binary protocol version
        /// </summary>
        int BinaryProtocolVersion { get; }
        IAsyncResult BeginExecute(Query query, AsyncCallback callback, object state);
        IAsyncResult BeginExecute(Query query, object tag, AsyncCallback callback, object state);
        IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, AsyncCallback callback, object state);
        IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, object tag, AsyncCallback callback, object state);
        IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state);
        /// <summary>
        ///  Switches to the specified keyspace.
        /// </summary>
        /// <param name="keyspace_name">Name of keyspace that is to be used.</param
        void ChangeKeyspace(string keyspace_name);
        /// <summary>
        ///  Creates new keyspace in current cluster.        
        /// </summary>
        /// <param name="keyspace_name">Name of keyspace to be created.</param>
        /// <param name="replication">Replication property for this keyspace.
        /// To set it, refer to the <see cref="ReplicationStrategies"/> class methods. 
        /// It is a dictionary of replication property sub-options where key is a sub-option name and value is a value for that sub-option. 
        /// <p>Default value is <code>'SimpleStrategy'</code> with <code>'replication_factor' = 1</code></p></param>
        /// <param name="durable_writes">Whether to use the commit log for updates on this keyspace. Default is set to <code>true</code>.</param>
        void CreateKeyspace(string keyspace_name, Dictionary<string, string> replication = null, bool durable_writes = true);
        /// <summary>
        ///  Creates new keyspace in current cluster.
        ///  If keyspace with specified name already exists, then this method does nothing.
        /// </summary>
        /// <param name="keyspace_name">Name of keyspace to be created.</param>
        /// <param name="replication">Replication property for this keyspace.
        /// To set it, refer to the <see cref="ReplicationStrategies"/> class methods. 
        /// It is a dictionary of replication property sub-options where key is a sub-option name and value is a value for that sub-option.
        /// <p>Default value is <code>'SimpleStrategy'</code> with <code>'replication_factor' = 2</code></p></param>
        /// <param name="durable_writes">Whether to use the commit log for updates on this keyspace. Default is set to <code>true</code>.</param>
        void CreateKeyspaceIfNotExists(string keyspace_name, Dictionary<string, string> replication = null, bool durable_writes = true);
        /// <summary>
        ///  Deletes specified keyspace from current cluster.
        ///  If keyspace with specified name does not exist, then exception will be thrown.
        /// </summary>
        /// <param name="keyspace_name">Name of keyspace to be deleted.</param>
        void DeleteKeyspace(string keyspace_name);
        /// <summary>
        ///  Deletes specified keyspace from current cluster.
        ///  If keyspace with specified name does not exist, then this method does nothing.
        /// </summary>
        /// <param name="keyspace_name">Name of keyspace to be deleted.</param>
        void DeleteKeyspaceIfExists(string keyspace_name);
        RowSet EndExecute(IAsyncResult ar);
        PreparedStatement EndPrepare(IAsyncResult ar);
        /// <summary>
        /// Executes the provided query.
        /// </summary>
        RowSet Execute(Query query);
        /// <summary>
        /// Executes the provided query.
        /// </summary>
        RowSet Execute(string cqlQuery);
        /// <summary>
        /// Executes the provided query.
        /// </summary>
        RowSet Execute(string cqlQuery, ConsistencyLevel consistency);
        /// <summary>
        /// Executes the provided query.
        /// </summary>
        RowSet Execute(string cqlQuery, int pageSize);
        /// <summary>
        /// Executes a query asynchronously
        /// </summary>
        /// <param name="query">The query to execute</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task<RowSet> ExecuteAsync(Query query);
        PreparedStatement Prepare(string cqlQuery);
        void WaitForSchemaAgreement(RowSet rs);
        bool WaitForSchemaAgreement(System.Net.IPAddress forHost);
    }
}
