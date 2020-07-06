// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    public interface IMetadataSnapshotProvider
    {
        event HostsEventHandler HostsEvent;

        event SchemaChangedEventHandler SchemaChangedEvent;

        /// <summary>
        /// Event that gets triggered when a new host is added to the cluster
        /// </summary>
        event Action<Host> HostAdded;

        /// <summary>
        /// Event that gets triggered when a host has been removed from the cluster
        /// </summary>
        event Action<Host> HostRemoved;

        Configuration Configuration { get; }

        /// <summary>
        /// <para>
        ///  Returns all known hosts of this cluster from the driver's cache. The driver's cache
        /// is kept up to date using server protocol events so it will not be populated until the metadata initialization is done
        /// and a connection is open.
        /// </para> 
        /// <para>
        /// This method might return an empty collection if the metadata initialization has not finished yet.
        /// </para>
        /// </summary>
        ICollection<Host> AllHostsSnapshot();

        /// <summary>
        /// <para>
        /// Get the replicas without performing any I/O (it will use the driver's cache). The driver's cache
        /// is kept up to date using server protocol events so it will not be populated until the metadata initialization is done
        /// and a connection is open.
        /// </para> 
        /// <para>
        /// This method might return an empty collection if the metadata initialization has not finished yet.
        /// </para>
        /// </summary>
        IEnumerable<IPEndPoint> AllReplicasSnapshot();

        /// <summary>
        /// <para>
        /// Get the replicas for a given keyspace and partition key without performing any I/O (it will use the driver's cache).
        /// The driver's cache is kept up to date using server protocol events so it will not be populated
        /// until the metadata initialization is done and a connection is open.
        /// </para> 
        /// <para>
        /// This method might return an empty collection if the metadata initialization has not finished yet.
        /// </para>
        /// </summary>
        ICollection<Host> GetReplicasSnapshot(string keyspaceName, byte[] partitionKey);

        /// <summary>
        /// <para>
        /// Get the replicas for a partition key without performing any I/O (it will use the driver's cache).
        /// The driver's cache is kept up to date using server protocol events so it will not be populated
        /// until the metadata initialization is done and a connection is open.
        /// </para> 
        /// <para>
        /// This method might return an empty collection if the metadata initialization has not finished yet.
        /// </para>
        /// </summary>
        ICollection<Host> GetReplicasSnapshot(byte[] partitionKey);
    }
}