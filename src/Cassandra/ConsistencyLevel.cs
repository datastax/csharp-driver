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

namespace Cassandra
{
    /// <summary>
    /// Consistency refers to how up-to-date and synchronized a row of Cassandra data is on all of its replicas.
    /// When selecting, the consistency level specifies how many replicas must respond to a read request before returning data to the client application.
    /// When updating, inserting or deleting the consistency level specifies the number of replicas on which the write must succeed before returning an acknowledgment to the client application.
    /// </summary>
    public enum ConsistencyLevel
    {
        /// <summary>
        /// Writing: A write must be written to at least one node. If all replica nodes for the given row key are down, the write can still succeed after a hinted handoff has been written. If all replica nodes are down at write time, an ANY write is not readable until the replica nodes for that row have recovered.
        /// </summary>
        Any = 0x0000,
        /// <summary>
        /// Returns a response from the closest replica, as determined by the snitch.
        /// </summary>
        One = 0x0001,
        /// <summary>
        /// Returns the most recent data from two of the closest replicas.
        /// </summary>
        Two = 0x0002,
        /// <summary>
        /// Returns the most recent data from three of the closest replicas.
        /// </summary>
        Three = 0x0003,
        /// <summary>
        /// Reading: Returns the record with the most recent timestamp after a quorum of replicas has responded regardless of data center.
        /// <para>
        /// Writing: A write must be written to the commit log and memory table on a quorum of replica nodes.
        /// </para>
        /// </summary>
        Quorum = 0x0004,
        /// <summary>
        /// Reading: Returns the record with the most recent timestamp after all replicas have responded. The read operation will fail if a replica does not respond.
        /// <para>
        /// Writing: A write must be written to the commit log and memory table on all replica nodes in the cluster for that row.
        /// </para>
        /// </summary>
        All = 0x0005,
        /// <summary>
        /// Reading: Returns the record with the most recent timestamp once a quorum of replicas in the current data center as the coordinator node has reported.
        /// <para>
        /// Writing: A write must be written to the commit log and memory table on a quorum of replica nodes in the same data center as the coordinator node. Avoids latency of inter-data center communication.
        /// </para>
        /// </summary>
        LocalQuorum = 0x0006,
        /// <summary>
        /// Reading: Returns the record once a quorum of replicas in each data center of the cluster has responded.
        /// <para>Writing: Strong consistency. A write must be written to the commit log and memtable on a quorum of replica nodes in all data centers.</para>
        /// </summary>
        EachQuorum = 0x0007,
        /// <summary>
        /// Allows reading the current (and possibly uncommitted) state of data without proposing a new addition or update. If a SERIAL read finds an uncommitted transaction in progress, it will commit the transaction as part of the read.
        /// </summary>
        Serial = 0x0008,
        /// <summary>
        /// Same as <c>Serial</c>, but confined to the data center.
        /// </summary>
        LocalSerial = 0x0009,
        /// <summary>
        /// Similar to <c>One</c> but only within the DC the coordinator is in.
        /// </summary>
        LocalOne = 0x000A
    }
}
