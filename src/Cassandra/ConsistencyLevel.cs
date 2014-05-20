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
        Two = 0x0002,
        Three = 0x0003,
        /// <summary>
        /// Reading: Returns the record with the most recent timestamp after a quorum of replicas has responded regardless of data center.
        /// Writing: A write must be written to the commit log and memory table on a quorum of replica nodes.
        /// </summary>
        Quorum = 0x0004,
        /// <summary>
        /// Reading: Returns the record with the most recent timestamp after all replicas have responded. The read operation will fail if a replica does not respond.
        /// Writing: A write must be written to the commit log and memory table on all replica nodes in the cluster for that row.
        /// </summary>
        All = 0x0005,
        /// <summary>
        /// Reading: Returns the record with the most recent timestamp once a quorum of replicas in the current data center as the coordinator node has reported.
        /// Writing: A write must be written to the commit log and memory table on a quorum of replica nodes in the same data center as the coordinator node. Avoids latency of inter-data center communication.
        /// </summary>
        LocalQuorum = 0x0006,
        EachQuorum = 0x0007,
        Serial = 0x0008,
        LocalSerial = 0x0009,
        LocalOne = 0x0010
    }
}