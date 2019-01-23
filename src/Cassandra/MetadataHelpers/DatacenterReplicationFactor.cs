//
//       Copyright DataStax, Inc.
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
//

using System;

namespace Cassandra.MetadataHelpers
{
    internal class DatacenterReplicationFactor : IEquatable<DatacenterReplicationFactor>
    {
        public string Datacenter { get; }
        public int ReplicationFactor { get; }

        public DatacenterReplicationFactor(string datacenter, int replicationFactor)
        {
            Datacenter = datacenter;
            ReplicationFactor = replicationFactor;
        }
        
        public override bool Equals(object obj)
        {
            return Equals(obj as DatacenterReplicationFactor);
        }

        public bool Equals(DatacenterReplicationFactor other)
        {
            return other != null &&
                   Datacenter == other.Datacenter &&
                   ReplicationFactor == other.ReplicationFactor;
        }

        public override int GetHashCode()
        {
            var hashCode = -1601459050;
            hashCode = hashCode * -1521134295 + Datacenter?.GetHashCode() ?? string.Empty.GetHashCode();
            hashCode = hashCode * -1521134295 + ReplicationFactor.GetHashCode();
            return hashCode;
        }
    }
}