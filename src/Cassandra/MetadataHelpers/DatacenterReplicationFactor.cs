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
    internal struct DatacenterReplicationFactor : IEquatable<DatacenterReplicationFactor>
    {
        private readonly int _hashCode;

        public DatacenterReplicationFactor(string datacenter, int replicationFactor)
        {
            Datacenter = datacenter ?? throw new ArgumentNullException(nameof(datacenter));
            ReplicationFactor = replicationFactor;
            _hashCode = DatacenterReplicationFactor.ComputeHashCode(Datacenter, ReplicationFactor);
        }

        public string Datacenter { get; }

        public int ReplicationFactor { get; }
        
        public override bool Equals(object obj)
        {
            return obj.GetType() == GetType() && Equals((DatacenterReplicationFactor)obj);
        }

        public bool Equals(DatacenterReplicationFactor other)
        {
            return Datacenter == other.Datacenter &&
                   ReplicationFactor == other.ReplicationFactor;
        }

        public override int GetHashCode()
        {
            return _hashCode;
        }

        private static int ComputeHashCode(string datacenter, int replicationFactor)
        {
            return Utils.CombineHashCode(new object[] { datacenter, replicationFactor });
        }
    }
}