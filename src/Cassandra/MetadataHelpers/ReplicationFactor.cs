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

namespace Cassandra.MetadataHelpers
{
    internal class ReplicationFactor : IEquatable<ReplicationFactor>, IComparable<ReplicationFactor>
    {
        private ReplicationFactor(int allReplicas, int transientReplicas)
        {
            AllReplicas = allReplicas;
            TransientReplicas = transientReplicas;
            FullReplicas = allReplicas - transientReplicas;
        }

        public int AllReplicas { get; }

        public int TransientReplicas { get; }

        public int FullReplicas { get; }

        public bool HasTransientReplicas() => AllReplicas != FullReplicas;

        public override string ToString()
        {
            return AllReplicas + (HasTransientReplicas() ? "/" + TransientReplicas : "");
        }

        public static ReplicationFactor Parse(string rf)
        {
            if (rf == null)
            {
                throw new ArgumentNullException(nameof(rf));
            }

            var slashIndex = rf.IndexOf('/');
            if (slashIndex < 0)
            {
                return new ReplicationFactor(ReplicationFactor.ParseNumberOfReplicas(rf), 0);
            }

            var allPart = rf.Substring(0, slashIndex);
            var transientPart = rf.Substring(slashIndex + 1);
            var parsedAllPart = ReplicationFactor.ParseNumberOfReplicas(allPart);
            var parsedTransientPart = ReplicationFactor.ParseNumberOfReplicas(transientPart);
            
            return new ReplicationFactor(parsedAllPart, parsedTransientPart);
        }

        private static int ParseNumberOfReplicas(string numberOfReplicas)
        {
            if (!int.TryParse(numberOfReplicas, out var parsed))
            {
                throw new FormatException("Value of keyspace strategy option is in invalid format!");
            }

            return parsed;
        }

        public bool Equals(ReplicationFactor other)
        {
            if (other == null)
            {
                return false;
            }

            if (object.ReferenceEquals(this, other))
            {
                return true;
            }

            return AllReplicas == other.AllReplicas 
                   && TransientReplicas == other.TransientReplicas;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ReplicationFactor);
        }

        public override int GetHashCode()
        {
            return Utils.CombineHashCode(new object[] { AllReplicas, TransientReplicas });
        }

        public int CompareTo(ReplicationFactor other)
        {
            if (object.ReferenceEquals(this, other))
            {
                return 0;
            }

            if (other == null)
            {
                return 1;
            }

            var allReplicasComparison = AllReplicas.CompareTo(other.AllReplicas);
            return allReplicasComparison != 0 
                ? allReplicasComparison 
                : TransientReplicas.CompareTo(other.TransientReplicas);
        }
    }
}