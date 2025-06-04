using System;
using System.Collections.Generic;

namespace Cassandra
{
    public class HostShardPair
    {
        public Guid HostID { get; }
        public int Shard { get; }

        public HostShardPair(Guid host, int shard)
        {
            HostID = host;
            Shard = shard;
        }

        public override string ToString() => $"HostShardPair{{host={HostID}, shard={Shard}}}";
    }

    public class Tablet : IComparable<Tablet>, IEquatable<Tablet>
    {
        public long FirstToken { get; }
        public long LastToken { get; }
        public List<HostShardPair> Replicas { get; }

        public Tablet(long firstToken, long lastToken, List<HostShardPair> replicas)
        {
            FirstToken = firstToken;
            LastToken = lastToken;
            Replicas = replicas;
        }

        public int CompareTo(Tablet other) => LastToken.CompareTo(other.LastToken);

        public override bool Equals(object obj) => Equals(obj as Tablet);

        public bool Equals(Tablet other)
        {
            return other != null &&
                    FirstToken == other.FirstToken &&
                    LastToken == other.LastToken &&
                    EqualityComparer<List<HostShardPair>>.Default.Equals(Replicas, other.Replicas);
        }

        public override int GetHashCode()
        {
            // Manual hash code implementation for .NET Standard 2.0
            int hash = 17;
            hash = hash * 23 + FirstToken.GetHashCode();
            hash = hash * 23 + LastToken.GetHashCode();
            hash = hash * 23 + (Replicas != null ? Replicas.GetHashCode() : 0);
            return hash;
        }

        public override string ToString()
        {
            return $"Tablet{{firstToken={FirstToken}, lastToken={LastToken}, replicas={Replicas}}}";
        }
    }
}
