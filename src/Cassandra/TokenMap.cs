using System;
using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    internal class TokenMap
    {

        private readonly Dictionary<IToken, HashSet<IPAddress>> _tokenToCassandraClusterHosts;
        private readonly IToken[] _ring;
        internal readonly TokenFactory Factory;

        private TokenMap(TokenFactory factory, Dictionary<IToken, HashSet<IPAddress>> tokenToCassandraClusterHosts, List<IToken> ring)
        {
            this.Factory = factory;
            this._tokenToCassandraClusterHosts = tokenToCassandraClusterHosts;
            this._ring = ring.ToArray();
            Array.Sort(this._ring);
        }

        public static TokenMap Build(String partitioner, Dictionary<IPAddress, HashSet<string>> allTokens)
        {

            TokenFactory factory = TokenFactory.GetFactory(partitioner);
            if (factory == null)
                return null;

            Dictionary<IToken, HashSet<IPAddress>> tokenToCassandraClusterHosts = new Dictionary<IToken, HashSet<IPAddress>>();
            HashSet<IToken> allSorted = new HashSet<IToken>();

            foreach (var entry in allTokens)
            {
                var cassandraClusterHost = entry.Key;
                foreach (string tokenStr in entry.Value)
                {
                    try
                    {
                        IToken t = factory.Parse(tokenStr);
                        allSorted.Add(t);
                        if (!tokenToCassandraClusterHosts.ContainsKey(t))
                            tokenToCassandraClusterHosts.Add(t, new HashSet<IPAddress>());
                        tokenToCassandraClusterHosts[t].Add(cassandraClusterHost);
                    }
                    catch (ArgumentException)
                    {
                        // If we failed parsing that token, skip it
                    }
                }
            }
            return new TokenMap(factory, tokenToCassandraClusterHosts, new List<IToken>(allSorted));
        }

        public HashSet<IPAddress> GetReplicas(IToken token)
        {

            // Find the primary replica
            int i = Array.BinarySearch(_ring,token);
            if (i < 0)
            {
                i = (i + 1) * (-1);
                if (i >= _ring.Length)
                    i = 0;
            }

            return _tokenToCassandraClusterHosts[_ring[i]];
        }
    }
}