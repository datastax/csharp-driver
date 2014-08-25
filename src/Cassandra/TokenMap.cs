using System;
using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    internal class TokenMap
    {
        internal readonly TokenFactory Factory;
        private readonly IToken[] _ring;
        private readonly Dictionary<IToken, HashSet<IPEndPoint>> _tokenToCassandraClusterHosts;

        private TokenMap(TokenFactory factory, Dictionary<IToken, HashSet<IPEndPoint>> tokenToCassandraClusterHosts, List<IToken> ring)
        {
            Factory = factory;
            _tokenToCassandraClusterHosts = tokenToCassandraClusterHosts;
            _ring = ring.ToArray();
            Array.Sort(_ring);
        }

        public static TokenMap Build(String partitioner, Dictionary<IPEndPoint, HashSet<string>> allTokens)
        {
            TokenFactory factory = TokenFactory.GetFactory(partitioner);
            if (factory == null)
                return null;

            var tokenToCassandraClusterHosts = new Dictionary<IToken, HashSet<IPEndPoint>>();
            var allSorted = new HashSet<IToken>();

            foreach (KeyValuePair<IPEndPoint, HashSet<string>> entry in allTokens)
            {
                IPEndPoint cassandraClusterHost = entry.Key;
                foreach (string tokenStr in entry.Value)
                {
                    try
                    {
                        IToken t = factory.Parse(tokenStr);
                        allSorted.Add(t);
                        if (!tokenToCassandraClusterHosts.ContainsKey(t))
                            tokenToCassandraClusterHosts.Add(t, new HashSet<IPEndPoint>());
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

        public HashSet<IPEndPoint> GetReplicas(IToken token)
        {
            // Find the primary replica
            int i = Array.BinarySearch(_ring, token);
            if (i < 0)
            {
                i = (i + 1)*(-1);
                if (i >= _ring.Length)
                    i = 0;
            }

            return _tokenToCassandraClusterHosts[_ring[i]];
        }
    }
}