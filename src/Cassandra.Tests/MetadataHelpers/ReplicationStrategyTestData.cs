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

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.MetadataHelpers;
using Cassandra.Tests.TestHelpers;

namespace Cassandra.Tests.MetadataHelpers
{
    internal class ReplicationStrategyTestData
    {
        public IReadOnlyList<IToken> Ring { get; set; }

        public IReadOnlyDictionary<IToken, Host> PrimaryReplicas { get; set; }

        public int NumberOfHostsWithTokens { get; set; }

        public IReadOnlyDictionary<string, DatacenterInfo> Datacenters { get; set; }

        public static ReplicationStrategyTestData Create(
            int numberOfHostsPerRack = 3, int numberOfDcs = 3, int numberOfRacksPerDc = 3, int numberOfTokensPerHost = 10)
        {
            var hosts = new Dictionary<string, Tuple<Dictionary<string, List<Host>>, DatacenterInfo>>();
            var allTokens = new List<IToken>();
            var primaryReplicas = new Dictionary<IToken, Host>();

            foreach (var dc in Enumerable.Range(1, numberOfDcs))
            {
                hosts.Add($"dc{dc}", new Tuple<Dictionary<string, List<Host>>, DatacenterInfo>(new Dictionary<string, List<Host>>(), new DatacenterInfo { HostLength = numberOfHostsPerRack * numberOfRacksPerDc }));
                foreach (var rack in Enumerable.Range(1, numberOfRacksPerDc))
                {
                    hosts[$"dc{dc}"].Item1.Add($"rack{rack}", new List<Host>());
                    hosts[$"dc{dc}"].Item2.AddRack($"rack{rack}");
                    foreach (var host in Enumerable.Range(1, numberOfHostsPerRack))
                    {
                        var tokensList = new List<IToken>();
                        var tokensStrList = new List<string>();
                        for (var i = 1; i <= numberOfTokensPerHost; i++)
                        {
                            var token = new M3PToken(dc * 1000000 + rack * 100000 + host * 10000 + 1000 * i);
                            tokensList.Add(token);
                            tokensStrList.Add(i.ToString());
                        }
                        allTokens.AddRange(tokensList);

                        var newHost = TestHostFactory.Create(ipAddress: $"127.{dc}.{rack}.{host}", dc: $"dc{dc}", rack: $"rack{rack}", tokens: tokensStrList);
                        hosts[$"dc{dc}"].Item1[$"rack{rack}"].Add(newHost);

                        foreach (var token in tokensList)
                        {
                            primaryReplicas.Add(token, newHost);
                        }
                    }
                }
            }

            return new ReplicationStrategyTestData
            {
                Ring = allTokens,
                Datacenters = hosts.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.Item2),
                NumberOfHostsWithTokens = numberOfHostsPerRack * numberOfRacksPerDc * numberOfDcs,
                PrimaryReplicas = primaryReplicas
            };
        }
    }
}