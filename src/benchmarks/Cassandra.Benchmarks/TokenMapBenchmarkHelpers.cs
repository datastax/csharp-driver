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

using System.Collections.Generic;
using Cassandra.Tests;

namespace Cassandra.Benchmarks
{
    internal class TokenMapBenchmarkHelpers
    {
        public static Host[] GenerateHosts(int numberOfDcs, int numberOfRacksPerDc, int hostsPerRack, int numTokens)
        {
            var hosts = new List<Host>();
            var hostsAddedToRack = 0;
            var racksAddedToDc = 0;
            var dcsAdded = 0;
            var currentDcStr = $"dc{dcsAdded}";
            var currentRackStr = $"dc{dcsAdded}_rack{racksAddedToDc}";

            var numberOfHosts = hostsPerRack * numberOfRacksPerDc * numberOfDcs;

            for (var i = 0; i < numberOfHosts; i++)
            {
                hosts.Add(TestHelper.CreateHost(
                    $"192.168.0.{i}", currentDcStr, currentRackStr, TokenMapBenchmarkHelpers.GenerateAssignedTokens(i, numTokens)));
                hostsAddedToRack++;

                if (hostsAddedToRack >= hostsPerRack)
                {
                    if (racksAddedToDc < numberOfRacksPerDc - 1)
                    {
                        racksAddedToDc++;
                        currentRackStr = $"dc{dcsAdded}_rack{racksAddedToDc}";
                        hostsAddedToRack = 0;
                    }
                    else if (dcsAdded < numberOfDcs - 1)
                    {
                        dcsAdded++;
                        currentRackStr = $"dc{dcsAdded}_rack{racksAddedToDc}";
                        currentDcStr = $"dc{dcsAdded}";
                        racksAddedToDc = 0;
                        hostsAddedToRack = 0;
                    }
                    else
                    {
                        currentRackStr = $"dc{dcsAdded}_rack0";
                    }
                }
            }

            return hosts.ToArray();
        }

        public static HashSet<string> GenerateAssignedTokens(int initialToken, int numberTokens)
        {
            var set = new HashSet<string>();
            for (var i = 0; i < numberTokens; i++)
            {
                set.Add(initialToken.ToString());
                initialToken += 1000;
            }

            return set;
        }

        public static KeyspaceMetadata[] GenerateKeyspaces(int numberOfKeyspaces, IDictionary<string, int>[] uniqueReplicationOptions)
        {
            var keyspaces = new List<KeyspaceMetadata>();
            for (var i = 0; i < numberOfKeyspaces; i++)
            {
                var replicationOptionsIndex = i % uniqueReplicationOptions.Length;
                var replicationOptions = uniqueReplicationOptions[replicationOptionsIndex];
                var isSimpleStrategy = replicationOptions.ContainsKey("replication_factor");
                var strategy = isSimpleStrategy
                    ? ReplicationStrategies.SimpleStrategy
                    : ReplicationStrategies.NetworkTopologyStrategy;
                keyspaces.Add(new KeyspaceMetadata(null, $"ks{i}", true, strategy, new Dictionary<string, int>(replicationOptions)));
            }

            return keyspaces.ToArray();
        }
    }
}