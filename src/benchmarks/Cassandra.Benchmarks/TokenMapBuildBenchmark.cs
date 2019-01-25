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
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;

namespace Cassandra.Benchmarks
{
    [ClrJob(baseline: true), CoreJob]
    [RPlotExporter, RankColumn]
    [MemoryDiagnoser]
    public class TokenMapBuildBenchmark
    {
        private Host[] _hosts;
        private KeyspaceMetadata[] _keyspaces;
        private const int Seed = 123131;

        // multiple targets
        // [GlobalSetup(Target = nameof(BenchmarkA) + "," + nameof(BenchmarkC))]

        [GlobalSetup(Target = nameof(TokenMapBuildBenchmark.NineHosts_ThreeDatacenters_ThreeRacks_OneKeyspace_OneReplicationConfiguration))]
        public void Setup_NineHosts_ThreeDatacenters_ThreeRacks_OneKeyspace_OneReplicationConfiguration()
        {
            _hosts = TokenMapBenchmarkHelpers.GenerateHosts(3, 3, 1, 256);
            _keyspaces = TokenMapBenchmarkHelpers.GenerateKeyspaces(1, new IDictionary<string, int>[]
            {
                new Dictionary<string, int>
                {
                    {"dc0", 2},
                    {"dc1", 2},
                    {"dc2", 2}
                }
            });
        }

        [Benchmark]
        public void NineHosts_ThreeDatacenters_ThreeRacks_OneKeyspace_OneReplicationConfiguration()
        {
            var map = TokenMap.Build("Murmur3Partitioner", _hosts, _keyspaces);
        }

        [GlobalSetup(Target = nameof(TokenMapBuildBenchmark.NineHosts_ThreeDatacenters_ThreeRacks_OneThousandKeyspaces_OneReplicationConfiguration))]
        public void Setup_NineHosts_ThreeDatacenters_ThreeRacks_OneThousandKeyspaces_OneReplicationConfiguration()
        {
            _hosts = TokenMapBenchmarkHelpers.GenerateHosts(3, 3, 1, 256);
            _keyspaces = TokenMapBenchmarkHelpers.GenerateKeyspaces(1000, new IDictionary<string, int>[]
            {
                new Dictionary<string, int>
                {
                    {"dc0", 2},
                    {"dc1", 2},
                    {"dc2", 2}
                }
            });
        }

        [Benchmark]
        public void NineHosts_ThreeDatacenters_ThreeRacks_OneThousandKeyspaces_OneReplicationConfiguration()
        {
            var map = TokenMap.Build("Murmur3Partitioner", _hosts, _keyspaces);
        }

        [GlobalSetup(Target = nameof(TokenMapBuildBenchmark.TwentyHosts_TwoDatacenters_OneRack_OneKeyspace_OneReplicationConfiguration))]
        public void Setup_TwentyHosts_TwoDatacenters_OneRack_OneKeyspace_OneReplicationConfiguration()
        {
            _hosts = TokenMapBenchmarkHelpers.GenerateHosts(2, 1, 10, 256);
            _keyspaces = TokenMapBenchmarkHelpers.GenerateKeyspaces(1, new IDictionary<string, int>[]
            {
                new Dictionary<string, int>
                {
                    {"dc0", 3},
                    {"dc1", 3}
                }
            });
            var rnd = new Random(TokenMapBuildBenchmark.Seed);
            _hosts = _hosts.OrderBy(x => rnd.Next()).ToArray();
        }

        [Benchmark]
        public void TwentyHosts_TwoDatacenters_OneRack_OneKeyspace_OneReplicationConfiguration()
        {
            var map = TokenMap.Build("Murmur3Partitioner", _hosts, _keyspaces);
        }


        [GlobalSetup(Target = nameof(TokenMapBuildBenchmark.TwentyHosts_TwoDatacenters_OneRack_OneHundredKeyspaces_TenReplicationConfigurations))]
        public void Setup_TwentyHosts_TwoDatacenters_OneRack_OneHundredKeyspaces_TenReplicationConfigurations()
        {
            _hosts = TokenMapBenchmarkHelpers.GenerateHosts(2, 1, 10, 256);
            _keyspaces = TokenMapBenchmarkHelpers.GenerateKeyspaces(100, new IDictionary<string, int>[]
            {
                new Dictionary<string, int> {{"dc0", 3}, {"dc1", 3}}, 
                new Dictionary<string, int> {{"dc0", 3}},
                new Dictionary<string, int> {{"dc1", 2}, {"dc0", 1}},
                new Dictionary<string, int> {{"dc0", 1}, {"dc1", 2}},
                new Dictionary<string, int> {{"dc0", 2}, {"dc1", 2}}, 
                new Dictionary<string, int> {{"dc1", 3}},
                new Dictionary<string, int> {{"dc1", 1}, {"dc0", 1}},
                new Dictionary<string, int> {{"dc0", 1}, {"dc1", 1}},
                new Dictionary<string, int> {{"dc0", 1}, {"dc1", 3}},
                new Dictionary<string, int> {{"replication_factor", 3}}
            });
            var rnd = new Random(TokenMapBuildBenchmark.Seed);
            _hosts = _hosts.OrderBy(x => rnd.Next()).ToArray(); 
        }

        [Benchmark]
        public void TwentyHosts_TwoDatacenters_OneRack_OneHundredKeyspaces_TenReplicationConfigurations()
        {
            var map = TokenMap.Build("Murmur3Partitioner", _hosts, _keyspaces);
        }
        
        [GlobalSetup(Target = nameof(TokenMapBuildBenchmark.TwentyHosts_TwoDatacenters_OneRack_OneHundredKeyspaces_FiveNetworkTopologyReplicationConfigurations))]
        public void Setup_SixteenHosts_FourDatacenters_OneRack_OneHundredKeyspaces_FiveNetworkTopologyReplicationConfigurations()
        {
            _hosts = TokenMapBenchmarkHelpers.GenerateHosts(4, 1, 4, 256);
            _keyspaces = TokenMapBenchmarkHelpers.GenerateKeyspaces(100, new IDictionary<string, int>[]
            {
                new Dictionary<string, int> {{"dc0", 3}, {"dc1", 3}, {"dc2", 3}, {"dc3", 3}},
                new Dictionary<string, int> {{"dc0", 3}, {"dc1", 3}},
                new Dictionary<string, int> {{"dc0", 3}},
                new Dictionary<string, int> {{"dc1", 3}},
                new Dictionary<string, int> {{"dc2", 3}, {"dc3", 3}}
            });
            var rnd = new Random(TokenMapBuildBenchmark.Seed);
            _hosts = _hosts.OrderBy(x => rnd.Next()).ToArray(); 
        }

        [Benchmark]
        public void TwentyHosts_TwoDatacenters_OneRack_OneHundredKeyspaces_FiveNetworkTopologyReplicationConfigurations()
        {
            var map = TokenMap.Build("Murmur3Partitioner", _hosts, _keyspaces);
        }
    }
}