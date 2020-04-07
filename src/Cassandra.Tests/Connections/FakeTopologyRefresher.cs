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

using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Connections;

namespace Cassandra.Tests.Connections
{
    internal class FakeTopologyRefresher : ITopologyRefresher
    {
        private readonly Metadata _metadata;
        private readonly Configuration _config;
        private readonly IDictionary<IPEndPoint, IRow> _hosts;

        public FakeTopologyRefresher(Metadata metadata, Configuration config, IDictionary<IPEndPoint, IRow> hosts)
        {
            _metadata = metadata;
            _config = config;
            _hosts = hosts;
        }

        public Task<Host> RefreshNodeListAsync(IConnectionEndPoint currentEndPoint, IConnection connection, ProtocolVersion version)
        {
            foreach (var h in _hosts)
            {
                if (_metadata.GetHost(h.Key) == null)
                {
                    var host = _metadata.AddHost(h.Key);
                    host.SetInfo(h.Value);
                }
            }

            _metadata.Partitioner = "Murmur3Partitioner";
            return Task.FromResult(_metadata.Hosts.First());
        }
    }
}