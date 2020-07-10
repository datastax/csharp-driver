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
using Cassandra.Connections.Control;
using Cassandra.Serialization;

namespace Cassandra.Tests.Connections.TestHelpers
{
    internal class FakeTopologyRefresher : ITopologyRefresher
    {
        private readonly IInternalMetadata _internalMetadata;
        private readonly IDictionary<IPEndPoint, IRow> _hosts;

        public FakeTopologyRefresher(
            IInternalMetadata internalMetadata, IDictionary<IPEndPoint, IRow> hosts)
        {
            _internalMetadata = internalMetadata;
            _hosts = hosts;
        }

        public Task<Host> RefreshNodeListAsync(
            IConnectionEndPoint currentEndPoint, IConnection connection, ISerializer serializer)
        {
            foreach (var h in _hosts)
            {
                if (_internalMetadata.GetHost(h.Key) == null)
                {
                    var host = _internalMetadata.AddHost(h.Key);
                    host.SetInfo(h.Value);
                }
            }

            _internalMetadata.SetPartitioner("Murmur3Partitioner");
            return Task.FromResult(_internalMetadata.Hosts.First());
        }
    }
}