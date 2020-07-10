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
using System.Collections.Generic;
using System.Net;
using Cassandra.Connections.Control;

namespace Cassandra.Tests.Connections.TestHelpers
{
    internal class FakeTopologyRefresherFactory : ITopologyRefresherFactory
    {
        private readonly IDictionary<IPEndPoint, IRow> _rows;

        public FakeTopologyRefresherFactory(IDictionary<IPEndPoint, IRow> rows)
        {
            _rows = rows;
        }
        
        public FakeTopologyRefresherFactory(ICollection<Host> hosts)
        {
            var rows = new Dictionary<IPEndPoint, IRow>();
            foreach (var h in hosts)
            {
                if (rows.ContainsKey(h.Address))
                {
                    rows.Remove(h.Address);
                }

                rows.Add(
                    h.Address,
                    TestHelper.CreateRow(new Dictionary<string, object>
                    {
                        { "data_center", h.Datacenter },
                        { "rack", h.Rack },
                        { "tokens", h.Tokens },
                        { "release_version", h.CassandraVersion?.ToString() ?? "3.11.6" },
                        { "host_id", h.HostId }
                    }));
            }
            _rows = rows;
        }

        public ITopologyRefresher Create(IInternalMetadata internalMetadata, Configuration config)
        {
            return new FakeTopologyRefresher(internalMetadata, _rows);
        }
    }
}