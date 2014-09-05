//
//      Copyright (C) 2012-2014 DataStax Inc.
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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    internal class Hosts
    {
        private readonly ConcurrentDictionary<IPEndPoint, Host> _hosts = new ConcurrentDictionary<IPEndPoint, Host>();
        private readonly IReconnectionPolicy _rp;

        public Hosts(IReconnectionPolicy rp)
        {
            _rp = rp;
        }

        public bool TryGet(IPEndPoint endpoint, out Host host)
        {
            return _hosts.TryGetValue(endpoint, out host);
        }

        public ICollection<Host> ToCollection()
        {
            return new List<Host>(_hosts.Values);
        }

        public bool AddIfNotExistsOrBringUpIfDown(IPEndPoint ep)
        {
            if (!_hosts.ContainsKey(ep))
                if (_hosts.TryAdd(ep, new Host(ep, _rp)))
                    return true;

            Host host;
            if (_hosts.TryGetValue(ep, out host))
                return host.BringUpIfDown();
            return false;
        }

        public bool SetDownIfExists(IPEndPoint ep)
        {
            Host host;
            if (_hosts.TryGetValue(ep, out host))
                return host.SetDown();
            return false;
        }

        public void RemoveIfExists(IPEndPoint ep)
        {
            Host host;
            _hosts.TryRemove(ep, out host);
        }

        public IEnumerable<IPEndPoint> AllEndPointsToCollection()
        {
            return new List<IPEndPoint>(_hosts.Keys);
        }
    }
}