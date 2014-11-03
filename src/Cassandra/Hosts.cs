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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Linq;

namespace Cassandra
{
    internal class Hosts : IEnumerable<Host>
    {
        private readonly ConcurrentDictionary<IPAddress, Host> _hosts = new ConcurrentDictionary<IPAddress, Host>();
        private readonly IReconnectionPolicy _rp;
        public event Action<Host, DateTimeOffset> HostDown;

        public Hosts(IReconnectionPolicy rp)
        {
            _rp = rp;
        }

        public bool TryGet(IPAddress endpoint, out Host host)
        {
            return _hosts.TryGetValue(endpoint, out host);
        }

        public ICollection<Host> ToCollection()
        {
            return _hosts.Values;
        }

        public bool AddIfNotExistsOrBringUpIfDown(IPAddress ep)
        {
            if (!_hosts.ContainsKey(ep))
            {
                var h = new Host(ep, _rp);
                if (_hosts.TryAdd(ep, h))
                {
                    h.Down += OnHostDown;
                    return true;
                }
            }

            Host host;
            if (_hosts.TryGetValue(ep, out host))
            {
                return host.BringUpIfDown();
            }
            return false;
        }

        private void OnHostDown(Host h, DateTimeOffset nextUpTime)
        {
            if (HostDown != null)
            {
                HostDown(h, nextUpTime);
            }
        }

        public bool SetDownIfExists(IPAddress ep)
        {
            Host host;
            if (_hosts.TryGetValue(ep, out host))
            {
                return host.SetDown();
            }
            return false;
        }

        public void RemoveIfExists(IPAddress ep)
        {
            Host host;
            if (_hosts.TryRemove(ep, out host))
            {
                host.SetDown();
                host.Down -= OnHostDown;
            }
        }

        public IEnumerable<IPAddress> AllEndPointsToCollection()
        {
            return _hosts.Keys;
        }

        public IEnumerator<Host> GetEnumerator()
        {
            return _hosts.Values.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _hosts.Values.GetEnumerator();
        }
    }
}