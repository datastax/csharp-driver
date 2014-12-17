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
        private readonly ConcurrentDictionary<IPEndPoint, Host> _hosts = new ConcurrentDictionary<IPEndPoint, Host>();
        private readonly IReconnectionPolicy _rp;
        /// <summary>
        /// Event that gets triggered when a new host has been added
        /// </summary>
        internal event Action<Host, DateTimeOffset> Down;
        /// <summary>
        /// Event that gets triggered when a new host has been added to the pool
        /// </summary>
        internal event Action<Host> Added;
        /// <summary>
        /// Event that gets triggered when a host has been removed
        /// </summary>
        internal event Action<Host> Removed;

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
            return _hosts.Values;
        }

        /// <summary>
        /// Adds the host if not exists
        /// </summary>
        public Host Add(IPEndPoint key)
        {
            var newHost = new Host(key, _rp);
            var host = _hosts.GetOrAdd(key, newHost);
            if (Object.ReferenceEquals(newHost, host) && Added != null)
            {
                //The node was added and there is an event handler
                //Fire the event
                Added(newHost);
            }
            return host;
        }

        private void OnHostDown(Host h, DateTimeOffset nextUpTime)
        {
            if (Down != null)
            {
                Down(h, nextUpTime);
            }
        }

        public bool SetDownIfExists(IPEndPoint ep)
        {
            Host host;
            if (_hosts.TryGetValue(ep, out host))
            {
                return host.SetDown();
            }
            return false;
        }

        public void RemoveIfExists(IPEndPoint ep)
        {
            Host host;
            if (_hosts.TryRemove(ep, out host))
            {
                host.SetDown();
                host.Down -= OnHostDown;
                if (Removed != null)
                {
                    Removed(host);
                }
            }
        }

        public IEnumerable<IPEndPoint> AllEndPointsToCollection()
        {
            return new List<IPEndPoint>(_hosts.Keys);
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