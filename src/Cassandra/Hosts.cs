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
        /// <summary>
        /// Event that gets triggered when a host is considered as DOWN (not available)
        /// </summary>
        internal event Action<Host, DateTimeOffset> Down;
        /// <summary>
        /// Event that gets triggered when a host is considered back UP (available for queries)
        /// </summary>
        internal event Action<Host> Up;
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

        public bool TryGet(IPAddress endpoint, out Host host)
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
        public Host Add(IPAddress key)
        {
            var newHost = new Host(key, _rp);
            var host = _hosts.GetOrAdd(key, newHost);
            if (ReferenceEquals(newHost, host))
            {
                //The node was added
                host.Down += OnHostDown;
                host.Up += OnHostUp;
                if (Added != null)
                {
                    Added(newHost);
                }
            }
            return host;
        }

        private void OnHostDown(Host sender, DateTimeOffset nextUpTime)
        {
            if (Down != null)
            {
                Down(sender, nextUpTime);
            }
        }

        private void OnHostUp(Host sender)
        {
            if (Up != null)
            {
                Up(sender);
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
                host.Up -= OnHostUp;
                if (Removed != null)
                {
                    Removed(host);
                }
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