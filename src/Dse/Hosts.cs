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
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Linq;
using Cassandra.Collections;

namespace Cassandra
{
    internal class Hosts : IEnumerable<Host>
    {
        private readonly CopyOnWriteDictionary<IPEndPoint, Host> _hosts = new CopyOnWriteDictionary<IPEndPoint, Host>();
        /// <summary>
        /// Event that gets triggered when a host is considered as DOWN (not available)
        /// </summary>
        internal event Action<Host> Down;
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

        /// <summary>
        /// Gets the total amount of hosts in the cluster
        /// </summary>
        internal int Count
        {
            get { return _hosts.Count; }
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
            var newHost = new Host(key);
            var host = _hosts.GetOrAdd(key, newHost);
            if (!ReferenceEquals(newHost, host))
            {
                //The host was not added, return the existing host
                return host;
            }
            //The node was added
            host.Down += OnHostDown;
            host.Up += OnHostUp;
            if (Added != null)
            {
                Added(newHost);
            }
            return host;
        }

        private void OnHostDown(Host sender)
        {
            if (Down != null)
            {
                Down(sender);
            }
        }

        private void OnHostUp(Host sender)
        {
            if (Up != null)
            {
                Up(sender);
            }
        }

        public void RemoveIfExists(IPEndPoint ep)
        {
            Host host;
            if (!_hosts.TryRemove(ep, out host))
            {
                //The host does not exists
                return;
            }
            host.Down -= OnHostDown;
            host.Up -= OnHostUp;
            host.SetAsRemoved();
            if (Removed != null)
            {
                Removed(host);
            }
        }

        public IEnumerable<IPEndPoint> AllEndPointsToCollection()
        {
            return _hosts.Keys;
        }

        public IEnumerator<Host> GetEnumerator()
        {
            return _hosts.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _hosts.Values.GetEnumerator();
        }
    }
}