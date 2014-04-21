//
//      Copyright (C) 2012 DataStax Inc.
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
﻿using System;
using System.Collections.Generic;
using System.Net;
using System.Collections.Concurrent;
using System.Threading;

namespace Cassandra
{
    /// <summary>
    ///  The distance to a Cassandra node as assigned by a
    ///  <link>com.datastax.driver.core.policies.LoadBalancingPolicy</link> (through
    ///  its <code>* distance</code> method). The distance assigned to an host
    ///  influence how many connections the driver maintains towards this host. If for
    ///  a given host the assigned <code>HostDistance</code> is <code>Local</code> or
    ///  <code>Remote</code>, some connections will be maintained by the driver to
    ///  this host. More active connections will be kept to <code>Local</code> host
    ///  than to a <code>Remote</code> one (and thus well behaving
    ///  <code>LoadBalancingPolicy</code> should assign a <code>Remote</code> distance
    ///  only to hosts that are the less often queried). &lt;p&gt; However, if an host is
    ///  assigned the distance <code>Ignored</code>, no connection to that host will
    ///  maintained active. In other words, <code>Ignored</code> should be assigned to
    ///  hosts that should not be used by this driver (because they are in a remote
    ///  datacenter for instance).
    /// </summary>
    public enum HostDistance
    {
        Local,
        Remote,
        Ignored
    }

    /// <summary>
    ///  A Cassandra node. This class keeps the informations the driver maintain on a
    ///  given Cassandra node.
    /// </summary>
    public class Host
    {
        private readonly IPAddress _address;

        private string _datacenter;
        private string _rack;

        private volatile bool _isUpNow = true;
        private DateTimeOffset _nextUpTime;
        readonly IReconnectionPolicy _reconnectionPolicy;
        private IReconnectionSchedule _reconnectionSchedule;

        public bool IsUp
        {
            get { return _isUpNow; }
        }

        public bool IsConsiderablyUp
        {
            get
            {
                return _isUpNow || (_nextUpTime <= DateTimeOffset.Now);
            }
        }

        public bool SetDown()
        {
            if (IsConsiderablyUp)
            {
                Thread.MemoryBarrier();
                _nextUpTime = DateTimeOffset.Now.AddMilliseconds(_reconnectionSchedule.NextDelayMs());
            }
            if (_isUpNow)
            {
                _isUpNow = false;
                return true;
            }
            return false;
        }

        public bool BringUpIfDown()
        {
            if (!_isUpNow)
            {
                Interlocked.Exchange(ref _reconnectionSchedule, _reconnectionPolicy.NewSchedule());
                _isUpNow = true;
                return true;
            }
            return false;
        }

        public Host(IPAddress address, IReconnectionPolicy reconnectionPolicy)
        {
            this._address = address;
            this._reconnectionPolicy = reconnectionPolicy;
            this._reconnectionSchedule = reconnectionPolicy.NewSchedule();
        }

        public void SetLocationInfo(string datacenter, string rack)
        {
            this._datacenter = datacenter;
            this._rack = rack;
        }

        /// <summary>
        ///  Gets the node address.
        /// </summary>
        public IPAddress Address
        {
            get
            {
                return _address;
            }
        }

        /// <summary>
        ///  Gets the name of the datacenter this host is part of. The returned
        ///  datacenter name is the one as known by Cassandra. Also note that it is
        ///  possible for this information to not be available. In that case this method
        ///  returns <code>null</code> and caller should always expect that possibility.
        /// </summary>
        public string Datacenter
        {
            get
            {
                return _datacenter;
            }
        }

        /// <summary>
        ///  Gets the name of the rack this host is part of. The returned rack name is
        ///  the one as known by Cassandra. Also note that it is possible for this
        ///  information to not be available. In that case this method returns
        ///  <code>null</code> and caller should always expect that possibility.
        /// </summary>
        public string Rack
        {
            get
            {
                return _rack;
            }
        }

    }

    internal class Hosts
    {
        IReconnectionPolicy rp;

        public Hosts(IReconnectionPolicy rp)
        {
            this.rp = rp;
        }

        //class IPAddressComparer : IComparer<IPAddress>
        //{
        //    public int Compare(IPAddress x, IPAddress y)
        //    {
        //        return x.ToString().CompareTo(y.ToString());
        //    }
        //}

        private readonly ConcurrentDictionary<IPAddress, Host> _hosts = new ConcurrentDictionary<IPAddress, Host>();

        public bool TryGet(IPAddress endpoint, out Host host)
        {
            return _hosts.TryGetValue(endpoint, out host);
        }

        public ICollection<Host> ToCollection()
        {
            return new List<Host>(_hosts.Values);
        }

        public bool AddIfNotExistsOrBringUpIfDown(IPAddress ep)
        {
            if (!_hosts.ContainsKey(ep))
                if (_hosts.TryAdd(ep, new Host(ep, rp)))
                    return true;

            Host host;
            if (_hosts.TryGetValue(ep, out host))
                return host.BringUpIfDown();
            else
                return false;
        }

        public bool SetDownIfExists(IPAddress ep)
        {
            Host host;
            if (_hosts.TryGetValue(ep, out host))
                return host.SetDown();
            else
                return false;
        }

        public void RemoveIfExists(IPAddress ep)
        {
            Host host;
            _hosts.TryRemove(ep, out host);
        }

        public IEnumerable<IPAddress> AllEndPointsToCollection()
        {
            return new List<IPAddress>(_hosts.Keys);
        }
    }
}