using System;
using System.Collections.Generic;
using System.Net;

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
    ///  only to hosts that are the less often queried). <p> However, if an host is
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

        private bool _isUpNow = true;
        private DateTime _nextUpTime;
        readonly IReconnectionPolicy _reconnectionPolicy;
        private IReconnectionSchedule _reconnectionSchedule;

        public bool IsConsiderablyUp
        {
            get
            {
                return _isUpNow || _nextUpTime <= DateTime.Now;
            }
        }

        public bool SetDown()
        {
            if (IsConsiderablyUp)
                _nextUpTime = DateTime.Now.AddMilliseconds(_reconnectionSchedule.NextDelayMs());
            if (_isUpNow)
            {
                _isUpNow = false;
                return true;
            }
            return false;
        }

        public bool BringUpIfDown()
        {
            _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            if (!_isUpNow)
            {
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

        class IPAddressComparer : IComparer<IPAddress>
        {
            public int Compare(IPAddress x, IPAddress y)
            {
                return x.ToString().CompareTo(y.ToString());
            }
        }

        private readonly SortedDictionary<IPAddress, Host> _hosts = new SortedDictionary<IPAddress, Host>(new IPAddressComparer());
       

        public Host this[IPAddress endpoint]
        {
            get
            {
                lock (_hosts)
                {
                    if (_hosts.ContainsKey(endpoint))
                        return _hosts[endpoint];
                    else
                        return null;
                }
            }
        }

        public ICollection<Host> All()
        {
            lock (_hosts)
                return new List<Host>(_hosts.Values);
        }

        public bool AddIfNotExistsOrBringUpIfDown(IPAddress ep)
        {
            lock (_hosts)
            {
                if (!_hosts.ContainsKey(ep))
                {
                    _hosts.Add(ep, new Host(ep, rp));
                    return true;
                }
                else
                    return _hosts[ep].BringUpIfDown();
            }
        }

        public bool SetDownIfExists(IPAddress ep)
        {
            lock (_hosts)
                if (_hosts.ContainsKey(ep))
                    return _hosts[ep].SetDown();
            return false;
        }

        public void RemoveIfExists(IPAddress ep)
        {
            lock (_hosts)
                if (_hosts.ContainsKey(ep))
                    _hosts.Remove(ep);
        }

        public IEnumerable<IPAddress> AllEndPoints()
        {
            lock (_hosts)
                return new List<IPAddress>(_hosts.Keys);
        }
    }
}