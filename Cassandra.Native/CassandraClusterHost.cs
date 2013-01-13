using System;
using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    /**
     * The distance to a Cassandra node as assigned by a
     * {@link com.datastax.driver.core.policies.LoadBalancingPolicy} (through its {@code
     * distance} method).
     *
     * The distance assigned to an host influence how many connections the driver
     * maintains towards this host. If for a given host the assigned {@code HostDistance}
     * is {@code LOCAL} or {@code REMOTE}, some connections will be maintained by
     * the driver to this host. More active connections will be kept to
     * {@code LOCAL} host than to a {@code REMOTE} one (and thus well behaving
     * {@code LoadBalancingPolicy} should assign a {@code REMOTE} distance only to
     * hosts that are the less often queried).
     * <p>
     * However, if an host is assigned the distance {@code IGNORED}, no connection
     * to that host will maintained active. In other words, {@code IGNORED} should
     * be assigned to hosts that should not be used by this driver (because they
     * are in a remote datacenter for instance).
     */
    public enum HostDistance
    {
        Local,
        Remote,
        Ignored
    }

    /**
     * A Cassandra node.
     *
     * This class keeps the informations the driver maintain on a given Cassandra node.
     */
    public class Host
    {
        private readonly IPAddress _address;

        private string _datacenter;
        private string _rack;

        private bool _isUpNow = true;
        private DateTime _nextUpTime;
        readonly ReconnectionPolicy _reconnectionPolicy;
        private ReconnectionSchedule _reconnectionSchedule;

        public bool IsConsiderablyUp
        {
            get
            {
                return _isUpNow || _nextUpTime <= DateTime.Now;
            }
        }

        public void SetDown()
        {
            _isUpNow = false;
            _nextUpTime = DateTime.Now.AddMilliseconds(_reconnectionSchedule.NextDelayMs());
        }

        public void BringUpIfDown()
        {
            this._reconnectionSchedule = _reconnectionPolicy.NewSchedule();
            _isUpNow = true;
        }

        // ClusterMetadata keeps one Host object per inet address, so don't use
        // that constructor unless you know what you do (use ClusterMetadata.getHost typically).
        public Host(IPAddress address, ReconnectionPolicy reconnectionPolicy)
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

        /**
         * Returns the node address.
         *
         * @return the node {@link InetAddress}.
         */
        public IPAddress Address
        {
            get
            {
                return _address;
            }
        }

        /**
         * Returns the name of the datacenter this host is part of.
         *
         * The returned datacenter name is the one as known by Cassandra. Also note
         * that it is possible for this information to not be available. In that
         * case this method returns {@code null} and caller should always expect
         * that possibility.
         *
         * @return the Cassandra datacenter name.
         */
        public string Datacenter
        {
            get
            {
                return _datacenter;
            }
        }

        /**
         * Returns the name of the rack this host is part of.
         *
         * The returned rack name is the one as known by Cassandra. Also note that
         * it is possible for this information to not be available. In that case
         * this method returns {@code null} and caller should always expect that
         * possibility.
         *
         * @return the Cassandra rack name.
         */
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
        private readonly Dictionary<IPAddress, Host> _hosts = new Dictionary<IPAddress, Host>();

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

        public void AddIfNotExistsOrBringUpIfDown(IPAddress ep, ReconnectionPolicy rp)
        {
            lock (_hosts)
            {
                if (!_hosts.ContainsKey(ep))
                    _hosts.Add(ep, new Host(ep, rp));
                else
                    _hosts[ep].BringUpIfDown();
            }
        }

        public void SetDownIfExists(IPAddress ep)
        {
            lock (_hosts)
                if (_hosts.ContainsKey(ep))
                    _hosts[ep].SetDown();
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