using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Threading;
using Cassandra;

namespace Cassandra.Native
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
    public enum CassandraHostDistance
    {
        LOCAL,
        REMOTE,
        IGNORED
    }
    
    /**
     * A Cassandra node.
     *
     * This class keeps the informations the driver maintain on a given Cassandra node.
     */
    public class CassandraClusterHost
    {
        private readonly IPEndPoint address;

        private volatile string datacenter;
        private volatile string rack;

        private Timer reconnectionTimer;
        private ReconnectionSchedule reconnectionSchedule;

        public bool IsUp { get; private set; }

        public void SetDown() 
        {
            IsUp = false;
            reconnectionTimer.Change(reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
        }
        public void BringUp() 
        {
            reconnectionTimer.Change(Timeout.Infinite, Timeout.Infinite);
            IsUp = true;
        }

        // ClusterMetadata keeps one Host object per inet address, so don't use
        // that constructor unless you know what you do (use ClusterMetadata.getHost typically).
        public CassandraClusterHost(IPEndPoint address, ReconnectionSchedule reconnectionSchedule)
        {
            this.address = address;
            this.IsUp = true;
            this.reconnectionSchedule = reconnectionSchedule;
            this.reconnectionTimer = new Timer(reconectionProc, null, Timeout.Infinite, Timeout.Infinite);
        }

        void SetLocationInfo(string datacenter, string rack)
        {
            this.datacenter = datacenter;
            this.rack = rack;
        }

        /**
         * Returns the node address.
         *
         * @return the node {@link InetAddress}.
         */
        public IPEndPoint Address
        {
            get
            {
                return address;
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
                return datacenter;
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
                return rack;
            }
        }

        private void reconectionProc(object _)
        {
            BringUp();
        }
    }
}