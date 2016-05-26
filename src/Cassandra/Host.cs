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
using System.Collections.Generic;
using System.Net;
using System.Threading;

namespace Cassandra
{
    /// <summary>
    ///  A Cassandra node. This class keeps the informations the driver maintain on a
    ///  given Cassandra node.
    /// </summary>
    public class Host
    {
        private static readonly Logger Logger = new Logger(typeof(Host));
        private readonly IReconnectionPolicy _reconnectionPolicy;
        private long _isUpNow = 1;
        /// <summary>
        /// Used as a flag to limit the amount of connection pools attempting reconnection to 1.
        /// </summary>
        private int _isAttemptingReconnection;
        private long _nextUpTime;
        private IReconnectionSchedule _reconnectionSchedule;
        /// <summary>
        /// Event that gets raised when the host is set as DOWN (not available) by the driver, after being UP.
        /// It provides the delay for the next reconnection attempt.
        /// </summary>
        internal event Action<Host, long> Down;
        /// <summary>
        /// Event that gets raised when the host is considered as DOWN by the driver after checking: 
        /// a failed reconnection attempt or set as down after being up.
        /// It provides the delay for the next reconnection attempt.
        /// <remarks>
        /// It gets raised more frequently than <see cref="Down"/>, as it is raised per each failed reconnection attempt also.
        /// </remarks>
        /// </summary>
        internal event Action<Host, long> CheckedAsDown;
        /// <summary>
        /// Event that gets raised when the host is considered back UP (available for queries) by the driver.
        /// </summary>
        internal event Action<Host> Up;
        /// <summary>
        /// Event that gets raised when the host is being decommissioned from the cluster.
        /// </summary>
        internal event Action Remove;

        /// <summary>
        /// Determines if the host is UP for the driver
        /// </summary>
        public bool IsUp
        {
            get { return Interlocked.Read(ref _isUpNow) == 1L; }
        }

        /// <summary>
        /// Determines if the host can be considered as UP
        /// </summary>
        public bool IsConsiderablyUp
        {
            get { return Interlocked.Read(ref _isUpNow) == 1L || _nextUpTime <= DateTimeOffset.Now.Ticks; }
        }

        /// <summary>
        ///  Gets the node address.
        /// </summary>
        public IPEndPoint Address { get; private set; }

        /// <summary>
        /// Tokens assigned to the host
        /// </summary>
        internal IEnumerable<string> Tokens { get; set; }

        /// <summary>
        ///  Gets the name of the datacenter this host is part of. The returned
        ///  datacenter name is the one as known by Cassandra. Also note that it is
        ///  possible for this information to not be available. In that case this method
        ///  returns <c>null</c> and caller should always expect that possibility.
        /// </summary>
        public string Datacenter { get; internal set; }

        /// <summary>
        ///  Gets the name of the rack this host is part of. The returned rack name is
        ///  the one as known by Cassandra. Also note that it is possible for this
        ///  information to not be available. In that case this method returns
        ///  <c>null</c> and caller should always expect that possibility.
        /// </summary>
        public string Rack { get; private set; }

        /// <summary>
        /// The Cassandra version the host is running.
        /// <remarks>
        /// The value returned can be null if the information is unavailable.
        /// </remarks>
        /// </summary>
        public Version CassandraVersion { get; internal set; }

        public Host(IPEndPoint address, IReconnectionPolicy reconnectionPolicy)
        {
            Address = address;
            _reconnectionPolicy = reconnectionPolicy;
            _reconnectionSchedule = reconnectionPolicy.NewSchedule();
        }

        /// <summary>
        /// Sets the Host as Down
        /// </summary>
        public bool SetDown()
        {
            return SetDown(false);
        }

        internal bool SetDown(bool failedReconnection)
        {
            var wasUp = Interlocked.CompareExchange(ref _isUpNow, 0, 1) == 1;
            if (!wasUp && !failedReconnection)
            {
                return false;
            }
            //Host was UP or there was a failed reconnection attempt
            var delay = _reconnectionSchedule.NextDelayMs();
            Logger.Warning("Host {0} considered as DOWN. Reconnection delay {1}ms", Address, delay);
            Interlocked.Exchange(ref _nextUpTime, DateTimeOffset.Now.Ticks + (delay * TimeSpan.TicksPerMillisecond));
            Interlocked.Exchange(ref _isAttemptingReconnection, 0);
            //raise checked as down event
            if (CheckedAsDown != null)
            {
                CheckedAsDown(this, delay);
            }
            if (!wasUp)
            {
                return false;
            }
            //raise DOWN event
            if (Down != null)
            {
                Down(this, delay);
            }
            return true;
        }

        /// <summary>
        /// Sets the host as being attempting reconnection and returns true if the atomic operation succeeded.
        /// </summary>
        internal bool SetAttemptingReconnection()
        {
            return Interlocked.CompareExchange(ref _isAttemptingReconnection, 1, 0) == 0;
        }

        /// <summary>
        /// Returns true if the host was DOWN and it was set as UP
        /// </summary>
        public bool BringUpIfDown()
        {
            var wasUp = Interlocked.CompareExchange(ref _isUpNow, 1, 0);
            if (wasUp == 0)
            {
                Logger.Info("Host {0} is now UP", Address);
                Interlocked.Exchange(ref _reconnectionSchedule, _reconnectionPolicy.NewSchedule());
                Interlocked.Exchange(ref _isAttemptingReconnection, 0);
                if (Up != null)
                {
                    Up(this);
                }
                return true;
            }
            return false;
        }

        public void SetAsRemoved()
        {
            Logger.Info("Decommissioning node {0}", Address);
            Interlocked.Exchange(ref _isUpNow, 0);
            Interlocked.Exchange(ref _nextUpTime, long.MaxValue);
            if (Remove != null)
            {
                Remove();
            }
        }

        internal void SetLocationInfo(string datacenter, string rack)
        {
            Datacenter = datacenter;
            Rack = rack;
        }

        /// <summary>
        /// The hash value of the address of the host
        /// </summary>
        public override int GetHashCode()
        {
            return Address.GetHashCode();
        }
    }
}
