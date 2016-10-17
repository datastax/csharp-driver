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
    /// Represents a Cassandra node.
    /// </summary>
    public class Host
    {
        private static readonly Logger Logger = new Logger(typeof(Host));
        private long _isUpNow = 1;
        private int _distance = (int) HostDistance.Ignored;

        /// <summary>
        /// Event that gets raised when the host is set as DOWN (not available) by the driver, after being UP.
        /// It provides the delay for the next reconnection attempt.
        /// </summary>
        internal event Action<Host> Down;

        /// <summary>
        /// Event that gets raised when the host is considered back UP (available for queries) by the driver.
        /// </summary>
        internal event Action<Host> Up;

        /// <summary>
        /// Event that gets raised when the host is being decommissioned from the cluster.
        /// </summary>
        internal event Action Remove;

        /// <summary>
        /// Event that gets raised when there is a change in the distance, perceived by the host.
        /// </summary>
        internal event Action<HostDistance, HostDistance> DistanceChanged;

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
            get { return IsUp; }
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

        /// <summary>
        /// Creates a new instance of <see cref="Host"/>.
        /// </summary>
        // ReSharper disable once UnusedParameter.Local : Part of the public API
        public Host(IPEndPoint address, IReconnectionPolicy reconnectionPolicy) : this(address)
        {

        }
        
        internal Host(IPEndPoint address)
        {
            Address = address;
        }

        /// <summary>
        /// Sets the Host as Down.
        /// Returns false if it was already considered as Down by the driver.
        /// </summary>
        public bool SetDown()
        {
            var wasUp = Interlocked.CompareExchange(ref _isUpNow, 0, 1) == 1;
            if (!wasUp)
            {
                return false;
            }
            Logger.Warning("Host {0} considered as DOWN.", Address);
            if (Down != null)
            {
                Down(this);
            }
            return true;
        }

        /// <summary>
        /// Returns true if the host was DOWN and it was set as UP.
        /// </summary>
        public bool BringUpIfDown()
        {
            var wasUp = Interlocked.CompareExchange(ref _isUpNow, 1, 0) == 1;
            if (wasUp)
            {
                return false;
            }
            Logger.Info("Host {0} is now UP", Address);
            if (Up != null)
            {
                Up(this);
            }
            return true;
        }

        public void SetAsRemoved()
        {
            Logger.Info("Decommissioning node {0}", Address);
            Interlocked.Exchange(ref _isUpNow, 0);
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

        /// <summary>
        /// Updates the internal state representing the distance.
        /// </summary>
        internal void SetDistance(HostDistance distance)
        {
            var previousDistance = (HostDistance) Interlocked.Exchange(ref _distance, (int)distance);
            if (previousDistance != distance && DistanceChanged != null)
            {
                DistanceChanged(previousDistance, distance);
            }
        }
    }
}
