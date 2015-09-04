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
        private int _isUpNow = 1;
        private long _nextUpTime;
        private IReconnectionSchedule _reconnectionSchedule;
        private int _reconnectionDelayFlag;
        /// <summary>
        /// Event that gets raised when the host is considered as DOWN (not available) by the driver.
        /// It will provide the time were reconnection will be attempted
        /// </summary>
        internal event Action<Host, long> Down;
        /// <summary>
        /// Event that gets raised when the host is considered back UP (available for queries) by the driver.
        /// </summary>
        internal event Action<Host> Up;

        /// <summary>
        /// Determines if the host is UP for the driver
        /// </summary>
        public bool IsUp
        {
            get { return Thread.VolatileRead(ref _isUpNow) == 1; }
        }

        /// <summary>
        /// Determines if the host can be considered as UP
        /// </summary>
        public bool IsConsiderablyUp
        {
            get { return Thread.VolatileRead(ref _isUpNow) == 1 || Thread.VolatileRead(ref _nextUpTime) <= DateTimeOffset.Now.Ticks; }
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
            var wasUp = Interlocked.CompareExchange(ref _isUpNow, 0, 1);
            //it was up OR the reconnection time has passed
            if (wasUp == 1 || Thread.VolatileRead(ref _nextUpTime) <= DateTimeOffset.Now.Ticks)
            {
                if (Interlocked.CompareExchange(ref _reconnectionDelayFlag, 1, 0) != 0)
                {
                    //only allow 1 thread to set the next delay as it is not a thread-safe operation
                    return false;
                }
                try
                {
                    var delay = _reconnectionSchedule.NextDelayMs();
                    Logger.Warning("Host {0} considered as DOWN. Reconnection delay {1}ms", Address, delay);
                    _nextUpTime = DateTimeOffset.Now.Ticks + (delay*TimeSpan.TicksPerMillisecond);
                    if (Down != null)
                    {
                        //Raise event
                        Down(this, delay);
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _reconnectionDelayFlag, 0);
                }
                return true;
            }
            return false;
        }

        /// <summary>
        /// Returns true if the host was DOWN and it was set as UP
        /// </summary>
        public bool BringUpIfDown()
        {
            var wasUp = Interlocked.CompareExchange(ref _isUpNow, 1, 0);
            if (wasUp == 0)
            {
                Logger.Info("Host {0} is now UP");
                Interlocked.Exchange(ref _reconnectionSchedule, _reconnectionPolicy.NewSchedule());
                if (Up != null)
                {
                    Up(this);
                }
                return true;
            }
            return false;
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
