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
        private static readonly Logger Logger = new Logger(typeof(ControlConnection));
        private readonly IReconnectionPolicy _reconnectionPolicy;
        private volatile bool _isUpNow = true;
        private DateTimeOffset _nextUpTime;
        private IReconnectionSchedule _reconnectionSchedule;
        /// <summary>
        /// Event that gets raised when the host is considered as DOWN (not available) by the driver.
        /// It will provide the time were reconnection will be attempted
        /// </summary>
        internal event Action<Host, DateTimeOffset> Down;

        /// <summary>
        /// Determines if the host is UP for the driver
        /// </summary>
        public bool IsUp
        {
            get { return _isUpNow; }
        }

        /// <summary>
        /// Determines if the host can be considered as UP
        /// </summary>
        public bool IsConsiderablyUp
        {
            get { return _isUpNow || (_nextUpTime <= DateTimeOffset.Now); }
        }

        /// <summary>
        /// Determines if the host, due to the connection error can be resurrected if no other host is alive.
        /// </summary>
        public bool Resurrect { get; set; }

        /// <summary>
        ///  Gets the node address.
        /// </summary>
        public IPAddress Address { get; private set; }

        /// <summary>
        ///  Gets the name of the datacenter this host is part of. The returned
        ///  datacenter name is the one as known by Cassandra. Also note that it is
        ///  possible for this information to not be available. In that case this method
        ///  returns <c>null</c> and caller should always expect that possibility.
        /// </summary>
        public string Datacenter { get; private set; }

        /// <summary>
        ///  Gets the name of the rack this host is part of. The returned rack name is
        ///  the one as known by Cassandra. Also note that it is possible for this
        ///  information to not be available. In that case this method returns
        ///  <c>null</c> and caller should always expect that possibility.
        /// </summary>
        public string Rack { get; private set; }

        public Host(IPAddress address, IReconnectionPolicy reconnectionPolicy)
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
            if (IsConsiderablyUp)
            {
                Logger.Warning("Host " + this.Address.ToString() + " considered as DOWN");
                Thread.MemoryBarrier();
                _nextUpTime = DateTimeOffset.Now.AddMilliseconds(_reconnectionSchedule.NextDelayMs());
                if (Down != null)
                {
                    //Raise event
                    Down(this, _nextUpTime);
                }
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
