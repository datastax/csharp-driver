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
}