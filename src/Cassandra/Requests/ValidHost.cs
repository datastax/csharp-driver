//
//      Copyright (C) DataStax Inc.
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

namespace Cassandra.Requests
{
    internal class ValidHost
    {
        private ValidHost(Host host, HostDistance distance)
        {
            Host = host;
            Distance = distance;
        }

        public Host Host { get; }

        public HostDistance Distance { get; }

        /// <summary>
        /// Builds a <see cref="ValidHost"/> instance.
        /// </summary>
        /// <returns>Newly built instance if valid or <code>null</code> if not valid
        /// (e.g. the host is ignored or the driver sees it as down)</returns>
        public static ValidHost New(Host host, HostDistance distance)
        {
            if (distance == HostDistance.Ignored)
            {
                // We should not use an ignored host
                return null;
            }
            
            if (!host.IsUp)
            {
                // The host is not considered UP by the driver.
                // We could have filtered earlier by hosts that are considered UP, but we must
                // check the host distance first.
                return null;
            }

            return new ValidHost(host, distance);
        }
    }
}