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
using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    /// <summary>
    ///  Exception thrown when a query cannot be performed because no host are
    ///  available. This exception is thrown if <ul> <li>either there is no host live
    ///  in the cluster at the moment of the query</li> <li>all host that have been
    ///  tried have failed due to a connection problem</li> </ul> For debugging
    ///  purpose, the list of hosts that have been tried along with the failure cause
    ///  can be retrieved using the <link>#errors</link> method.
    /// </summary>
    public class NoHostAvailableException : DriverException
    {
        /// <summary>
        ///  Gets the hosts tried along with descriptions of the error encountered while trying them. 
        /// </summary>
        public Dictionary<IPAddress, List<Exception>> Errors { get; private set; }

        public NoHostAvailableException(Dictionary<IPAddress, List<Exception>> errors)
            : base(MakeMessage(errors))
        {
            Errors = errors;
        }

        private static String MakeMessage(Dictionary<IPAddress, List<Exception>> errors)
        {
            var addrs = new List<string>();
            foreach (IPAddress err in errors.Keys)
                addrs.Add(err.ToString());

            return string.Format("None of the hosts tried for query are available (tried: {0})", string.Join(",", addrs.ToArray()));
        }
    }
}