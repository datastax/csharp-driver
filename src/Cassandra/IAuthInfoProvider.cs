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

using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    /// <summary>
    ///  Authentication informations provider to connect to Cassandra nodes. <p> The
    ///  authentication information themselves are just a key-value pairs. Which exact
    ///  key-value pairs are required depends on the authenticator set for the
    ///  Cassandra nodes.</p>
    /// </summary>
    /// 
    internal interface IAuthInfoProvider
        // only for protocol V1 Credentials support
    {
        /// <summary>
        ///  The authentication informations to use to connect to <c>host</c>.
        ///  Please note that if authentication is required, this method will be called to
        ///  initialize each new connection created by the driver. It is thus a good idea
        ///  to make sure this method returns relatively quickly.
        /// </summary>
        /// <param name="host"> the Cassandra host for which authentication information
        ///  are requested. </param>
        /// 
        /// <returns>The authentication informations to use.</returns>
        IDictionary<string, string> GetAuthInfos(IPAddress host);
    }
}