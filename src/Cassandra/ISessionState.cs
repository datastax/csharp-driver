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

using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// Represents the state of a <see cref="ISession"/>.
    /// <para>Exposes information on the connections maintained by a Client at a specific time.</para>
    /// </summary>
    public interface ISessionState
    {
        /// <summary>
        /// The hosts to which the <see cref="ISession"/> is connected to, at the time this state was obtained.
        /// <para>
        /// Please note that this method really returns the hosts for which the session currently
        /// holds a connection pool. As such, it's unlikely but not impossible for a host to be listed
        /// in the output of this method but to have <see cref="GetOpenConnections"/> return 0, if the
        /// pool itself is created but no connections have been successfully opened yet.
        /// </para>
        /// </summary>
        /// <returns></returns>
        IReadOnlyCollection<Host> GetConnectedHosts();

        /// <summary>
        /// The number of open connections to a given host.
        /// </summary>
        /// <param name="host">The host to get open connections for.</param>
        /// <returns>
        /// The number of open connections to {@code host}. If the session is not connected to that host, 0 is returned.
        /// </returns>
        int GetOpenConnections(Host host);

        /// <summary>
        /// The number of queries that are currently being executed through a given host. 
        /// <para>
        /// This corresponds to the number of queries that have been sent (by the session this
        /// is a State of) to the server Host on one of its connections but haven't yet returned.
        /// In that sense this provides a sort of measure of how busy the connections to that node
        /// are (at the time the state was obtained at least).
        /// </para>
        /// </summary>
        /// <param name="host">The host to get in-flight queries for.</param>
        /// <returns>The number of executing queries to <see cref="Host"/> at the time the state was obtained.</returns>
        int GetInFlightQueries(Host host);
    }
}