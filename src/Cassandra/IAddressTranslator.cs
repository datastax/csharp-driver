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

using System.Net;

namespace Cassandra
{
    /// <summary>
    ///     Translates IP addresses received from Cassandra nodes into locally queryable addresses.
    /// </summary>
    /// <remarks>
    ///     The driver auto-detect new Cassandra nodes added to the cluster through server side
    ///     pushed notifications and through checking the system tables. For each node, the address
    ///     the driver will receive will correspond to the address set as rpc_address in the node
    ///     yaml file. In most case, this is the correct address to use by the driver and that is
    ///     what is used by default. However, sometimes the addresses received through this
    ///     mechanism will either not be reachable directly by the driver or should not be the
    ///     preferred address to use to reach the node (for instance, the rpc_address set on
    ///     Cassandra nodes might be a private IP, but some clients may have to use a public IP,
    ///     or pass by a router to reach that node). This interface allows to deal with such cases,
    ///     by allowing to translate an address as sent by a Cassandra node to another address
    ///     to be used by the driver for connection.
    ///     Please note that the contact points addresses provided while creating the
    ///     <c>Cluster</c> instance are not "translated", only IP address retrieve from or sent
    ///     by Cassandra nodes to the driver are.
    /// </remarks>
    public interface IAddressTranslator
    {
        /// <summary>
        ///     Translates a Cassandra rpc_address to another address if necessary.
        /// </summary>
        /// <param name="address">
        ///     the address of a node as returned by Cassandra. Note that if the rpc_address of
        ///     a node has been configured to 0.0.0.0 server side, then the provided address will
        ///     be the node listen_address, <b>not</b> 0.0.0.0. Also note that the port for
        ///     <c>IPEndPoint</c> will always be the one set at Cluster construction time
        ///     (9042 by default).
        /// </param>
        /// <returns>
        ///     the address the driver should actually use to connect to the node. If the return is
        ///     <c>null</c>, then address will be used by the driver (it is thus equivalent to
        ///     returning address directly).
        /// </returns>
        IPEndPoint Translate(IPEndPoint address);
    }
}