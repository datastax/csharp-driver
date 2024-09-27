// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Observers.Abstractions;
using Cassandra.SessionManagement;

namespace Cassandra.Requests
{
    internal interface IReprepareHandler
    {
        /// <summary>
        /// Sends the prepare request to all nodes have have an existing open connection. Will not attempt to send the request to hosts that were tried before (successfully or not).
        /// </summary>
        /// <returns></returns>
        Task ReprepareOnAllNodesWithExistingConnections(
            IInternalSession session, InternalPrepareRequest request, PrepareResult prepareResult, IRequestObserver observer, SessionRequestInfo sessionRequestInfo);

        Task ReprepareOnSingleNodeAsync(
            KeyValuePair<Host, IHostConnectionPool> poolKvp, PreparedStatement ps, IRequest request, SemaphoreSlim sem, bool throwException);

        Task ReprepareOnSingleNodeAsync(
            IRequestObserver observer, 
            SessionRequestInfo sessionRequestInfo, 
            KeyValuePair<Host, IHostConnectionPool> poolKvp, 
            PreparedStatement ps, 
            IRequest request, 
            SemaphoreSlim sem, 
            bool throwException);
    }
}