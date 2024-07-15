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

using System;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// A request tracker with triggerd events for Session and Node lvel requests.
    /// </summary>
    public interface IRequestTracker
    {
        /// <summary>
        /// Triggered when the request starts.
        /// </summary>
        /// <param name="request"></param>
        /// <returns>Contextual task.</returns>
        Task OnStartAsync(RequestTrackingInfo request);

        /// <summary>
        /// Triggered when the session level request finishes successfully.
        /// </summary>
        /// <param name="request"></param>
        /// <returns>Contextual task.</returns>
        Task OnSuccessAsync(RequestTrackingInfo request);

        /// <summary>
        /// Triggered when the session level request finishes unsuccessfully.
        /// </summary>
        /// <param name="request">Request contextual information.</param>
        /// <param name="ex">Request exception.</param>
        /// <returns>Contextual task.</returns>
        Task OnErrorAsync(RequestTrackingInfo request, Exception ex);

        /// <summary>
        /// Triggered when the node level request finishes successfully.
        /// </summary>
        /// <param name="request">Request contextual information.</param>
        /// <param name="hostInfo">Struct with host contextual information.</param>
        /// <returns>Contextual task.</returns>
        Task OnNodeSuccessAsync(RequestTrackingInfo request, HostTrackingInfo hostInfo);

        /// <summary>
        /// Triggered when the session node request finishes unsuccessfully.
        /// </summary>
        /// <param name="request"><see cref="RequestTrackingInfo"/> object with contextual information.</param>
        /// <param name="hostInfo">Struct with host contextual information.</param>
        /// <param name="ex">Exception information.</param>
        /// <returns>Contextual task.</returns>
        Task OnNodeErrorAsync(RequestTrackingInfo request, HostTrackingInfo hostInfo, Exception ex);
    }
}
