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
        /// <param name="sessionRequestInfo"></param>
        /// <returns>Contextual task.</returns>
        Task OnStartAsync(SessionRequestInfo sessionRequestInfo);

        /// <summary>
        /// Triggered when the session level request finishes successfully.
        /// </summary>
        /// <param name="sessionRequestInfo"></param>
        /// <returns>Contextual task.</returns>
        Task OnSuccessAsync(SessionRequestInfo sessionRequestInfo);

        /// <summary>
        /// Triggered when the session level request finishes unsuccessfully.
        /// </summary>
        /// <param name="sessionRequestInfo">Request contextual information.</param>
        /// <param name="ex">Request exception.</param>
        /// <returns>Contextual task.</returns>
        Task OnErrorAsync(SessionRequestInfo sessionRequestInfo, Exception ex);

        /// <summary>
        /// Triggered when the node level request finishes successfully.
        /// </summary>
        /// <param name="sessionRequestInfo">Request contextual information.</param>
        /// <param name="nodeRequestInfo">Struct with host contextual information.</param>
        /// <returns>Contextual task.</returns>
        Task OnNodeSuccessAsync(SessionRequestInfo sessionRequestInfo, NodeRequestInfo nodeRequestInfo);

        /// <summary>
        /// Triggered when the node request finishes unsuccessfully.
        /// </summary>
        /// <param name="sessionRequestInfo"><see cref="SessionRequestInfo"/> object with contextual information.</param>
        /// <param name="nodeRequestInfo">Struct with host contextual information.</param>
        /// <param name="ex">Exception information.</param>
        /// <returns>Contextual task.</returns>
        Task OnNodeErrorAsync(SessionRequestInfo sessionRequestInfo, NodeRequestInfo nodeRequestInfo, Exception ex);

        /// <summary>
        /// Triggered when the node request is aborted (e.g. pending speculative execution that was canceled due to another execution completing).
        /// </summary>
        /// <param name="sessionRequestInfo"><see cref="SessionRequestInfo"/> object with contextual information.</param>
        /// <param name="nodeRequestInfo">Struct with host contextual information.</param>
        /// <returns>Contextual task.</returns>
        Task OnNodeAborted(SessionRequestInfo sessionRequestInfo, NodeRequestInfo nodeRequestInfo);

        /// <summary>
        /// Triggered when the node request starts.
        /// </summary>
        /// <param name="sessionRequestInfo"><see cref="SessionRequestInfo"/> object with contextual information.</param>
        /// <param name="nodeRequestInfo">Struct with host contextual information.</param>
        /// <returns>Contextual task.</returns>
        Task OnNodeStartAsync(SessionRequestInfo sessionRequestInfo, NodeRequestInfo nodeRequestInfo);
    }
}
