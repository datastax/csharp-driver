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
using Cassandra.Connections;
using Cassandra.Observers.Abstractions;
using Cassandra.Requests;

namespace Cassandra.Observers.Composite
{
    internal class CompositeRequestObserver : IRequestObserver
    {
        private readonly IRequestObserver _o1;
        private readonly IRequestObserver _o2;

        public CompositeRequestObserver(IRequestObserver o1, IRequestObserver o2)
        {
            _o1 = o1;
            _o2 = o2;
        }

        public async Task OnNodeRequestErrorAsync(
            RequestErrorType errorType,
            RetryDecision.RetryDecisionType decision,
            RequestTrackingInfo r, 
            HostTrackingInfo hostTrackingInfo,
            Exception ex)
        {
            await _o1.OnNodeRequestErrorAsync(errorType, decision, r, hostTrackingInfo, ex).ConfigureAwait(false);
            await _o2.OnNodeRequestErrorAsync(errorType, decision, r, hostTrackingInfo, ex).ConfigureAwait(false);
        }

        public async Task OnNodeRequestErrorAsync(IRequestError error, RequestTrackingInfo r, HostTrackingInfo hostTrackingInfo)
        {
            await _o1.OnNodeRequestErrorAsync(error, r, hostTrackingInfo).ConfigureAwait(false);
            await _o2.OnNodeRequestErrorAsync(error, r, hostTrackingInfo).ConfigureAwait(false);
        }

        public async Task OnRequestFailureAsync(Exception ex, RequestTrackingInfo r)
        {
            await _o1.OnRequestFailureAsync(ex, r).ConfigureAwait(false);
            await _o2.OnRequestFailureAsync(ex, r).ConfigureAwait(false);
        }

        public async Task OnRequestSuccessAsync(RequestTrackingInfo r)
        {
            await _o1.OnRequestSuccessAsync(r).ConfigureAwait(false);
            await _o2.OnRequestSuccessAsync(r).ConfigureAwait(false);
        }

        public async Task OnNodeRequestAbortedAsync(RequestTrackingInfo requestTrackingInfo, HostTrackingInfo hostTrackingInfo)
        {
            await _o1.OnNodeRequestAbortedAsync(requestTrackingInfo, hostTrackingInfo).ConfigureAwait(false);
            await _o2.OnNodeRequestAbortedAsync(requestTrackingInfo, hostTrackingInfo).ConfigureAwait(false);
        }

        public async Task OnRequestStartAsync(RequestTrackingInfo requestTrackingInfo)
        {
            await _o1.OnRequestStartAsync(requestTrackingInfo).ConfigureAwait(false);
            await _o2.OnRequestStartAsync(requestTrackingInfo).ConfigureAwait(false);
        }

        public void OnSpeculativeExecution(Host host, long delay)
        {
            _o1.OnSpeculativeExecution(host, delay);
            _o2.OnSpeculativeExecution(host, delay);
        }

        public async Task OnNodeStartAsync(RequestTrackingInfo requestTrackingInfo, HostTrackingInfo hostTrackingInfo)
        {
            await _o1.OnNodeStartAsync(requestTrackingInfo, hostTrackingInfo).ConfigureAwait(false);
            await _o2.OnNodeStartAsync(requestTrackingInfo, hostTrackingInfo).ConfigureAwait(false);
        }

        public async Task OnNodeSuccessAsync(RequestTrackingInfo requestTrackingInfo, HostTrackingInfo hostTrackingInfo)
        {
            await _o1.OnNodeSuccessAsync(requestTrackingInfo, hostTrackingInfo).ConfigureAwait(false);
            await _o2.OnNodeSuccessAsync(requestTrackingInfo, hostTrackingInfo).ConfigureAwait(false);
        }
    }
}
