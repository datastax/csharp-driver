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
            Host host,
            RequestErrorType errorType,
            RetryDecision.RetryDecisionType decision,
            RequestTrackingInfo r,
            Exception ex)
        {
            await _o1.OnNodeRequestErrorAsync(host, errorType, decision, r, ex).ConfigureAwait(false);
            await _o2.OnNodeRequestErrorAsync(host, errorType, decision, r, ex).ConfigureAwait(false);
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

        public async Task OnNodeStartAsync(Host host, RequestTrackingInfo requestTrackingInfo)
        {
            await _o1.OnNodeStartAsync(host, requestTrackingInfo).ConfigureAwait(false);
            await _o2.OnNodeStartAsync(host, requestTrackingInfo).ConfigureAwait(false);
        }

        public async Task OnNodeSuccessAsync(Host host, RequestTrackingInfo requestTrackingInfo)
        {
            await _o1.OnNodeSuccessAsync(host, requestTrackingInfo).ConfigureAwait(false);
            await _o2.OnNodeSuccessAsync(host, requestTrackingInfo).ConfigureAwait(false);
        }
    }
}
