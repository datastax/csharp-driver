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
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra.Observers.Abstractions;
using Cassandra.Requests;

namespace Cassandra.Observers.Composite
{
    internal class CompositeRequestObserver : IRequestObserver
    {
        private readonly IEnumerable<IRequestObserver> observers;

        public CompositeRequestObserver(IEnumerable<IRequestObserver> observers)
        {
            this.observers = observers;
        }

        public async Task OnNodeRequestError(
            Host host,
            RequestErrorType errorType,
            RetryDecision.RetryDecisionType decision,
            RequestTrackingInfo r,
            Exception ex)
        {
            foreach (var observer in observers)
            {
                await observer.OnNodeRequestError(host, errorType, decision, r, ex).ConfigureAwait(false);
            }
        }

        public async Task OnRequestFailure(Exception ex, RequestTrackingInfo r)
        {
            foreach (var observer in observers)
            {
                await observer.OnRequestFailure(ex, r).ConfigureAwait(false);
            }
        }

        public async Task OnRequestSuccess(RequestTrackingInfo r)
        {
            foreach (var observer in observers)
            {
                await observer.OnRequestSuccess(r).ConfigureAwait(false);
            }
        }

        public async Task OnRequestStart(RequestTrackingInfo requestTrackingInfo)
        {
            foreach (var observer in observers)
            {
                await observer.OnRequestStart(requestTrackingInfo).ConfigureAwait(false);
            }
        }

        public void OnSpeculativeExecution(Host host, long delay)
        {
            foreach (var observer in observers)
            {
                observer.OnSpeculativeExecution(host, delay);
            }
        }

        public async Task OnNodeStart(Host host, RequestTrackingInfo requestTrackingInfo)
        {
            foreach (var observer in observers)
            {
                await observer.OnNodeStart(host, requestTrackingInfo).ConfigureAwait(false);
            }
        }

        public async Task OnNodeSuccess(Host host, RequestTrackingInfo requestTrackingInfo)
        {
            foreach (var observer in observers)
            {
                await observer.OnNodeSuccess(host, requestTrackingInfo).ConfigureAwait(false);
            }
        }
    }
}
