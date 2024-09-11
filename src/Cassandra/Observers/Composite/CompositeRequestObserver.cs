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

        public void OnNodeRequestError(
            Host host,
            RequestErrorType errorType,
            RetryDecision.RetryDecisionType decision,
            RequestTrackingInfo r,
            Exception ex)
        {
            foreach (var observer in observers)
            {
                observer.OnNodeRequestError(host, errorType, decision, r, ex);
            }
        }

        public void OnRequestFailure(Exception ex, RequestTrackingInfo r)
        {
            foreach (var observer in observers)
            {
                observer.OnRequestFailure(ex, r);
            }
        }

        public void OnRequestSuccess(RequestTrackingInfo r)
        {
            foreach (var observer in observers)
            {
                observer.OnRequestSuccess(r);
            }
        }

        public void OnRequestStart(RequestTrackingInfo requestTrackingInfo)
        {
            foreach (var observer in observers)
            {
                observer.OnRequestStart(requestTrackingInfo);
            }
        }

        public void OnSpeculativeExecution(Host host, long delay)
        {
            foreach (var observer in observers)
            {
                observer.OnSpeculativeExecution(host, delay);
            }
        }

        public void OnNodeStart(Host host, RequestTrackingInfo requestTrackingInfo)
        {
            foreach (var observer in observers)
            {
                observer.OnNodeStart(host, requestTrackingInfo);
            }
        }

        public void OnNodeSuccess(Host host, RequestTrackingInfo requestTrackingInfo)
        {
            foreach (var observer in observers)
            {
                observer.OnNodeSuccess(host, requestTrackingInfo);
            }
        }
    }
}
