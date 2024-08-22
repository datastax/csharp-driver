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

namespace Cassandra.Observers.Null
{
    internal class NullRequestObserver : IRequestObserver
    {
        public static readonly IRequestObserver Instance = new NullRequestObserver();

        private NullRequestObserver()
        {
        }

        public void OnNodeRequestError(Host host, RequestErrorType errorType, RetryDecision.RetryDecisionType decision)
        {
        }

        public Task OnNodeRequestErrorAsync(Host host, RequestErrorType errorType, RetryDecision.RetryDecisionType decision, RequestTrackingInfo r, Exception ex)
        {
            return Task.FromResult(0);
        }

        public Task OnNodeStartAsync(Host host, RequestTrackingInfo requestTrackingInfo)
        {
            return Task.FromResult(0);
        }

        public Task OnNodeSuccessAsync(Host host, RequestTrackingInfo requestTrackingInfo)
        {
            return Task.FromResult(0);
        }

        public Task OnRequestFailureAsync(Exception ex, RequestTrackingInfo r)
        {
            return Task.FromResult(0);
        }

        public Task OnRequestStartAsync(RequestTrackingInfo r)
        {
            return Task.FromResult(0);
        }

        public Task OnRequestSuccessAsync(RequestTrackingInfo r)
        {
            return Task.FromResult(0);
        }

        public void OnSpeculativeExecution(Host host, long delay)
        {
        }
    }
}