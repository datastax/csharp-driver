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

namespace Cassandra.Observers.RequestTracker
{
    internal class RequestTrackerObserver : IRequestObserver
    {
        private readonly IRequestTracker _requestTracker;

        public RequestTrackerObserver(IRequestTracker requestTracker)
        {
            _requestTracker = requestTracker;
        }

        public Task OnNodeRequestErrorAsync(
            RequestErrorType errorType,
            RetryDecision.RetryDecisionType decision,
            RequestTrackingInfo r, 
            HostTrackingInfo hostTrackingInfo,
            Exception ex)
        {
            return _requestTracker.OnNodeErrorAsync(r, hostTrackingInfo, ex);
        }

        public Task OnNodeRequestErrorAsync(IRequestError error, RequestTrackingInfo r, HostTrackingInfo hostTrackingInfo)
        {
            return _requestTracker.OnNodeErrorAsync(r, hostTrackingInfo, error.Exception);
        }

        public Task OnRequestSuccessAsync(RequestTrackingInfo r)
        {
            return _requestTracker.OnSuccessAsync(r);
        }

        public Task OnRequestFailureAsync(Exception ex, RequestTrackingInfo r)
        {
            return _requestTracker.OnErrorAsync(r, ex);
        }

        public Task OnNodeRequestAbortedAsync(RequestTrackingInfo requestTrackingInfo, HostTrackingInfo hostTrackingInfo)
        {
            return _requestTracker.OnNodeAborted(requestTrackingInfo, hostTrackingInfo);
        }

        public Task OnRequestStartAsync(RequestTrackingInfo r)
        {
            return _requestTracker.OnStartAsync(r);
        }

        public void OnSpeculativeExecution(Host host, long delay)
        {
        }

        public Task OnNodeStartAsync(RequestTrackingInfo requestTrackingInfo, HostTrackingInfo hostTrackingInfo)
        {
            return _requestTracker.OnNodeStartAsync(requestTrackingInfo, hostTrackingInfo);
        }

        public Task OnNodeSuccessAsync(RequestTrackingInfo requestTrackingInfo, HostTrackingInfo hostTrackingInfo)
        {
            return _requestTracker.OnNodeSuccessAsync(requestTrackingInfo, hostTrackingInfo);
        }
    }
}
