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
            SessionRequestInfo r,
            NodeRequestInfo nodeRequestInfo,
            Exception ex)
        {
            return _requestTracker.OnNodeErrorAsync(r, nodeRequestInfo, ex);
        }

        public Task OnNodeRequestErrorAsync(IRequestError error, SessionRequestInfo r, NodeRequestInfo nodeRequestInfo)
        {
            return _requestTracker.OnNodeErrorAsync(r, nodeRequestInfo, error.Exception);
        }

        public Task OnRequestSuccessAsync(SessionRequestInfo r)
        {
            return _requestTracker.OnSuccessAsync(r);
        }

        public Task OnRequestFailureAsync(Exception ex, SessionRequestInfo r)
        {
            return _requestTracker.OnErrorAsync(r, ex);
        }

        public Task OnNodeRequestAbortedAsync(SessionRequestInfo sessionRequestInfo, NodeRequestInfo nodeRequestInfo)
        {
            return _requestTracker.OnNodeAborted(sessionRequestInfo, nodeRequestInfo);
        }

        public Task OnRequestStartAsync(SessionRequestInfo r)
        {
            return _requestTracker.OnStartAsync(r);
        }

        public void OnSpeculativeExecution(Host host, long delay)
        {
        }

        public Task OnNodeStartAsync(SessionRequestInfo sessionRequestInfo, NodeRequestInfo nodeRequestInfo)
        {
            return _requestTracker.OnNodeStartAsync(sessionRequestInfo, nodeRequestInfo);
        }

        public Task OnNodeSuccessAsync(SessionRequestInfo sessionRequestInfo, NodeRequestInfo nodeRequestInfo)
        {
            return _requestTracker.OnNodeSuccessAsync(sessionRequestInfo, nodeRequestInfo);
        }
    }
}
