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
using Cassandra.Tasks;

namespace Cassandra.Observers.Null
{
    internal class NullRequestObserver : IRequestObserver
    {
        public static readonly IRequestObserver Instance = new NullRequestObserver();

        private NullRequestObserver()
        {
        }

        public Task OnNodeRequestErrorAsync(RequestErrorType errorType, RetryDecision.RetryDecisionType decision, SessionRequestInfo r, NodeRequestInfo h, Exception ex)
        {
            return TaskHelper.Completed;
        }

        public Task OnNodeRequestErrorAsync(IRequestError error, SessionRequestInfo r, NodeRequestInfo nodeRequestInfo)
        {
            return TaskHelper.Completed;
        }

        public Task OnNodeStartAsync(SessionRequestInfo sessionRequestInfo, NodeRequestInfo h)
        {
            return TaskHelper.Completed;
        }

        public Task OnNodeSuccessAsync(SessionRequestInfo sessionRequestInfo, NodeRequestInfo h)
        {
            return TaskHelper.Completed;
        }

        public Task OnNodeRequestAbortedAsync(SessionRequestInfo sessionRequestInfo, NodeRequestInfo nodeRequestInfo)
        {
            return TaskHelper.Completed;
        }

        public Task OnRequestFailureAsync(Exception ex, SessionRequestInfo r)
        {
            return TaskHelper.Completed;
        }

        public Task OnRequestStartAsync(SessionRequestInfo r)
        {
            return TaskHelper.Completed;
        }

        public Task OnRequestSuccessAsync(SessionRequestInfo r)
        {
            return TaskHelper.Completed;
        }

        public void OnSpeculativeExecution(Host host, long delay)
        {
        }
    }
}