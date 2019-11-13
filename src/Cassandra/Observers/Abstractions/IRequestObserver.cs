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
using Cassandra.Requests;

namespace Cassandra.Observers.Abstractions
{
    /// <summary>
    /// Exposes callbacks for events related to <see cref="IRequestExecution"/> and <see cref="IRequestHandler"/>.
    /// </summary>
    internal interface IRequestObserver
    {
        void OnSpeculativeExecution(Host host, long delay);

        void OnRequestError(Host host, RequestErrorType errorType, RetryDecision.RetryDecisionType decision);

        void OnRequestStart();

        void OnRequestFinish(Exception exception);
    }
}