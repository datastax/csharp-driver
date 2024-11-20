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
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Observers.Abstractions;

namespace Cassandra.Requests
{
    internal class TcsMetricsRequestResultHandler : IRequestResultHandler
    {
        private readonly IRequestObserver _requestObserver;
        private readonly TaskCompletionSource<RowSet> _taskCompletionSource;
        private long _done = 0;

        public TcsMetricsRequestResultHandler(IRequestObserver requestObserver)
        {
            _requestObserver = requestObserver;
            _taskCompletionSource = new TaskCompletionSource<RowSet>();
        }

        public async Task TrySetResultAsync(RowSet result, SessionRequestInfo sessionRequestInfo)
        {
            if (Interlocked.CompareExchange(ref _done, 1, 0) == 0)
            {
                await _requestObserver.OnRequestSuccessAsync(sessionRequestInfo).ConfigureAwait(false);
                _taskCompletionSource.SetResult(result);
            }
        }

        public async Task TrySetExceptionAsync(Exception exception, SessionRequestInfo sessionRequestInfo)
        {
            if (Interlocked.CompareExchange(ref _done, 1, 0) == 0)
            {
                await _requestObserver.OnRequestFailureAsync(exception, sessionRequestInfo).ConfigureAwait(false);
                _taskCompletionSource.SetException(exception);
            }
        }

        public Task<RowSet> Task => _taskCompletionSource.Task;
    }
}