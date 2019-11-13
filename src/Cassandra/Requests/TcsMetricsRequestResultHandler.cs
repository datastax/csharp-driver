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

namespace Cassandra.Requests
{
    internal class TcsMetricsRequestResultHandler : IRequestResultHandler
    {
        private readonly IRequestObserver _requestObserver;
        private readonly TaskCompletionSource<RowSet> _taskCompletionSource;

        public TcsMetricsRequestResultHandler(IRequestObserver requestObserver)
        {
            _requestObserver = requestObserver;
            _taskCompletionSource = new TaskCompletionSource<RowSet>();
            _requestObserver.OnRequestStart();
        }

        public void TrySetResult(RowSet result)
        {
            if (_taskCompletionSource.TrySetResult(result))
            {
                _requestObserver.OnRequestFinish(null);
            }
        }

        public void TrySetException(Exception exception)
        {
            if (_taskCompletionSource.TrySetException(exception))
            {
                _requestObserver.OnRequestFinish(exception);
            }
        }

        public Task<RowSet> Task => _taskCompletionSource.Task;
    }
}