//
//      Copyright (C) 2012 DataStax Inc.
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

//based on http://blogs.msdn.com/b/nikos/archive/2011/03/14/how-to-implement-iasyncresult-in-another-way.aspx

namespace Cassandra
{
    internal class AsyncResult<TResult> : AsyncResultNoResult
    {
        // Field set when operation completes
        internal readonly int StreamId;
        private TResult _result;

        internal AsyncResult(int streamId, AsyncCallback asyncCallback, object state, object owner, string operationId, object sender,
                             object tag) :
                                 base(asyncCallback, state, owner, operationId, sender, tag)
        {
            StreamId = streamId;
        }

        internal void SetResult(TResult result)
        {
            _result = result;
        }

        public new static TResult End(IAsyncResult result, object owner, string operationId)
        {
            var asyncResult = result as AsyncResult<TResult>;
            if (asyncResult == null)
            {
                throw new ArgumentException(
                    "Result passed represents an operation not supported " +
                    "by this framework.",
                    "result");
            }

            // Wait until operation has completed 
            AsyncResultNoResult.End(result, owner, operationId);

            // Return the result (if above didn't throw)
            return asyncResult._result;
        }
    }
}