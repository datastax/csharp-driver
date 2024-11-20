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

using Cassandra.Connections;
using Cassandra.Observers.Abstractions;
using Cassandra.Responses;

namespace Cassandra.Observers.Composite
{
    internal class CompositeOperationObserver : IOperationObserver
    {
        private readonly IOperationObserver _o1;
        private readonly IOperationObserver _o2;

        public CompositeOperationObserver(IOperationObserver o1, IOperationObserver o2)
        {
            _o1 = o1;
            _o2 = o2;
        }

        public void OnOperationSend(long requestSize, long timestamp)
        {
            _o1.OnOperationSend(requestSize, timestamp);
            _o2.OnOperationSend(requestSize, timestamp);
        }

        public void OnOperationReceive(IRequestError exception, Response response, long timestamp)
        {
            _o1.OnOperationReceive(exception, response, timestamp);
            _o2.OnOperationReceive(exception, response, timestamp);
        }
    }
}
