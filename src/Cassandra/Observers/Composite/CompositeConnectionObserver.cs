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
using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers.Composite
{
    internal class CompositeConnectionObserver : IConnectionObserver
    {
        private readonly IConnectionObserver _o1;
        private readonly IConnectionObserver _o2;
        public CompositeConnectionObserver(IConnectionObserver o1, IConnectionObserver o2)
        {
            _o1 = o1;
            _o2 = o2;
        }

        public void OnBytesSent(long size)
        {
            _o1.OnBytesSent(size);
            _o2.OnBytesSent(size);
        }

        public void OnBytesReceived(long size)
        {
            _o1.OnBytesReceived(size);
            _o2.OnBytesReceived(size);
        }

        public void OnErrorOnOpen(Exception exception)
        {
            _o1.OnErrorOnOpen(exception);
            _o2.OnErrorOnOpen(exception);
        }

        public IOperationObserver CreateOperationObserver()
        {
            return new CompositeOperationObserver(_o1.CreateOperationObserver(), _o2.CreateOperationObserver());
        }
    }
}
