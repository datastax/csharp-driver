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
using System.Collections.Generic;
using System.Linq;
using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers.Composite
{
    internal class CompositeConnectionObserver : IConnectionObserver
    {
        private readonly IList<IConnectionObserver> _observers;

        public CompositeConnectionObserver(IList<IConnectionObserver> observers)
        {
            _observers = observers;
        }

        public void OnBytesSent(long size)
        {
            foreach (var o in _observers)
            {
                o.OnBytesSent(size);
            }
        }

        public void OnBytesReceived(long size)
        {
            foreach (var o in _observers)
            {
                o.OnBytesReceived(size);
            }
        }

        public void OnErrorOnOpen(Exception exception)
        {
            foreach (var o in _observers)
            {
                o.OnErrorOnOpen(exception);
            }
        }

        public IOperationObserver CreateOperationObserver()
        {
            return new CompositeOperationObserver(_observers.Select(o => o.CreateOperationObserver()).ToList());
        }
    }
}
