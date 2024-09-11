﻿//
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

using Cassandra.Observers.Abstractions;
using Cassandra.Observers.Null;

namespace Cassandra.Observers.RequestTracker
{
    internal class RequestTrackerObserverFactory : IObserverFactory
    {
        private readonly IRequestTracker _tracker;

        public RequestTrackerObserverFactory(IRequestTracker tracker)
        {
            this._tracker = tracker;
        }

        public IConnectionObserver CreateConnectionObserver(Host host)
        {
            return NullConnectionObserver.Instance;
        }

        public IRequestObserver CreateRequestObserver()
        {
            return new RequestTrackerObserver(_tracker);
        }
    }
}
