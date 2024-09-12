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

using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers.Composite
{
    internal class CompositeObserverFactory : IObserverFactory
    {
        private readonly IObserverFactory _f1;
        private readonly IObserverFactory _f2;

        public CompositeObserverFactory(IObserverFactory f1, IObserverFactory f2)
        {
            _f1 = f1;
            _f2 = f2;
        }
        public IConnectionObserver CreateConnectionObserver(Host host)
        {
            return new CompositeConnectionObserver(_f1.CreateConnectionObserver(host), _f2.CreateConnectionObserver(host));
        }

        public IRequestObserver CreateRequestObserver()
        {
            return new CompositeRequestObserver(_f1.CreateRequestObserver(), _f2.CreateRequestObserver());
        }
    }
}
