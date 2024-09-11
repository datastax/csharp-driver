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

using System.Collections.Generic;
using System.Linq;
using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers.Composite
{
    internal class CompositeObserverFactory : IObserverFactory
    {
        private readonly IList<IObserverFactory> _factories;

        public CompositeObserverFactory(IList<IObserverFactory> factories)
        {
            _factories = factories;
        }
        public IConnectionObserver CreateConnectionObserver(Host host)
        {
            return new CompositeConnectionObserver(
                _factories.Select(x => x.CreateConnectionObserver(host)).ToList());
        }

        public IRequestObserver CreateRequestObserver()
        {
            return new CompositeRequestObserver(
                _factories.Select(x => x.CreateRequestObserver()).ToList());
        }
    }
}
