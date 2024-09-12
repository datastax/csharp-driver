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

using Cassandra.Metrics.Internal;
using Cassandra.Observers.Abstractions;

namespace Cassandra.Observers.Composite
{
    internal class CompositeObserverFactoryBuilder : IObserverFactoryBuilder
    {
        private readonly IObserverFactoryBuilder _b1;
        private readonly IObserverFactoryBuilder _b2;

        public CompositeObserverFactoryBuilder(IObserverFactoryBuilder b1, IObserverFactoryBuilder b2)
        {
            _b1 = b1;
            _b2 = b2;
        }

        public IObserverFactory Build(IMetricsManager manager)
        {
            return new CompositeObserverFactory(_b1.Build(manager), _b2.Build(manager));
        }
    }
}
