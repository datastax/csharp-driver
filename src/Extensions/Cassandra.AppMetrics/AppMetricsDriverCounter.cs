//
//       Copyright (C) 2019 DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System.Collections.Generic;

using App.Metrics;
using App.Metrics.Counter;

using Cassandra.Metrics.Abstractions;

namespace Cassandra.AppMetrics
{
    internal class AppMetricsDriverCounter : IDriverCounter
    {
        private readonly IMetrics _metrics;
        private readonly ICounter _counter;
        private readonly string _formattedContext;
        private readonly string _name;

        public AppMetricsDriverCounter(
            IMetrics metrics, ICounter counter, string formattedContext, string name, string fullName)
        {
            _metrics = metrics;
            _counter = counter;
            _formattedContext = formattedContext;
            _name = name;
            FullName = fullName;
        }

        public void Increment(long value)
        {
            _counter.Increment(value);
        }

        public void Decrement(long value)
        {
            _counter.Decrement(value);
        }

        public void Reset()
        {
            _counter.Reset();
        }
        
        public string FullName { get; }

        public long GetValue()
        {
            return _metrics.Snapshot.GetForContext(_formattedContext).Counters.ValueFor(_name).Count;
        }
    }
}