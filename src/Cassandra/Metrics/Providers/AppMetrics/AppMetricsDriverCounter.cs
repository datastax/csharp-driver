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

#if NETSTANDARD2_0

using System.Collections.Generic;
using App.Metrics;
using App.Metrics.Counter;

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Providers.AppMetrics
{
    internal class AppMetricsDriverCounter : IDriverCounter
    {
        private readonly IMetrics _metrics;
        private readonly ICounter _counter;
        private readonly string _formattedContext;

        public AppMetricsDriverCounter(
            IMetrics metrics, ICounter counter, IEnumerable<string> context, string formattedContext, string name)
        {
            _metrics = metrics;
            _counter = counter;
            Context = context;
            _formattedContext = formattedContext;
            MetricName = name;
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

        public IEnumerable<string> Context { get; }

        public string MetricName { get; }

        public long GetValue()
        {
            return _metrics.Snapshot.GetCounterValue(_formattedContext, MetricName).Count;
        }
    }
}
#endif