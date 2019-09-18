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

using System.Collections;
using System.Collections.Generic;

using App.Metrics;
using App.Metrics.Gauge;

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Providers.AppMetrics
{
    // todo(sivukhin, 07.08.2019): Dispose gauges somehow?
    internal class AppMetricsDriverGauge : IDriverGauge
    {
        private readonly IMetrics _metrics;
        private readonly IGauge _gauge;
        private readonly string _formattedContext;

        public AppMetricsDriverGauge(IMetrics metrics, IGauge gauge, IEnumerable<string> context, string formattedContext, string name)
        {
            _metrics = metrics;
            _gauge = gauge;
            Context = context;
            _formattedContext = formattedContext;
            MetricName = name;
        }

        public IEnumerable<string> Context { get; }

        public string MetricName { get; }

        public double? GetValue()
        {
            var value = _metrics.Snapshot.GetGaugeValue(_formattedContext, MetricName);
            return double.IsNaN(value) ? null : (double?)value;
        }
        
        public void Dispose()
        {
        }
    }
}
#endif