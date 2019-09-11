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
        private readonly string _context;
        private readonly string _name;

        public AppMetricsDriverGauge(IMetrics metrics, IGauge gauge, string context, string name)
        {
            _metrics = metrics;
            _gauge = gauge;
            _context = context;
            _name = name;
        }

        public double GetValue()
        {
            return _metrics.Snapshot.GetGaugeValue(_context, _name);
        }

        public void SetValue(double value)
        {
            _gauge.SetValue(value);
        }

        public void Dispose()
        {
        }
    }
}
#endif