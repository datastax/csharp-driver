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

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Providers.Null
{
    internal class NullDriverMetricsProvider : IDriverMetricsProvider
    {
        public IDriverTimer Timer(string bucket, IMetric metric)
        {
            return NullDriverTimer.Instance;
        }
        
        public IDriverMeter Meter(string bucket, IMetric metric)
        {
            return NullDriverMeter.Instance;
        }

        public IDriverCounter Counter(string bucket, IMetric metric)
        {
            return NullDriverCounter.Instance;
        }

        public IDriverGauge Gauge(string bucket, IMetric metric, Func<double?> valueProvider)
        {
            return NullDriverGauge.Instance;
        }

        public IDriverGauge Gauge(string bucket, IMetric metric)
        {
            return NullDriverGauge.Instance;
        }

        public void ShutdownMetricsBucket(string bucket)
        {
        }
    }
}