//
//       Copyright (C) DataStax Inc.
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

#if NETSTANDARD2_0

using App.Metrics.Histogram;

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Providers.AppMetrics
{
    internal class AppMetricsHistogramValue : IHistogramValue
    {
        private HistogramValue histogram;

        public AppMetricsHistogramValue(HistogramValue histogram)
        {
            this.histogram = histogram;
        }

        public long Count => histogram.Count;

        public double Sum => histogram.Sum;

        public double LastValue => histogram.LastValue;

        public double Max => histogram.Max;

        public double Mean => histogram.Mean;

        public double Median => histogram.Median;

        public double Min => histogram.Min;

        public double Percentile75 => histogram.Percentile75;

        public double Percentile95 => histogram.Percentile95;

        public double Percentile98 => histogram.Percentile98;

        public double Percentile99 => histogram.Percentile99;

        public double Percentile999 => histogram.Percentile999;

        public int SampleSize => histogram.SampleSize;

        public double StdDev => histogram.StdDev;
    }
}
#endif