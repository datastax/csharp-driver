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

using App.Metrics.Histogram;

namespace Cassandra.AppMetrics.MetricValues
{
    /// <summary>
    /// Histogram value based on <see cref="HistogramValue"/>.
    /// </summary>
    public interface IAppMetricsHistogramValue
    {
        long Count { get; }

        double Sum { get; }
        
        double LastValue { get; }

        double Max { get; }

        double Mean { get; }

        double Median { get; }

        double Min { get; }
        
        double Percentile75 { get; }

        double Percentile95 { get; }

        double Percentile98 { get; }

        double Percentile99 { get; }

        double Percentile999 { get; }

        int SampleSize { get; }

        double StdDev { get; }
    }
}