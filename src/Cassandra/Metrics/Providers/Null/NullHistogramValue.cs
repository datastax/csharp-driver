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

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Providers.Null
{
    internal class NullHistogramValue : IHistogramValue
    {
        public static readonly IHistogramValue Instance = new NullHistogramValue();

        private NullHistogramValue()
        {
        }

        public long Count => 0;

        public double Sum => 0;

        public double LastValue => 0;

        public double Max => 0;

        public double Mean => 0;

        public double Median => 0;

        public double Min => 0;

        public double Percentile75 => 0;

        public double Percentile95 => 0;

        public double Percentile98 => 0;

        public double Percentile99 => 0;

        public double Percentile999 => 0;

        public int SampleSize => 0;

        public double StdDev => 0;
    }
}