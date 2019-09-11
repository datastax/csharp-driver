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

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Providers.Null
{
    internal class NullMeterValue : IMeterValue
    {
        public static readonly IMeterValue Instance = new NullMeterValue();

        private NullMeterValue()
        {
        }

        public long Count => 0;

        public double FifteenMinuteRate => 0;

        public double FiveMinuteRate => 0;

        public double MeanRate => 0;

        public double OneMinuteRate => 0;

        public DriverTimeUnit RateUnit => default(DriverTimeUnit);
    }
}