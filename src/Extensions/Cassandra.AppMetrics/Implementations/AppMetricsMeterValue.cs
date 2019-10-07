﻿//
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

using App.Metrics.Meter;
using Cassandra.AppMetrics.MetricValues;
using Cassandra.Metrics.Abstractions;

namespace Cassandra.AppMetrics.Implementations
{
    internal class AppMetricsMeterValue : IAppMetricsMeterValue
    {
        private readonly MeterValue _rate;

        public AppMetricsMeterValue(MeterValue rate)
        {
            _rate = rate;
        }

        public long Count => _rate.Count;

        public double FifteenMinuteRate => _rate.FifteenMinuteRate;

        public double FiveMinuteRate => _rate.FiveMinuteRate;

        public double MeanRate => _rate.MeanRate;

        public double OneMinuteRate => _rate.OneMinuteRate;

        public DriverTimeUnit RateUnit => _rate.RateUnit.ToDriverTimeUnit();
    }
}