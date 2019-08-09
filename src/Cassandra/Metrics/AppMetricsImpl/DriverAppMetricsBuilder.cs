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
using System;
using App.Metrics;
using App.Metrics.Builder;

namespace Cassandra.Metrics.AppMetricsImpl
{
    internal class DriverAppMetricsBuilder : IDriverAppMetricsBuilder
    {
        public IMetricsBuilder MetricsBuilder { get; }
        public TimeSpan SchedulerDelay { get; private set; }

        public DriverAppMetricsBuilder()
        {
            MetricsBuilder = new MetricsBuilder();
            SchedulerDelay = TimeSpan.FromSeconds(1);
        }

        public IDriverAppMetricsBuilder WithReporters(Action<IMetricsReportingBuilder> reportersConfigurator)
        {
            reportersConfigurator(MetricsBuilder.Report);
            return this;
        }

        public IDriverAppMetricsBuilder WithSchedulerDelay(TimeSpan delay)
        {
            SchedulerDelay = delay;
            return this;
        }

        public IDriverAppMetricsBuilder WithAdvancedConfiguration(Action<IMetricsBuilder> metricsConfigurator)
        {
            metricsConfigurator(MetricsBuilder);
            return this;
        }
    }
}
#endif