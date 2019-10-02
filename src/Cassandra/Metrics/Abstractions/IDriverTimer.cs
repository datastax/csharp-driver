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

namespace Cassandra.Metrics.Abstractions
{
    /// <summary>
    /// A <see cref="IDriverTimer"/> is a combination of a <see cref="IDriverHistogram"/> and a <see cref="IDriverMeter"/>
    /// allowing us to measure the duration of a type of event, the rate of its occurrence and provide duration statistics,
    /// for example tracking the time it takes to execute a particular CQL request.
    /// </summary>
    public interface IDriverTimer : IDriverMetric
    {
        /// <summary>
        /// Starts the timer for a single measurement.
        /// </summary>
        /// <returns>An instance of <see cref="IDriverTimerMeasurement"/> that can be used to stop the timer for this measurement
        /// and add the value to the metric value.</returns>
        IDriverTimerMeasurement StartMeasuring();
    }
}