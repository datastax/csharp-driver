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

namespace Cassandra.Metrics.Abstractions
{
    /// <summary>
    /// Represents an individual measurement obtained with an instance of <see cref="IDriverTimer"/>.
    /// </summary>
    public interface IDriverTimerMeasurement
    {
        /// <summary>
        /// Stops measuring and adds the measurement to the related metric.
        /// </summary>
        void StopMeasuring();
    }
}