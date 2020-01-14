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

namespace Cassandra
{
    /// <summary>
    /// Options related to Monitor Reporting.
    /// This feature is not supported with Apache Cassandra clusters for now, so in that case this feature will
    /// always be disabled even if it is set as enabled with <see cref="SetMonitorReportingEnabled"/>.
    /// </summary>
    public sealed class MonitorReportingOptions
    {
        internal const long DefaultStatusEventDelayMilliseconds = 300000L;

        internal const bool DefaultMonitorReportingEnabled = true;
        
        internal long StatusEventDelayMilliseconds { get; private set; } = MonitorReportingOptions.DefaultStatusEventDelayMilliseconds;

        /// <summary>
        /// This property is used to determine whether Monitor Reporting is enabled or not.
        /// </summary>
        public bool MonitorReportingEnabled { get; private set; } = MonitorReportingOptions.DefaultMonitorReportingEnabled;
        
        /// <summary>
        /// Determines whether or not events are sent to the connected cluster for monitor reporting.
        /// </summary>
        /// <remarks>If not set through this method, the default value (<code>true</code>) will be used.</remarks>
        /// <param name="monitorReportingEnabled">Flag that controls whether monitor reporting is enabled or disabled.</param>
        /// <returns>This MonitorReportingOptions instance.</returns>
        public MonitorReportingOptions SetMonitorReportingEnabled(bool monitorReportingEnabled)
        {
            MonitorReportingEnabled = monitorReportingEnabled;
            return this;
        }

        internal MonitorReportingOptions SetStatusEventDelayMilliseconds(long delay)
        {
            StatusEventDelayMilliseconds = delay;
            return this;
        } 
    }
}