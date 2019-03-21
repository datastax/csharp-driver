// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

namespace Dse
{
    /// <summary>
    /// Options related to Monitor Reporting.
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
        /// Determines whether or not events are sent to the connected DSE cluster for monitor reporting.
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