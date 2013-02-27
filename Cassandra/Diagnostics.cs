using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace Cassandra
{
    public static class Diagnostics
    {
        /// <summary>
        /// Specifies what messages should be passed to the output log. 
        /// <para></para>   
        /// <para><value>TraceLevel.Off</value> - Output no tracing messages.</para>   
        /// <para><value>TraceLevel.Error</value>  - Output error-handling messages.</para> 
        /// <para><value>TraceLevel.Warning</value> - Output warnings and error-handling messages.</para>
        /// <para><value>TraceLevel.Info</value> - Output informational messages, warnings, and error-handling messages.</para>
        /// <para><value>TraceLevel.Verbose</value> - Output all debugging and tracing messages.</para>                
        /// </summary>
        public static readonly TraceSwitch CassandraTraceSwitch = new TraceSwitch("TraceSwitch", "This switch lets user to choose which kind of messages should be included in log.");

        /// <summary>
        /// Defines if exception StackTrace information should be printed by trace logger.
        /// <para>Default value is <value>false</value>.</para>
        /// </summary>
        public static bool CassandraStackTraceIncluded { get; set; }

        /// <summary>
        /// Defines if performance counters should be enabled.
        /// <para>Default value is <value>false</value>.</para>
        /// </summary>
        public static bool CassandraPerformanceCountersEnabled { get; set; }
    }
}
