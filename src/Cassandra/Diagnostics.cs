//
//      Copyright (C) 2012 DataStax Inc.
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
        public static readonly TraceSwitch CassandraTraceSwitch = new TraceSwitch("TraceSwitch",
                                                                                  "This switch lets the user choose which kind of messages should be included in log.");

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