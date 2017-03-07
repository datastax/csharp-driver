//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Dse
{
    /// <summary>
    /// Provides a set of methods and properties related to logging in the driver.
    /// </summary>
    public static class Diagnostics
    {
        /// <summary>
        /// Determines if a <see cref="ILoggerFactory"/> API should be used to obtain a instance of logger.
        /// </summary>
        internal static volatile bool UseLoggerFactory;

        internal static readonly ILoggerFactory LoggerFactory = new LoggerFactory();

        /// <summary>
        /// Specifies what messages should be passed to the output log when using the <see cref="Trace"/> API.
        /// <para></para>
        /// <para><value>TraceLevel.Off</value> - Output no tracing messages.</para>   
        /// <para><value>TraceLevel.Error</value>  - Output error-handling messages.</para> 
        /// <para><value>TraceLevel.Warning</value> - Output warnings and error-handling messages.</para>
        /// <para><value>TraceLevel.Info</value> - Output informational messages, warnings, and error-handling messages.</para>
        /// <para><value>TraceLevel.Verbose</value> - Output all debugging and tracing messages.</para>                
        /// </summary>
        /// <remarks>
        /// Consider using <c>Microsoft.Extensions.Logging</c> API instead by adding a <see cref="ILoggerProvider"/>
        /// using the <see cref="AddLoggerProvider(ILoggerProvider)"/> method.
        /// </remarks>
        /// <seealso cref="AddLoggerProvider"/>
        public static readonly TraceSwitch CassandraTraceSwitch = new TraceSwitch(
            "TraceSwitch", "This switch lets the user choose which kind of messages should be included in log.");

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

        /// <summary>
        /// Adds a <see cref="ILoggerProvider" /> to the logger factory used by the driver.
        /// <para>
        /// Be sure to call this method before initializing the <see cref="ICluster"/> to ensure that
        /// <see cref="ILoggerFactory"/> API is used as driver logging mechanism instead of
        /// <see cref="Trace"/>.
        /// </para>
        /// </summary>
        /// <param name="provider">The logger provider to add to the logger factory</param>
        public static void AddLoggerProvider(ILoggerProvider provider)
        {
            UseLoggerFactory = true;
            LoggerFactory.AddProvider(provider);
        }
    }
}
