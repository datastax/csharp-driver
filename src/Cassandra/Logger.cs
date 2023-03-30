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

using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Cassandra
{
    /// <summary>
    /// Represents a driver Logger
    /// </summary>
    public class Logger
    {
        // Implementation information: the Logger API has been leaked into the public API.
        // To avoid introducing a breaking change, we will keep the Logger as a wrapper and factory of the
        // actual logger
        private readonly ILoggerHandler _loggerHandler;

        public Logger(Type type)
        {
            if (Diagnostics.UseLoggerFactory)
            {
                _loggerHandler = new FactoryBasedLoggerHandler(type);
            }
            else
            {
                _loggerHandler = new TraceBasedLoggerHandler(type);
            }
        }

        internal Logger(ILoggerHandler handler)
        {
            _loggerHandler = handler;
        }

        public void Error(Exception ex)
        {
            _loggerHandler.Error(ex);
        }

        public void Error(string message, Exception ex = null)
        {
            _loggerHandler.Error(message, ex);
        }

        public void Error(string message, params object[] args)
        {
            _loggerHandler.Error(message, args);
        }

        public void Warning(string message, params object[] args)
        {
            _loggerHandler.Warning(message, args);
        }

        public void Info(string message, params object[] args)
        {
            _loggerHandler.Info(message, args);
        }

        public void Verbose(string message, params object[] args)
        {
            _loggerHandler.Verbose(message, args);
        }
        
        /// <summary>
        /// Represents the actual logger
        /// </summary>
        internal interface ILoggerHandler
        {
            void Error(Exception ex);
            void Error(string message, Exception ex = null);
            void Error(string message, params object[] args);
            void Verbose(string message, params object[] args);
            void Info(string message, params object[] args);
            void Warning(string message, params object[] args);
        }

        internal class FactoryBasedLoggerHandler : ILoggerHandler
        {
            private readonly ILogger _logger;

            public FactoryBasedLoggerHandler(Type type)
            {
                _logger = Diagnostics.LoggerFactory.CreateLogger(type.FullName);
            }

            public void Error(Exception ex)
            {
                _logger.LogError(0, ex, "");
            }

            public void Error(string message, Exception ex = null)
            {
                _logger.LogError(0, ex, message);
            }

            public void Error(string message, params object[] args)
            {
                _logger.LogError(message, args);
            }

            public void Verbose(string message, params object[] args)
            {
                _logger.LogDebug(message, args);
            }

            public void Info(string message, params object[] args)
            {
                _logger.LogInformation(message, args);
            }

            public void Warning(string message, params object[] args)
            {
                _logger.LogWarning(message, args);
            }
        }

        internal class TraceBasedLoggerHandler : ILoggerHandler
        {
            private const string DateFormat = "MM/dd/yyyy H:mm:ss.fff zzz";
            private readonly string _category;

            public TraceBasedLoggerHandler(Type type)
            {
                _category = type.Name;
            }

            private static string PrintStackTrace(Exception ex)
            {
                var sb = new StringBuilder();
                // ReSharper disable once AssignNullToNotNullAttribute
                foreach (StackFrame frame in new StackTrace(ex, true).GetFrames().Skip(3))
                {
                    sb.Append(frame);
                }
                return sb.ToString();
            }

            private static string GetExceptionAndAllInnerEx(Exception ex, StringBuilder sb = null)
            {
                var recursive = true;
                if (sb == null)
                {
                    recursive = false;
                    sb = new StringBuilder();
                }
                sb.Append(string.Format("( Exception! Source {0} \n Message: {1} \n StackTrace:\n {2} ", ex.Source, ex.Message,
                                        (Diagnostics.CassandraStackTraceIncluded ?
                                            (recursive ? ex.StackTrace : PrintStackTrace(ex))
                                             : "To display StackTrace, change Debugging.StackTraceIncluded property value to true.")));
                if (ex.InnerException != null)
                {
                    GetExceptionAndAllInnerEx(ex.InnerException, sb);
                }
                sb.Append(")");
                return sb.ToString();
            }

            public void Error(Exception ex)
            {
                if (!Diagnostics.CassandraTraceSwitch.TraceError)
                {
                    return;
                }
                if (ex == null)
                {
                    return;
                }
                Trace.WriteLine(
                    string.Format("{0} #ERROR: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), GetExceptionAndAllInnerEx(ex)), _category);
            }

            public void Error(string msg, Exception ex = null)
            {
                if (!Diagnostics.CassandraTraceSwitch.TraceError)
                {
                    return;
                }
                Trace.WriteLine(
                    string.Format("{0} #ERROR: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat),
                        msg + (ex != null ? "\nEXCEPTION:\n " + GetExceptionAndAllInnerEx(ex) : string.Empty)), _category);
            }

            public void Error(string message, params object[] args)
            {
                if (!Diagnostics.CassandraTraceSwitch.TraceError)
                {
                    return;
                }
                if (args != null && args.Length > 0)
                {
                    message = string.Format(message, args);
                }
                Trace.WriteLine(string.Format("{0} #ERROR: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), message), _category);
            }

            public void Warning(string message, params object[] args)
            {
                if (!Diagnostics.CassandraTraceSwitch.TraceWarning)
                {
                    return;
                }
                if (args != null && args.Length > 0)
                {
                    message = string.Format(message, args);
                }
                Trace.WriteLine(string.Format("{0} #WARNING: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), message), _category);
            }

            public void Info(string message, params object[] args)
            {
                if (!Diagnostics.CassandraTraceSwitch.TraceInfo)
                {
                    return;
                }
                if (args != null && args.Length > 0)
                {
                    message = string.Format(message, args);
                }
                Trace.WriteLine(string.Format("{0} : {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), message), _category);
            }

            public void Verbose(string message, params object[] args)
            {
                if (!Diagnostics.CassandraTraceSwitch.TraceVerbose)
                {
                    return;
                }
                if (args != null && args.Length > 0)
                {
                    message = string.Format(message, args);
                }
                Trace.WriteLine(string.Format("{0} {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), message), _category);
            }
        }
    }

    
}