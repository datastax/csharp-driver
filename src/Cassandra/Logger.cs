//
//      Copyright (C) 2012-2014 DataStax Inc.
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

namespace Cassandra
{
    public class Logger
    {
        private const string DateFormat = "MM/dd/yyyy H:mm:ss.fff zzz";
        private readonly string _category;
        private StringBuilder _sb;

        public Logger(Type type)
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

        private string GetExceptionAndAllInnerEx(Exception ex, bool recur = false)
        {
            if (!recur || _sb == null)
                _sb = new StringBuilder();
            _sb.Append(String.Format("( Exception! Source {0} \n Message: {1} \n StackTrace:\n {2} ", ex.Source, ex.Message,
                                    (Diagnostics.CassandraStackTraceIncluded
                                         ? (recur ? ex.StackTrace : PrintStackTrace(ex))
                                         : "To display StackTrace, change Debugging.StackTraceIncluded property value to true.")));
            if (ex.InnerException != null)
                GetExceptionAndAllInnerEx(ex.InnerException, true);

            _sb.Append(")");
            return _sb.ToString();
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
                String.Format("{0} #ERROR: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), GetExceptionAndAllInnerEx(ex)), _category);
        }

        public void Error(string msg, Exception ex = null)
        {
            if (!Diagnostics.CassandraTraceSwitch.TraceError)
            {
                return;
            }
            Trace.WriteLine(
                String.Format("{0} #ERROR: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat),
                    msg + (ex != null ? "\nEXCEPTION:\n " + GetExceptionAndAllInnerEx(ex) : String.Empty)), _category);
        }

        public void Error(string message, params object[] args)
        {
            if (!Diagnostics.CassandraTraceSwitch.TraceError)
            {
                return;
            }
            if (args != null && args.Length > 0)
            {
                message = String.Format(message, args);
            }
            Trace.WriteLine(String.Format("{0} #ERROR: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), message), _category);
        }

        public void Warning(string message, params object[] args)
        {
            if (!Diagnostics.CassandraTraceSwitch.TraceWarning)
            {
                return;
            }
            if (args != null && args.Length > 0)
            {
                message = String.Format(message, args);
            }
            Trace.WriteLine(String.Format("{0} #WARNING: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), message), _category);
        }

        public void Info(string message, params object[] args)
        {
            if (!Diagnostics.CassandraTraceSwitch.TraceInfo)
            {
                return;
            }
            if (args != null && args.Length > 0)
            {
                message = String.Format(message, args);
            }
            Trace.WriteLine(String.Format("{0} : {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), message), _category);
        }

        public void Verbose(string message, params object[] args)
        {
            if (!Diagnostics.CassandraTraceSwitch.TraceVerbose)
            {
                return;
            }
            if (args != null && args.Length > 0)
            {
                message = String.Format(message, args);
            }
            Trace.WriteLine(String.Format("{0} {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), message), _category);
        }
    }
}