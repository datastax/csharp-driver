using System;
using System.Diagnostics;
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
            _category = type.FullName;
        }

        private string printStackTrace()
        {
            var sb = new StringBuilder();
            foreach (StackFrame frame in new StackTrace(3, true).GetFrames()) // skipping 3 frames from logger class. 
                sb.Append(frame);
            return sb.ToString();
        }

        private string GetExceptionAndAllInnerEx(Exception ex, bool recur = false)
        {
            if (!recur || _sb == null)
                _sb = new StringBuilder();
            _sb.Append(string.Format("( Exception! Source {0} \n Message: {1} \n StackTrace:\n {2} ", ex.Source, ex.Message,
                                    (Diagnostics.CassandraStackTraceIncluded
                                         ? (recur ? ex.StackTrace : printStackTrace())
                                         : "To display StackTrace, change Debugging.StackTraceIncluded property value to true."), _category));
            if (ex.InnerException != null)
                GetExceptionAndAllInnerEx(ex.InnerException, true);

            _sb.Append(")");
            return _sb.ToString();
        }

        public void Error(Exception ex)
        {
            if (ex != null) //shouldn't happen
            {
                if (Diagnostics.CassandraTraceSwitch.TraceError)
                    Trace.WriteLine(
                        string.Format("{0} #ERROR: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), GetExceptionAndAllInnerEx(ex)), _category);
            }
            else
                throw new InvalidOperationException();
        }

        public void Error(string msg, Exception ex = null)
        {
            if (Diagnostics.CassandraTraceSwitch.TraceError)
                Trace.WriteLine(
                    string.Format("{0} #ERROR: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat),
                                  msg + (ex != null ? "\nEXCEPTION:\n " + GetExceptionAndAllInnerEx(ex) : String.Empty)), _category);
        }

        public void Warning(string msg)
        {
            if (Diagnostics.CassandraTraceSwitch.TraceWarning)
                Trace.WriteLine(string.Format("{0} #WARNING: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), msg), _category);
        }

        public void Info(string msg)
        {
            if (Diagnostics.CassandraTraceSwitch.TraceInfo)
                Trace.WriteLine(string.Format("{0} #INFO: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), msg), _category);
        }

        public void Verbose(string msg)
        {
            if (Diagnostics.CassandraTraceSwitch.TraceVerbose)
                Trace.WriteLine(string.Format("{0} #VERBOSE: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), msg), _category);
        }
    }
}