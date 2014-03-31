using System;
using System.Diagnostics;
using System.Text;

namespace Cassandra
{
    public class Logger
    {                        
        private string category;
        private StringBuilder sb = null;
         
        public Logger(Type type)
        {            
            category = type.FullName;
        }

        private string printStackTrace()
        {
            StringBuilder sb = new StringBuilder();            
            foreach (var frame in new StackTrace(3, true).GetFrames()) // skipping 3 frames from logger class. 
                sb.Append(frame);
            return sb.ToString(); 
        }

        private string getExceptionAndAllInnerEx(Exception ex, bool recur = false)
        {
            if(!recur || sb == null)
                sb = new StringBuilder();
            sb.Append(string.Format("( Exception! Source {0} \n Message: {1} \n StackTrace:\n {2} ", ex.Source, ex.Message,
                                    (Diagnostics.CassandraStackTraceIncluded ? (recur ? ex.StackTrace : printStackTrace()) : "To display StackTrace, change Debugging.StackTraceIncluded property value to true."), this.category));
            if (ex.InnerException != null)
                getExceptionAndAllInnerEx(ex.InnerException, true);            
            
            sb.Append(")");
            return sb.ToString();
        }

        private readonly string DateFormat = "MM/dd/yyyy H:mm:ss.fff zzz";

        public void Error(Exception ex)
        {
            if (ex != null) //shouldn't happen
            {
                if (Diagnostics.CassandraTraceSwitch.TraceError)
                    Trace.WriteLine(string.Format("{0} #ERROR: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), getExceptionAndAllInnerEx(ex)), category);
            }
            else
                throw new InvalidOperationException();
        }

        public void Error(string msg, Exception ex = null)
        {
            if (Diagnostics.CassandraTraceSwitch.TraceError)
                Trace.WriteLine(string.Format("{0} #ERROR: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), msg + (ex != null ? "\nEXCEPTION:\n " + getExceptionAndAllInnerEx(ex) : String.Empty)), category);
        }
        
        public void Warning(string msg)
        {
            if(Diagnostics.CassandraTraceSwitch.TraceWarning)
                Trace.WriteLine(string.Format("{0} #WARNING: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), msg), category);
        }

        public void Info(string msg)
        {            
            if (Diagnostics.CassandraTraceSwitch.TraceInfo)
                Trace.WriteLine(string.Format("{0} #INFO: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), msg), category);
        }
        
        public void Verbose(string msg)
        {            
            if(Diagnostics.CassandraTraceSwitch.TraceVerbose)
                Trace.WriteLine(string.Format("{0} #VERBOSE: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), msg), category);
        }
    }
}