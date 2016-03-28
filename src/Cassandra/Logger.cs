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

namespace Cassandra
{
    public class Logger
    {
        private readonly TraceSource _trace;

        public Logger(Type type)
        {
            var typeName = type.FullName;
            if (type.IsGenericTypeDefinition || type.IsGenericType)
            {
                // Trim generic types (e.g.: Cassandra.Requests.RequestExecution`1)
                typeName = type.FullName.Substring(0, type.FullName.IndexOf('`'));
            }

            // Look for source matching the type name (e.g.: Cassandra.Requests.RequestExecution)
            _trace = new TraceSource(typeName);

            // If no such source is configured it will have just the DefaultTraceListener
            var sourceName = typeName;
            var wildcardSource = _trace;
            while (wildcardSource.Listeners.Count == 1 && wildcardSource.Listeners[0] is DefaultTraceListener)
            {
                // Trim source (e.g.: Cassandra.Requests)
                var dotIndex = sourceName.LastIndexOf('.');
                if (dotIndex == -1)
                {
                    // Exhausted options
                    wildcardSource = null;
                    break;
                }

                // Look for a wildcard source (e.g.: Cassandra.Requests.*)
                sourceName = sourceName.Substring(0, dotIndex);
                wildcardSource = new TraceSource(sourceName + ".*");
            }

            if (wildcardSource != null)
            {
                _trace = new TraceSource(typeName, wildcardSource.Switch.Level);
                _trace.Listeners.Clear();
                _trace.Listeners.AddRange(wildcardSource.Listeners);
            }
        }

        public void Error(Exception ex)
        {
            if (ex == null)
            {
                return;
            }
            _trace.TraceEvent(TraceEventType.Error, 0, "{0}", ex);
        }

        public void Error(string msg, Exception ex = null)
        {
            if (ex == null)
            {
                _trace.TraceEvent(TraceEventType.Error, 0, msg);
            }
            else
            {
                _trace.TraceEvent(TraceEventType.Error, 0, "{0}: {1}", msg, ex);
            }
        }

        public void Error(string message, params object[] args)
        {
            _trace.TraceEvent(TraceEventType.Error, 0, message, args);
        }

        public void Warning(string message, params object[] args)
        {
            _trace.TraceEvent(TraceEventType.Warning, 0, message, args);
        }

        public void Info(string message, params object[] args)
        {
            _trace.TraceEvent(TraceEventType.Information, 0, message, args);
        }

        public void Verbose(string message, params object[] args)
        {
            _trace.TraceEvent(TraceEventType.Verbose, 0, message, args);
        }
    }
}