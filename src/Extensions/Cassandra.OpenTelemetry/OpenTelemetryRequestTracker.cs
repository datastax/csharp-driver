//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using OpenTelemetry.Trace;

namespace Cassandra.OpenTelemetry
{
    /// <summary>
    /// OpenTelemetry request tracker implementation that includes tracing capabilities and follow
    /// the Trace Semantic Conventions v1.27.0.
    /// https://opentelemetry.io/docs/specs/semconv/database/database-spans/
    /// https://opentelemetry.io/docs/specs/semconv/database/cassandra/
    /// </summary>
    public class OpenTelemetryRequestTracker : IRequestTracker
    {
        internal static readonly ActivitySource ActivitySource = new ActivitySource(CassandraActivitySourceHelper.ActivitySourceName, CassandraActivitySourceHelper.Version);
        private readonly CassandraInstrumentationOptions _instrumentationOptions;
        private const string OtelActivityKey = "otel_activity";
        private const string OtelStmtKey = "otel_statement_string";
        private const string SessionOperationName = "Session_Request";
        private const string NodeOperationName = "Node_Request";

        /// <summary>
        /// Request Tracker implementation that implements OpenTelemetry instrumentation.
        /// </summary>
        public OpenTelemetryRequestTracker(CassandraInstrumentationOptions instrumentationOptions)
        {
            _instrumentationOptions = instrumentationOptions;
        }

        /// <summary>
        /// Starts an <see cref="Activity"/> when request starts and includes the following Cassandra specific tags:
        /// <list type="bullet">
        /// <item>
        /// <description>db.system that has a harcoded value of `cassandra`.</description>
        /// </item>
        /// <item>
        /// <description>db.operation.name that has a harcoded value of `Session Request`.</description>
        /// </item>
        /// <item>
        /// <description>db.namespace that has the Keyspace value, if set.</description>
        /// </item>
        /// <item>
        /// <description>db.query.text that has the database query if included in <see cref="CassandraInstrumentationOptions"/>.</description>
        /// </item>
        /// </list>
        /// </summary>
        /// <param name="request">Request contextual information.</param>
        /// <returns>Activity task.</returns>
        public virtual Task OnStartAsync(RequestTrackingInfo request)
        {
            var operationName = GetSessionOperationName(request);

            var keyspace = GetKeyspace(request);
            var activity = ActivitySource.StartActivity(GetActivityName(operationName, keyspace), ActivityKind.Client);

            if (activity == null)
            {
                return Task.CompletedTask;
            }

            activity.AddTag("db.system", "cassandra");
            activity.AddTag("db.operation.name", operationName);

            if (activity.IsAllDataRequested)
            {
                if (!string.IsNullOrEmpty(keyspace))
                {
                    activity.AddTag("db.namespace", keyspace);
                }

                var queryText = GetQueryText(request);
                if (_instrumentationOptions.IncludeDatabaseStatement && queryText != null)
                {
                    activity.AddTag("db.query.text", queryText);
                    request.Items.TryAdd(OtelStmtKey, queryText);
                }
            }

            request.Items.TryAdd(OtelActivityKey, activity);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Closes the <see cref="Activity"/> when the session request is successful.
        /// </summary>
        /// <param name="request">Request contextual information.</param>
        /// <returns>Completed task.</returns>
        public virtual Task OnSuccessAsync(RequestTrackingInfo request)
        {
            request.Items.TryRemove(OtelActivityKey, out var context);

            if (!(context is Activity activity))
            {
                return Task.CompletedTask;
            }
            activity.SetStatus(ActivityStatusCode.Ok);
            activity.Dispose();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Closes the <see cref="Activity"/> when the session request is unsuccessful.
        /// Includes an <see cref="ActivityEvent"/> containing information from the specified exception.
        /// </summary>
        /// <param name="request">Request contextual information.</param>
        /// <param name="ex">Exception information.</param>
        /// <returns>Completed task.</returns>
        public virtual Task OnErrorAsync(RequestTrackingInfo request, Exception ex)
        {
            request.Items.TryRemove(OtelActivityKey, out var context);

            if (!(context is Activity activity))
            {
                return Task.CompletedTask;
            }

            activity.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity.RecordException(ex);

            activity.Dispose();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Closes the <see cref="Activity"/> when the node request is successful.
        /// </summary>
        /// <param name="request">Request contextual information.</param>
        /// <param name="hostInfo">Struct with host contextual information.</param>
        /// <returns>Completed task.</returns>
        public virtual Task OnNodeSuccessAsync(RequestTrackingInfo request, HostTrackingInfo hostInfo)
        {
            var activityKey = $"{OtelActivityKey}.{hostInfo.ExecutionId}";

            request.Items.TryRemove(activityKey, out var context);

            if (!(context is Activity activity))
            {
                return Task.CompletedTask;
            }
            activity.SetStatus(ActivityStatusCode.Ok);
            activity.Dispose();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Closes the <see cref="Activity"/> when the node request level request is unsuccessful.
        /// Includes an <see cref="ActivityEvent"/> containing information from the specified exception.
        /// </summary>
        /// <param name="request"><see cref="RequestTrackingInfo"/> object with contextual information.</param>
        /// <param name="hostInfo">Struct with host contextual information.</param>
        /// <param name="ex">Exception information.</param>
        /// <returns>Completed task.</returns>
        public virtual Task OnNodeErrorAsync(RequestTrackingInfo request, HostTrackingInfo hostInfo, Exception ex)
        {
            var activityKey = $"{OtelActivityKey}.{hostInfo.ExecutionId}";
            
            request.Items.TryRemove(activityKey, out var context);

            if (!(context is Activity activity))
            {
                return Task.CompletedTask;
            }

            activity.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity.RecordException(ex);

            activity.Dispose();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Closes the <see cref="Activity"/> when the node request is aborted (e.g. pending speculative execution).
        /// </summary>
        /// <param name="request">Request contextual information.</param>
        /// <param name="hostInfo">Struct with host contextual information.</param>
        /// <returns>Completed task.</returns>
        public Task OnNodeAborted(RequestTrackingInfo request, HostTrackingInfo hostInfo)
        {
            var activityKey = $"{OtelActivityKey}.{hostInfo.ExecutionId}";

            request.Items.TryRemove(activityKey, out var context);

            if (!(context is Activity activity))
            {
                return Task.CompletedTask;
            }

            activity.SetStatus(ActivityStatusCode.Unset);
            activity.Dispose();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Starts an <see cref="Activity"/> when node request starts and includes the following Cassandra specific tags:
        /// <list type="bullet">
        /// <item>
        /// <description>db.system that has a harcoded value of `cassandra`.</description>
        /// </item>
        /// <item>
        /// <description>db.operation.name that has a harcoded value of `Node Request`.</description>
        /// </item>
        /// <item>
        /// <description>db.namespace that has the Keyspace value, if set.</description>
        /// </item>
        /// <item>
        /// <description>db.query.text that has the database query if included in <see cref="CassandraInstrumentationOptions"/>.</description>
        /// </item>
        /// <item>
        /// <description>server.address that has the host address value.</description>
        /// </item>
        /// <item>
        /// <description>server.port that has the host port value.</description>
        /// </item>
        /// </list>
        /// </summary>
        /// <returns>Activity task.</returns>
        public virtual Task OnNodeStartAsync(RequestTrackingInfo request, HostTrackingInfo hostInfo)
        {
            request.Items.TryGetValue(OtelActivityKey, out var sessionContext);

            if (!(sessionContext is Activity parentActivity))
            {
                return Task.CompletedTask;
            }

            var operationName = GetNodeOperationName(request);
            var keyspace = GetKeyspace(request);
            var activity = ActivitySource.StartActivity(GetActivityName(operationName, keyspace), ActivityKind.Client, parentActivity.Context);

            if (activity == null)
            {
                return Task.CompletedTask;
            }

            activity.AddTag("db.system", "cassandra");
            activity.AddTag("db.operation.name", operationName);
            activity.AddTag("server.address", hostInfo.Host?.Address?.Address.ToString());
            activity.AddTag("server.port", hostInfo.Host?.Address?.Port.ToString());

            if (activity.IsAllDataRequested)
            {
                if (!string.IsNullOrEmpty(keyspace))
                {
                    activity.AddTag("db.namespace", keyspace);
                }

                if (_instrumentationOptions.IncludeDatabaseStatement)
                {
                    if (request.Items.TryGetValue(OtelStmtKey, out var stmt))
                    {
                        activity.AddTag("db.query.text", stmt);
                    }
                }
            }

            request.Items.TryAdd($"{OtelActivityKey}.{hostInfo.ExecutionId}", activity);

            return Task.CompletedTask;
        }

        private string GetSessionOperationName(RequestTrackingInfo request)
        {
            return $"{SessionOperationName}({request.Statement?.GetType().Name ?? request.PrepareRequest?.GetType().Name})";
        }

        private string GetNodeOperationName(RequestTrackingInfo request)
        {
            return $"{NodeOperationName}({request.Statement?.GetType().Name ?? request.PrepareRequest?.GetType().Name})";
        }

        private string GetKeyspace(RequestTrackingInfo request)
        {
            var ks = request.Statement == null ? request.PrepareRequest?.Keyspace : request.Statement?.Keyspace;
            return ks ?? request.SessionKeyspace;
        }

        private string GetQueryText(RequestTrackingInfo request)
        {
            return request.Statement == null ? request.PrepareRequest?.Query : GetQueryTextFromStatement(request.Statement);
        }

        private string GetActivityName(string operationName, string ks)
        {
            return string.IsNullOrEmpty(ks) ? $"{operationName}" : $"{operationName} {ks}";
        }

        private string GetQueryTextFromStatement(IStatement statement)
        {
            string str;
            switch (statement)
            {
                case BatchStatement s:
                    var i = 0;
                    var sb = new StringBuilder();
                    var first = true;
                    foreach (var stmt in s.Statements)
                    {
                        if (i >= _instrumentationOptions.BatchChildStatementLimit)
                        {
                            break;
                        }
                        if (!first)
                        {
                            sb.Append($"; {stmt}");
                        }
                        else
                        {
                            sb.Append($"{stmt}");
                            first = false;
                        }
                    }

                    str = sb.ToString();
                    break;
                default:
                    str = statement.ToString();
                    break;
            }

            return str;
        }
    }
}
