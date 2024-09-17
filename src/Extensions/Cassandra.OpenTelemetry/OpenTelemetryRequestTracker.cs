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
using System.Threading.Tasks;
using OpenTelemetry.Trace;

namespace Cassandra.OpenTelemetry
{
    /// <summary>
    /// OpenTelemetry request tracker implementation that includes tracing capabilities and follow
    /// the Trace Semantic Conventions v1.24.0.
    /// https://opentelemetry.io/docs/specs/semconv/database/database-spans/
    /// https://opentelemetry.io/docs/specs/semconv/database/cassandra/
    /// </summary>
    public class OpenTelemetryRequestTracker : IRequestTracker
    {
        internal static readonly ActivitySource ActivitySource = new ActivitySource(CassandraActivitySourceHelper.ActivitySourceName, CassandraActivitySourceHelper.Version);
        private readonly CassandraInstrumentationOptions _instrumentationOptions;
        private static readonly string otelActivityKey = "otel_activity";
        private static readonly string sessionOperationName = "Session Request";
        private static readonly string nodeOperationName = "Node Request";

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
        /// <description>db.name that has the Keyspace value, if set.</description>
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
            var activityName = !string.IsNullOrEmpty(request.Statement?.Keyspace) ? $"{sessionOperationName} {request.Statement.Keyspace}" : sessionOperationName;

            var activity = ActivitySource.StartActivity(activityName, ActivityKind.Client);

            activity?.AddTag("db.system", "cassandra");
            activity?.AddTag("db.operation.name", sessionOperationName);

            if (activity != null && activity.IsAllDataRequested)
            {
                if (!string.IsNullOrEmpty(request.Statement?.Keyspace))
                {
                    activity.AddTag("db.namespace", request.Statement.Keyspace);
                }

                if (_instrumentationOptions.IncludeDatabaseStatement && request.Statement != null)
                {
                    activity.AddTag("db.query.text", request.Statement.ToString());
                }
            }

            request.Items.TryAdd(otelActivityKey, activity);

            return Task.FromResult(activity as object);
        }

        /// <summary>
        /// Closes the <see cref="Activity"/> when the session request is successful.
        /// </summary>
        /// <param name="request">Request contextual information.</param>
        /// <returns>Completed task.</returns>
        public virtual Task OnSuccessAsync(RequestTrackingInfo request)
        {
            request.Items.TryGetValue(otelActivityKey, out object context);

            if (context is Activity activity)
            {
                activity?.Dispose();
            }

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
            request.Items.TryGetValue(otelActivityKey, out object context);

            if (!(context is Activity activity))
            {
                return Task.CompletedTask;
            }

            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.RecordException(ex);

            activity?.Dispose();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Closes the <see cref="Activity"/> when the node request is successful.
        /// </summary>
        /// <param name="request">Request contextual information.</param>
        /// <param name="hostInfo">Struct with host contextual information.</param>
        /// <returns>Completed task./returns>
        public virtual Task OnNodeSuccessAsync(RequestTrackingInfo request, HostTrackingInfo hostInfo)
        {
            var activityKey = $"{otelActivityKey}.{hostInfo.Host.HostId}";

            request.Items.TryGetValue(activityKey, out object context);

            if (context is Activity activity)
            {
                activity?.Dispose();
            }

            request.Items.TryRemove(activityKey, out _);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Closes the <see cref="Activity"/> when the node request level request is unsuccessful.
        /// Includes an <see cref="ActivityEvent"/> containing information from the specified exception.
        /// </summary>
        /// <param name="request"><see cref="RequestTrackingInfo"/> object with contextual information.</param>
        /// <param name="hostInfo">Struct with host contextual information.</param>
        /// <param name="ex">Exception information.</param>
        /// <returns>Completed task./returns>
        public virtual Task OnNodeErrorAsync(RequestTrackingInfo request, HostTrackingInfo hostInfo, Exception ex)
        {
            var activityKey = $"{otelActivityKey}.{hostInfo.Host.HostId}";
            
            request.Items.TryGetValue(activityKey, out object context);

            if (!(context is Activity activity))
            {
                return Task.CompletedTask;
            }

            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.RecordException(ex);

            activity?.Dispose();

            request.Items.TryRemove(activityKey, out _);

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
        /// <description>db.name that has the Keyspace value, if set.</description>
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
        /// <param name="request">Request contextual information.</param>
        /// <returns>Activity task.</returns>
        public virtual Task OnNodeStart(RequestTrackingInfo request, HostTrackingInfo hostInfo)
        {
            request.Items.TryGetValue(otelActivityKey, out object sessionContext);

            if (!(sessionContext is Activity parentActivity) || parentActivity.Context == null)
            {
                return Task.CompletedTask;
            }

            var activityName = !string.IsNullOrEmpty(request.Statement.Keyspace) ? $"{nodeOperationName} {request.Statement.Keyspace}" : nodeOperationName;

            var activity = ActivitySource.StartActivity(activityName, ActivityKind.Client, parentActivity.Context);

            activity?.AddTag("db.system", "cassandra");
            activity?.AddTag("db.operation.name", nodeOperationName);
            activity?.AddTag("server.address", hostInfo.Host?.Address?.Address.ToString());
            activity?.AddTag("server.port", hostInfo.Host?.Address?.Port.ToString());

            if (activity != null && activity.IsAllDataRequested)
            {
                if (!string.IsNullOrEmpty(request.Statement.Keyspace))
                {
                    activity.AddTag("db.namespace", request.Statement.Keyspace);
                }

                if (_instrumentationOptions.IncludeDatabaseStatement && request.Statement != null)
                {
                    activity.AddTag("db.query.text", request.Statement.ToString());
                }
            }

            request.Items.TryAdd($"{otelActivityKey}.{hostInfo.Host.HostId}", activity);

            return Task.FromResult(activity as object);
        }
    }
}
