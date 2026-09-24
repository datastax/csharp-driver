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
using System.Threading.Tasks;
using Cassandra.OpenTelemetry;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class OpenTelemetryTests
    {
        private const string OtelActivityKey = "otel_activity";
        private const string DbNamespaceTag = "db.namespace";
        private const string DbQueryTextTag = "db.query.text";

        [Test]
        public async Task OpenTelemetryRequestTrackerOnStartAsync_StatementIsNull_DbQueryTextAndDbNamespaceTagsAreNotIncluded()
        {
            using (var listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == CassandraActivitySourceHelper.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded
            })
            {
                ActivitySource.AddActivityListener(listener);
                
                var cassandraInstrumentationOptions = new CassandraInstrumentationOptions { IncludeDatabaseStatement = true };
                var requestTracker = new OpenTelemetryRequestTracker(cassandraInstrumentationOptions);
                IStatement statement = null;
                var requestTrackingInfo = new SessionRequestInfo(statement, null);

                await requestTracker.OnStartAsync(requestTrackingInfo).ConfigureAwait(false);

                requestTrackingInfo.Items.TryGetValue(OtelActivityKey, out object context);

                var activity = context as Activity;

                Assert.NotNull(activity);
                Assert.Null(activity.Tags.FirstOrDefault(x => x.Key == DbNamespaceTag).Value);
                Assert.Null(activity.Tags.FirstOrDefault(x => x.Key == DbQueryTextTag).Value);
            }
        }

        [Test]
        public async Task OpenTelemetryRequestTrackerOnStartAsync_ListenerNotSampling_ActivityIsNull()
        {
            using (var listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == CassandraActivitySourceHelper.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.None
            })
            {
                ActivitySource.AddActivityListener(listener);

                var cassandraInstrumentationOptions = new CassandraInstrumentationOptions { IncludeDatabaseStatement = true };
                var requestTracker = new OpenTelemetryRequestTracker(cassandraInstrumentationOptions);
                IStatement statement = null;
                var requestTrackingInfo = new SessionRequestInfo(statement, null);

                await requestTracker.OnStartAsync(requestTrackingInfo).ConfigureAwait(false);

                requestTrackingInfo.Items.TryGetValue(OtelActivityKey, out object context);

                var activity = context as Activity;

                Assert.Null(activity);
            }
        }

        [Test]
        public async Task OpenTelemetryRequestTrackerOnNodeStartAsync_StatementIsNull_DbQueryTextAndDbNamespaceTagsAreNotIncluded()
        {
            using (var listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == CassandraActivitySourceHelper.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded
            })
            {
                ActivitySource.AddActivityListener(listener);

                var cassandraInstrumentationOptions = new CassandraInstrumentationOptions { IncludeDatabaseStatement = true };
                var requestTracker = new OpenTelemetryRequestTracker(cassandraInstrumentationOptions);
                
                IStatement statement = null;
                var requestTrackingInfo = new SessionRequestInfo(statement, null);

                var host = new Host(new System.Net.IPEndPoint(1, 9042), new ConstantReconnectionPolicy(1));
                var hostTrackingInfo = new NodeRequestInfo(host, null);

                await requestTracker.OnStartAsync(requestTrackingInfo).ConfigureAwait(false);
                await requestTracker.OnNodeStartAsync(requestTrackingInfo, hostTrackingInfo).ConfigureAwait(false);

                requestTrackingInfo.Items.TryGetValue($"{OtelActivityKey}.{hostTrackingInfo.ExecutionId}", out object context);

                var activity = context as Activity;

                Assert.NotNull(activity);
                Assert.Null(activity.Tags.FirstOrDefault(x => x.Key == DbNamespaceTag).Value);
                Assert.Null(activity.Tags.FirstOrDefault(x => x.Key == DbQueryTextTag).Value);
            }
        }

        [Test]
        public async Task OpenTelemetryRequestTrackerOnErrorAsync_ExceptionIsRecordedOnActivity()
        {
            Activity completedActivity = null;

            using (var listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == CassandraActivitySourceHelper.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => completedActivity = activity
            })
            {
                ActivitySource.AddActivityListener(listener);

                var requestTracker = new OpenTelemetryRequestTracker(new CassandraInstrumentationOptions());
                IStatement statement = null;
                var requestTrackingInfo = new SessionRequestInfo(statement, null);

                await requestTracker.OnStartAsync(requestTrackingInfo).ConfigureAwait(false);

                var ex = new InvalidOperationException("test error");
                await requestTracker.OnErrorAsync(requestTrackingInfo, ex).ConfigureAwait(false);

                Assert.NotNull(completedActivity);
                Assert.AreEqual(ActivityStatusCode.Error, completedActivity.Status);
                var exceptionEvent = completedActivity.Events.FirstOrDefault(e => e.Name == "exception");
                Assert.NotNull(exceptionEvent.Name, "Expected an 'exception' event to be recorded on the activity");
                Assert.AreEqual(typeof(InvalidOperationException).FullName, exceptionEvent.Tags.FirstOrDefault(t => t.Key == "exception.type").Value);
                Assert.AreEqual("test error", exceptionEvent.Tags.FirstOrDefault(t => t.Key == "exception.message").Value);
            }
        }

        [Test]
        public async Task OpenTelemetryRequestTrackerOnNodeErrorAsync_ExceptionIsRecordedOnActivity()
        {
            Activity completedActivity = null;

            using (var listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == CassandraActivitySourceHelper.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => completedActivity = activity
            })
            {
                ActivitySource.AddActivityListener(listener);

                var requestTracker = new OpenTelemetryRequestTracker(new CassandraInstrumentationOptions());
                IStatement statement = null;
                var requestTrackingInfo = new SessionRequestInfo(statement, null);
                var host = new Host(new System.Net.IPEndPoint(1, 9042), new ConstantReconnectionPolicy(1));
                var hostTrackingInfo = new NodeRequestInfo(host, null);

                await requestTracker.OnStartAsync(requestTrackingInfo).ConfigureAwait(false);
                await requestTracker.OnNodeStartAsync(requestTrackingInfo, hostTrackingInfo).ConfigureAwait(false);

                var ex = new InvalidOperationException("test node error");
                await requestTracker.OnNodeErrorAsync(requestTrackingInfo, hostTrackingInfo, ex).ConfigureAwait(false);

                Assert.NotNull(completedActivity);
                Assert.AreEqual(ActivityStatusCode.Error, completedActivity.Status);
                var exceptionEvent = completedActivity.Events.FirstOrDefault(e => e.Name == "exception");
                Assert.NotNull(exceptionEvent.Name, "Expected an 'exception' event to be recorded on the activity");
                Assert.AreEqual(typeof(InvalidOperationException).FullName, exceptionEvent.Tags.FirstOrDefault(t => t.Key == "exception.type").Value);
                Assert.AreEqual("test node error", exceptionEvent.Tags.FirstOrDefault(t => t.Key == "exception.message").Value);
            }
        }

        [Test]
        public async Task OpenTelemetryRequestTrackerOnNodeStartAsync_ListenerNotSampling_ActivityIsNull()
        {
            using (var listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == CassandraActivitySourceHelper.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.None
            })
            {
                ActivitySource.AddActivityListener(listener);

                var cassandraInstrumentationOptions = new CassandraInstrumentationOptions { IncludeDatabaseStatement = true };
                var requestTracker = new OpenTelemetryRequestTracker(cassandraInstrumentationOptions);
                
                IStatement statement = null;
                var requestTrackingInfo = new SessionRequestInfo(statement, null);
                
                var host = new Host(new System.Net.IPEndPoint(1, 9042), new ConstantReconnectionPolicy(1));
                var hostTrackingInfo = new NodeRequestInfo(host, null);

                await requestTracker.OnStartAsync(requestTrackingInfo).ConfigureAwait(false);
                await requestTracker.OnNodeStartAsync(requestTrackingInfo, hostTrackingInfo).ConfigureAwait(false);

                requestTrackingInfo.Items.TryGetValue($"{OtelActivityKey}.{host.HostId}", out object context);

                var activity = context as Activity;

                Assert.Null(activity);
            }
        }
    }
}