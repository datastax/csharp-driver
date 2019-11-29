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

using System.Collections.Generic;
using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics
{
    /// <summary>
    /// This class is used to customize options related to Metrics. It is used in 
    /// <see cref="Builder.WithMetrics(IDriverMetricsProvider,DriverMetricsOptions)"/>.
    /// </summary>
    public class DriverMetricsOptions
    {
        /// <summary>
        /// See <see cref="SetEnabledNodeMetrics"/> for more information. Defaults to <see cref="NodeMetric.DefaultNodeMetrics"/>.
        /// </summary>
        public IEnumerable<NodeMetric> EnabledNodeMetrics { get; private set; } = NodeMetric.DefaultNodeMetrics;

        /// <summary>
        /// See <see cref="SetEnabledSessionMetrics"/> for more information. Defaults to <see cref="SessionMetric.DefaultSessionMetrics"/>.
        /// </summary>
        public IEnumerable<SessionMetric> EnabledSessionMetrics { get; private set; } = SessionMetric.DefaultSessionMetrics;
        
        /// <summary>
        /// See <see cref="SetBucketPrefix"/> for more information.
        /// </summary>
        public string BucketPrefix { get; private set; }

        /// <summary>
        /// Builds an instance with the default options. Check each method's API docs for information about the default value for each option.
        /// </summary>
        public DriverMetricsOptions()
        {
        }
        
        /// <summary>
        /// Enables specific node metrics. The available node metrics can be found as static readonly properties in
        /// the <see cref="NodeMetric"/> class, e.g., <see cref="NodeMetric.Meters.BytesSent"/>.
        /// There is also a property that returns a collection with the default node metrics (<see cref="NodeMetric.DefaultNodeMetrics"/>)
        /// and one with all node metrics (<see cref="NodeMetric.AllNodeMetrics"/>).
        /// </summary>
        /// <returns>This instance.</returns>
        public DriverMetricsOptions SetEnabledNodeMetrics(IEnumerable<NodeMetric> enabledNodeMetrics)
        {
            EnabledNodeMetrics = enabledNodeMetrics;
            return this;
        }
        
        /// <summary>
        /// Enables specific session metrics. The available session metrics can be found as static readonly properties in
        /// the <see cref="SessionMetric"/> class, e.g., <see cref="SessionMetric.Meters.BytesSent"/>.
        /// There is also a property that returns a collection with the default session metrics (<see cref="SessionMetric.DefaultSessionMetrics"/>)
        /// and one with all session metrics (<see cref="SessionMetric.AllSessionMetrics"/>).
        /// </summary>
        /// <returns>This instance.</returns>
        public DriverMetricsOptions SetEnabledSessionMetrics(IEnumerable<SessionMetric> enabledSessionMetrics)
        {
            EnabledSessionMetrics = enabledSessionMetrics;
            return this;
        }

        /// <summary>
        /// Prepends a prefix to the bucket name for all metrics. The way the bucket name and metric name parameters
        /// are used depends on the <see cref="IDriverMetricsProvider"/> that is provided to the builder.
        /// In the case of the provider based on App.Metrics available in the CassandraCSharpDriver.AppMetrics package,
        /// the bucket name will be concatenated with the metric name (separated by a dot), which makes the full metric path like this:
        /// <code>
        /// Format: &lt;bucket-prefix&gt;.&lt;bucket-name&gt;.&lt;metric-name&gt;
        /// </code>
        /// Here is how the full metric path will look like for <see cref="NodeMetric.Counters.Retries"/> in practice with the AppMetrics provider:
        /// <code>
        /// // Set metric prefix
        /// var cluster = 
        ///     Cluster.Builder()
        ///            .AddContactPoint("127.0.0.1")
        ///            .WithSessionName("session")
        ///            .WithMetrics(
        ///                metrics.CreateDriverMetricsProvider(),
        ///                new DriverMetricsOptions().SetBucketPrefix("web.app"))
        ///            .Build();
        ///
        /// // Resulting metric name for the NodeMetric.Counters.Retries metric:
        /// web.app.session.nodes.127_0_0_1:9042.retries.total
        /// </code>
        /// </summary>
        public DriverMetricsOptions SetBucketPrefix(string bucketPrefix)
        {
            BucketPrefix = bucketPrefix;
            return this;
        }

        internal DriverMetricsOptions Clone()
        {
            return new DriverMetricsOptions
            {
                EnabledNodeMetrics = EnabledNodeMetrics,
                EnabledSessionMetrics = EnabledSessionMetrics,
                BucketPrefix = BucketPrefix
            };
        }
    }
}