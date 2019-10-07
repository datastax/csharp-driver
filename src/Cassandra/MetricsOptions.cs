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

namespace Cassandra
{
    /// <summary>
    /// This class is used to customize options related to Metrics. It is used in 
    /// <see cref="Builder.WithMetrics(IDriverMetricsProvider, MetricsOptions)"/>.
    /// </summary>
    public class MetricsOptions
    {
        /// <summary>
        /// See <see cref="SetDisabledNodeMetrics"/> for more information.
        /// </summary>
        public IEnumerable<NodeMetric> DisabledNodeMetrics { get; private set; } = new List<NodeMetric>();
        
        /// <summary>
        /// See <see cref="SetDisabledSessionMetrics"/> for more information.
        /// </summary>
        public IEnumerable<SessionMetric> DisabledSessionMetrics { get; private set; } = new List<SessionMetric>();
        
        /// <summary>
        /// See <see cref="SetPathPrefix"/> for more information.
        /// </summary>
        public string PathPrefix { get; private set; }
        
        /// <summary>
        /// Disables specific node metrics. The available node metrics can be found as static readonly properties in
        /// the <see cref="NodeMetric"/> class, e.g., <see cref="NodeMetric.Meters.BytesSent"/>.
        /// There is also a property that returns a collection with all node metrics: <see cref="NodeMetric.AllNodeMetrics"/>.
        /// </summary>
        /// <returns>This instance.</returns>
        public MetricsOptions SetDisabledNodeMetrics(IEnumerable<NodeMetric> disabledNodeMetrics)
        {
            DisabledNodeMetrics = disabledNodeMetrics;
            return this;
        }
        
        /// <summary>
        /// Disables specific session metrics. The available session metrics can be found as static readonly properties in
        /// the <see cref="SessionMetric"/> class, e.g., <see cref="SessionMetric.Meters.BytesSent"/>.
        /// There is also a property that returns a collection with all node metrics: <see cref="SessionMetric.AllSessionMetrics"/>.
        /// </summary>
        /// <returns>This instance.</returns>
        public MetricsOptions SetDisabledSessionMetrics(IEnumerable<SessionMetric> disabledSessionMetrics)
        {
            DisabledSessionMetrics = disabledSessionMetrics;
            return this;
        }

        /// <summary>
        /// Prepends context components to all metrics. The way these strings are used depends on the <see cref="IDriverMetricsProvider"/>
        /// that is provided to the builder. In the case of the provider based on App.Metrics available in the CassandraCSharpDriver.AppMetrics package,
        /// the context components will be concatenated with a dot separating each one, which makes the full metric path like this:
        /// <code>
        /// Format: &lt;path-prefix&gt;.&lt;session-name&gt;.nodes.&lt;node-address&gt;.&lt;metric-path&gt;
        /// </code>
        /// Here is how the full metric name will look like for <see cref="NodeMetric.Counters.Retries"/> in practice:
        /// <code>
        /// // Set metric prefix
        /// var cluster = 
        ///     Cluster.Builder()
        ///            .AddContactPoint("127.0.0.1")
        ///            .WithSessionName("session")
        ///            .WithMetrics(
        ///                new AppMetricsDriverMetricsProvider(metrics),
        ///                new MetricsOptions().SetPathPrefix("web.app"))
        ///            .Build();
        ///
        /// // Resulting metric name for the NodeMetric.Counters.Retries metric:
        /// web.app.session.nodes.127_0_0_1:9042.retries.total
        /// </code>
        /// </summary>
        /// <param name="pathPrefix"></param>
        /// <returns></returns>
        public MetricsOptions SetPathPrefix(string pathPrefix)
        {
            PathPrefix = pathPrefix;
            return this;
        }

        internal MetricsOptions Clone()
        {
            return new MetricsOptions
            {
                DisabledNodeMetrics = DisabledNodeMetrics,
                DisabledSessionMetrics = DisabledSessionMetrics,
                PathPrefix = PathPrefix
            };
        }
    }
}