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
using System.Collections.Generic;
using System.Linq;
using Cassandra.Metrics.Registries;

namespace Cassandra
{
    /// <summary>
    /// Represents an individual metric.
    /// </summary>
    public class NodeMetric : IEquatable<NodeMetric>
    {
        private readonly int _hashCode;

        internal NodeMetric(string name)
        {
            Context = Enumerable.Empty<string>();
            Name = name ?? throw new ArgumentNullException(nameof(name));
            _hashCode = NodeMetric.ComputeHashCode(Context, name);
        }

        internal NodeMetric(IEnumerable<string> context, string name)
        {
            Context = context ?? throw new ArgumentNullException(nameof(context));
            Name = name ?? throw new ArgumentNullException(nameof(name));
            _hashCode = NodeMetric.ComputeHashCode(context, name);
        }

        /// <summary>
        /// Context for this metric. It doesn't include the full context like the global context set in the builder,
        /// session name or node address.
        /// Here is what this property will return for <see cref="NodeMetrics.Meters.Errors.Request.ReadTimeout"/>:
        /// <code>
        /// // Assume the following full metric path the NodeMetrics.Meters.Errors.Request.ReadTimeout metric:
        /// web.app.session.nodes.127_0_0_1:9042.errors.request.read-timeout
        ///
        /// // The NodeMetric.Context property will return a collection equivalent to this:
        /// new [] { "errors", "request" }
        /// </code>
        /// </summary>
        public IEnumerable<string> Context { get; }

        /// <summary>
        /// Metric name without any context.
        /// Here is what this property will return for <see cref="NodeMetrics.Meters.Errors.Request.ReadTimeout"/>:
        /// <code>
        /// // Assume the following full metric path the NodeMetrics.Meters.Errors.Request.ReadTimeout metric:
        /// web.app.session.nodes.127_0_0_1:9042.errors.request.read-timeout
        ///
        /// // The NodeMetric.Name property will return
        /// read-timeout
        /// </code>
        /// </summary>
        public string Name { get; }

        public bool Equals(NodeMetric other)
        {
            return other != null && Context.SequenceEqual(other.Context) && string.Equals(Name, other.Name);
        }

        public override bool Equals(object obj)
        {
            return obj != null && obj is NodeMetric other && Equals(other);
        }

        public override int GetHashCode()
        {
            return _hashCode;
        }

        private static int ComputeHashCode(IEnumerable<string> context, string name)
        {
            return Utils.CombineHashCode(context.Concat(new[] { name }));
        }
    }
}