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

namespace Cassandra
{
    /// <summary>
    /// Represents an individual metric.
    /// </summary>
    public class SessionMetric : IEquatable<SessionMetric>
    {
        private readonly int _hashCode;

        internal SessionMetric(string name)
        {
            Context = Enumerable.Empty<string>();
            Name = name ?? throw new ArgumentNullException(nameof(name));
            _hashCode = SessionMetric.ComputeHashCode(Context, name);
        }

        internal SessionMetric(IEnumerable<string> context, string name)
        {
            Context = context ?? throw new ArgumentNullException(nameof(context));
            Name = name ?? throw new ArgumentNullException(nameof(name));
            _hashCode = SessionMetric.ComputeHashCode(context, name);
        }

        /// <summary>
        /// Context for this metric. It doesn't include the full context like the global context set in the builder or
        /// session name.
        /// Here is what this property will return for <see cref="SessionMetrics.Meters.CqlClientTimeouts"/>:
        /// <code>
        /// // Assume the following full metric path the SessionMetrics.Meters.CqlClientTimeouts metric:
        /// web.app.session.cql-client-timeouts
        ///
        /// // The SessionMetric.Context property will return an empty collection equivalent:
        /// new [] { }
        /// </code>
        /// </summary>
        public IEnumerable<string> Context { get; }

        /// <summary>
        /// Metric name without any context.
        /// Here is what this property will return for <see cref="SessionMetrics.Meters.CqlClientTimeouts"/>:
        /// <code>
        /// // Assume the following full metric path the SessionMetrics.Meters.CqlClientTimeouts metric:
        /// web.app.session.cql-client-timeouts
        ///
        /// // The SessionMetric.Name property will return
        /// cql-client-timeouts
        /// </code>
        /// </summary>
        public string Name { get; }

        public bool Equals(SessionMetric other)
        {
            return other != null && Context.SequenceEqual(other.Context) && string.Equals(Name, other.Name);
        }

        public override bool Equals(object obj)
        {
            return obj != null && obj is SessionMetric other && Equals(other);
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