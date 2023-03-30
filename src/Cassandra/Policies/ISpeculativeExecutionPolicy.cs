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

// ReSharper disable once CheckNamespace
namespace Cassandra
{
    /// <summary>
    /// The policy that decides if the driver will send speculative queries to the next hosts when the current host takes too long to respond.
    /// only idempotent statements will be speculatively retried, see <see cref="IStatement.IsIdempotent"/> for more information.
    /// </summary>
    public interface ISpeculativeExecutionPolicy : IDisposable
    {
        /// <summary>
        /// Initializes the policy at cluster startup.
        /// </summary>
        void Initialize(ICluster cluster);

        /// <summary>
        /// Returns the plan to use for a new query.
        /// </summary>
        /// <param name="keyspace">the currently logged keyspace</param>
        /// <param name="statement">the query for which to build a plan.</param>
        /// <returns></returns>
        ISpeculativeExecutionPlan NewPlan(string keyspace, IStatement statement);
    }
}
