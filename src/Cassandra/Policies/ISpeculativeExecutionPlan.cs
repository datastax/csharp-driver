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


// ReSharper disable once CheckNamespace
namespace Cassandra
{
    /// <summary>
    /// Represents a plan that governs speculative executions for a given query.
    /// </summary>
    public interface ISpeculativeExecutionPlan
    {
        /// <summary>
        /// Returns the time before the next speculative query.
        /// </summary>
        /// <param name="lastQueried">the host that was just queried.</param>
        /// <returns>the time (in milliseconds) before a speculative query is sent to the next host. If zero or negative, no speculative query will be sent.</returns>
        long NextExecution(Host lastQueried);
    }
}
