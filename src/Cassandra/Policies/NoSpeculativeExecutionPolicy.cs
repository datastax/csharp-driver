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
    /// A <see cref="ISpeculativeExecutionPolicy"/> that never schedules speculative executions.
    /// </summary>
    public class NoSpeculativeExecutionPolicy : ISpeculativeExecutionPolicy
    {
        private static readonly ISpeculativeExecutionPlan Plan = new NoSpeculativeExecutionPlan();
        public static readonly NoSpeculativeExecutionPolicy Instance = new NoSpeculativeExecutionPolicy();

        private NoSpeculativeExecutionPolicy()
        {
            
        }

        public void Dispose()
        {
            
        }

        public void Initialize(ICluster cluster)
        {
            
        }

        public ISpeculativeExecutionPlan NewPlan(string keyspace, IStatement statement)
        {
            return Plan;
        }

        private class NoSpeculativeExecutionPlan : ISpeculativeExecutionPlan
        {
            public long NextExecution(Host lastQueried)
            {
                return 0L;
            }
        }
    }
}