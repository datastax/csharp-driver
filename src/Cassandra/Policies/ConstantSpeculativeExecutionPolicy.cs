using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

// ReSharper disable once CheckNamespace
namespace Cassandra
{
    /// <summary>
    /// A <see cref="ISpeculativeExecutionPolicy"/> that schedules a given number of speculative 
    /// executions, separated by a fixed delay.
    /// </summary>
    public class ConstantSpeculativeExecutionPolicy : ISpeculativeExecutionPolicy
    {
        /// <summary>
        /// Creates a new instance of a <see cref="ISpeculativeExecutionPolicy"/> that schedules a given
        ///  number of speculative executions, separated by a fixed delay.
        /// </summary>
        /// <param name="delay">The constant delay in milliseconds between each speculative execution. Must be strictly positive.</param>
        /// <param name="maxSpeculativeExecutions">The number of speculative executions. Must be strictly positive.</param>
        public ConstantSpeculativeExecutionPolicy(long delay, int maxSpeculativeExecutions)
        {
            Delay = delay;
            MaxSpeculativeExecutions = maxSpeculativeExecutions;
            if (delay <= 0L)
            {
                throw new ArgumentOutOfRangeException("delay", "The delay must be positive");
            }
            if (maxSpeculativeExecutions <= 0)
            {
                throw new ArgumentOutOfRangeException("maxSpeculativeExecutions", "The maximum amount of speculative executions must be a positive number");
            }
        }

        public long Delay { get; }

        public int MaxSpeculativeExecutions { get; }

        public void Dispose()
        {
            
        }

        public void Initialize(ICluster cluster)
        {
            
        }

        public ISpeculativeExecutionPlan NewPlan(string keyspace, IStatement statement)
        {
            return new ConstantSpeculativeExecutionPlan(Delay, MaxSpeculativeExecutions);
        }

        private class ConstantSpeculativeExecutionPlan : ISpeculativeExecutionPlan
        {
            private readonly long _delay;
            private readonly int _maxSpeculativeExecutions;
            private int _executions;

            public ConstantSpeculativeExecutionPlan(long delay, int maxSpeculativeExecutions)
            {
                _delay = delay;
                _maxSpeculativeExecutions = maxSpeculativeExecutions;
            }

            public long NextExecution(Host lastQueried)
            {
                if (_executions++ < _maxSpeculativeExecutions)
                {
                    return _delay;
                }
                return 0L;
            }
        }
    }
}
