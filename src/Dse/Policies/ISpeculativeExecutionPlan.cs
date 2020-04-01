//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//



// ReSharper disable once CheckNamespace
namespace Dse
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
