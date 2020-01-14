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

using Cassandra.DataStax.Graph;

namespace Cassandra
{
    /// <summary>
    /// <para>
    /// An execution profile. See the documentation of <see cref="Builder.WithExecutionProfiles"/> for an explanation on how to create these profiles
    /// and add them to the cluster.
    /// </para>
    /// <para>
    /// You can use these profiles via their name (that is set when adding them to the cluster through <see cref="Builder.WithExecutionProfiles"/>)
    /// with the multiple driver APIs that execute requests (LINQ, Mapper and Session). Example with <see cref="ISession.ExecuteAsync(IStatement, string)"/>:
    /// </para>
    /// <para>
    /// <code>
    /// var rowSet = await session.ExecuteAsync(new SimpleStatement("SELECT * FROM keyspace.table"), "profile1").ConfigureAwait(false);
    /// </code>
    /// </para>
    /// </summary>
    public interface IExecutionProfile
    {
        /// <summary>
        /// <para>Retrieves the ConsistencyLevel set on this profile. It's <code>null</code> if not set.</para>
        /// <para>See <see cref="Cassandra.ConsistencyLevel"/> for additional context on this setting.</para>
        /// </summary>
        ConsistencyLevel? ConsistencyLevel { get; }
        
        /// <summary>
        /// <para>Retrieves the LoadBalancingPolicy set on this profile. It's <code>null</code> if not set.</para>
        /// <para>See <see cref="ILoadBalancingPolicy"/> for additional context on this setting.</para>
        /// </summary>
        ILoadBalancingPolicy LoadBalancingPolicy { get; }
        
        /// <summary>
        /// The per-host read timeout in milliseconds.
        /// <para>
        /// This defines how long the driver will wait for a given Cassandra node to answer a query.
        /// </para>
        /// Please note that this is not the maximum time a call to <see cref="ISession.Execute(string)"/> may block; this is the maximum time that call will wait for one particular Cassandra host, but other hosts will be tried if one of them timeout. In other words, a <see cref="Session.Execute(string)"/> call may theoretically wait up to ReadTimeoutMillis * {number_of_cassandra_hosts} (though the total number of hosts tried for a given query also depends on the LoadBalancingPolicy in use).
        /// Also note that for efficiency reasons, this read timeout is approximate, it may fire up to late. It is not meant to be used for precise timeout, but rather as a protection against misbehaving Cassandra nodes.
        /// </summary>
        int? ReadTimeoutMillis { get; }
        
        /// <summary>
        /// <para>Retrieves the RetryPolicy set on this profile. It's <code>null</code> if not set.</para>
        /// <para>See <see cref="RetryPolicy"/> for additional context on this setting.</para>
        /// </summary>
        IExtendedRetryPolicy RetryPolicy { get; }
        
        /// <summary>
        /// <para>Retrieves the SerialConsistencyLevel set on this profile. It's <code>null</code> if not set.</para>
        /// <para>See <see cref="Cassandra.ConsistencyLevel"/> for additional context on this setting.</para>
        /// </summary>
        ConsistencyLevel? SerialConsistencyLevel { get; }
        
        /// <summary>
        /// <para>Retrieves the SpeculativeExecutionPolicy set on this profile. It's <code>null</code> if not set.</para>
        /// <para>See <see cref="ISpeculativeExecutionPolicy"/> for additional context on this setting.</para>
        /// </summary>
        ISpeculativeExecutionPolicy SpeculativeExecutionPolicy { get; }

        /// <summary>
        /// <para>Retrieves the DSE Graph options set on this profile.</para>
        /// <para>See <see cref="GraphOptions"/> for additional information on the settings within the <see cref="GraphOptions"/> class.</para>
        /// </summary>
        GraphOptions GraphOptions { get; }
    }
}