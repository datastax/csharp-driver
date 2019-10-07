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

namespace Cassandra
{
    /// <summary>
    /// A policy that extends <see cref="IRetryPolicy"/> providing an additional method to handle
    /// unexpected errors.
    /// </summary>
    public interface IExtendedRetryPolicy : IRetryPolicy
    {
        /// <summary>
        /// Defines whether to retry and at which consistency level on an
        /// unexpected error.
        /// <para>
        /// This method might be invoked in the following situations:
        /// </para>
        /// <ol>
        ///   <li>On a client timeout, while waiting for the server response
        ///   (see <see cref="SocketOptions.ReadTimeoutMillis"/>).</li>
        ///   <li>On a socket error (socket closed, etc.).</li>
        ///   <li>When the contacted host replies with an <c>OVERLOADED</c> error or a <c>SERVER_ERROR</c>.</li>
        /// </ol>
        /// <para>
        /// Note that when such an error occurs, there is no guarantee that the mutation has been applied server-side
        /// or not.
        /// </para>
        /// </summary>
        /// <param name="statement">The original query that failed.</param>
        /// <param name="config">The current cluster configuration.</param>
        /// <param name="ex">The exception that caused this request to fail.</param>
        /// <param name="nbRetry">The number of retries already performed for this operation.</param>
        RetryDecision OnRequestError(IStatement statement, Configuration config, Exception ex, int nbRetry);
    }

    internal static class RetryPolicyExtensions
    {
        /// <summary>
        /// When the policy provided implements IExtendedRetryPolicy, it returns the same instance.
        /// Otherwise it returns a new instance of IExtendedRetryPolicy, delegating all <see cref="IRetryPolicy"/>
        /// methods to the provided policy and the rest to the defaultPolicy.
        /// </summary>
        /// <param name="policy">The policy to wrap or cast</param>
        /// <param name="defaultPolicy">
        /// The default policy to handle IExtendedRetryPolicy methods.
        /// When null, the default retry policy will be used.
        /// </param>
        internal static IExtendedRetryPolicy Wrap(this IRetryPolicy policy, IExtendedRetryPolicy defaultPolicy)
        {
            var resultPolicy = policy as IExtendedRetryPolicy;
            if (resultPolicy == null)
            {
                // Wrap the user provided policy
                return new WrappedExtendedRetryPolicy(policy, defaultPolicy);
            }
            // Return the user-provided policy casted to IExtendedRetryPolicy
            return resultPolicy;
        }

        /// <summary>
        /// A policy that delegates to the user provided retry policy for all <see cref="IRetryPolicy"/> method calls and
        /// to the default <see cref="IExtendedRetryPolicy"/> for the rest.
        /// </summary>
        internal class WrappedExtendedRetryPolicy : IExtendedRetryPolicy
        {
            public WrappedExtendedRetryPolicy(IRetryPolicy policy, IExtendedRetryPolicy defaultPolicy)
            {
                Policy = policy ?? throw new ArgumentNullException(nameof(policy));
                DefaultPolicy = defaultPolicy ?? throw new ArgumentNullException(nameof(defaultPolicy));
            }

            public IRetryPolicy Policy { get; }

            public IExtendedRetryPolicy DefaultPolicy { get; }

            public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, 
                                               int receivedResponses, bool dataRetrieved, int nbRetry)
            {
                return Policy.OnReadTimeout(query, cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
            }

            public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType,
                                                int requiredAcks, int receivedAcks, int nbRetry)
            {
                return Policy.OnWriteTimeout(query, cl, writeType, requiredAcks, receivedAcks, nbRetry);
            }

            public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica,
                                               int aliveReplica, int nbRetry)
            {
                return Policy.OnUnavailable(query, cl, requiredReplica, aliveReplica, nbRetry);
            }

            public RetryDecision OnRequestError(IStatement statement, Configuration config, Exception ex, int nbRetry)
            {
                return DefaultPolicy.OnRequestError(statement, config, ex, nbRetry);
            }
        }
    }
}
